use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{exec_datafusion_err, exec_err};
use datafusion::error::Result as DFResult;
use deltalake::arrow::array::Int64Builder;
use hashbrown::HashMap;
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{
    PlainTermArray, PlainTermArrayElementBuilder, PlainTermScalar,
};
use std::sync::Arc;

/// Implements an in-memory mapping for ObjectIds using a string interner.
#[derive(Debug)]
pub struct ObjectIdInMemoryMapping {
    /// String interner for deduplicating term values, datatypes, and languages.
    interner: StringInterner,
    /// Direct mapping from Object ID (the Vec index) to the MappedTerm.
    terms: Vec<MappedTerm>,
    /// Reverse mapping from MappedTerm to Object ID for fast encoding.
    term_to_id: HashMap<MappedTerm, i64, ahash::RandomState>,
    /// The next available Object ID.
    next_id: i64,
}

impl ObjectIdInMemoryMapping {
    /// Creates a new object id dictionary.
    pub fn empty() -> Self {
        Self {
            interner: StringInterner::new(),
            terms: Vec::new(),
            term_to_id: HashMap::with_hasher(ahash::RandomState::new()),
            next_id: 0,
        }
    }

    /// Encodes RDF Terms into Object IDs, assigning new IDs if necessary.
    pub fn encode_array(&mut self, array: &PlainTermArray) -> DFResult<Int64Array> {
        let array_parts = array.as_parts();
        let mut result_ids = Int64Builder::with_capacity(array.len());

        for idx in 0..array.len() {
            if array.inner().is_null(idx) {
                result_ids.append_null();
                continue;
            }

            let term_type = array_parts.term_type.value(idx);
            let value = array_parts.value.value(idx);
            let data_type = array_parts
                .data_type
                .is_valid(idx)
                .then(|| array_parts.data_type.value(idx));
            let language = array_parts
                .language_tag
                .is_valid(idx)
                .then(|| array_parts.language_tag.value(idx));

            let value_id = self.interner.get_or_intern(value);
            let data_type_id = data_type.map(|dt| self.interner.get_or_intern(dt));
            let language_id = language.map(|lang| self.interner.get_or_intern(lang));

            let mapped_term = MappedTerm {
                term_type,
                value: value_id,
                data_type: data_type_id,
                language: language_id,
            };

            if let Some(&id) = self.term_to_id.get(&mapped_term) {
                result_ids.append_value(id);
            } else {
                let id = self.next_id;
                self.next_id += 1;

                self.term_to_id.insert(mapped_term, id); // mapped_term is Copy now!
                self.terms.push(mapped_term);

                result_ids.append_value(id);
            }
        }

        Ok(result_ids.finish())
    }

    /// Resolves a bulk of Object IDs into their corresponding RDF Plain Terms
    pub fn resolve_plain_terms(&self, ids: &Int64Array) -> DFResult<ArrayRef> {
        let mut builder = PlainTermArrayElementBuilder::with_capacity(ids.len());

        for i in 0..ids.len() {
            if ids.is_valid(i) {
                let id = ids.value(i);

                if id >= 0 && (id as usize) < self.terms.len() {
                    let term = &self.terms[id as usize];
                    self.append_term_to_builder(term, &mut builder);
                } else {
                    return exec_err!("Object ID {} not found in dictionary", id);
                }
            } else {
                builder.append_null();
            }
        }

        Ok(builder.finish().into_array_ref())
    }

    /// Returns a list of RecordBatches for terms between start_id and self.next_id.
    pub fn read_batches_since_id(
        &self,
        start_id: i64,
        schema: &SchemaRef,
    ) -> DFResult<Vec<RecordBatch>> {
        const CHUNK_SIZE: usize = 8192;

        let current_next_id = self.next_id;

        if start_id >= current_next_id {
            return Ok(vec![]);
        }

        let mut batches = Vec::new();
        let mut current_id = start_id;

        while current_id < current_next_id {
            let total_remaining = (current_next_id - current_id) as usize;
            let take = total_remaining.min(CHUNK_SIZE);

            let mut builder = PlainTermArrayElementBuilder::new();

            let ids_slice = Arc::new(Int64Array::from_iter_values(
                current_id..(current_id + take as i64),
            ));

            for id in current_id..(current_id + take as i64) {
                let term = &self.terms[id as usize];
                self.append_term_to_builder(term, &mut builder);
            }

            let terms_array = builder.finish().into_array_ref();
            let batch =
                RecordBatch::try_new(Arc::clone(schema), vec![ids_slice, terms_array])?;

            batches.push(batch);
            current_id += take as i64;
        }

        Ok(batches)
    }

    pub fn next_id(&self) -> i64 {
        self.next_id
    }

    pub fn get_id_by_term(&self, term: &PlainTermScalar) -> Option<i64> {
        let parts = term.as_parts()?;

        let value_id = self.interner.get_id(parts.value)?;
        let data_type_id = match parts.data_type {
            Some(dt) => Some(self.interner.get_id(dt)?),
            None => None,
        };
        let language_id = match parts.language_tag {
            Some(lang) => Some(self.interner.get_id(lang)?),
            None => None,
        };

        let mapped_term = MappedTerm {
            term_type: parts.term_type,
            value: value_id,
            data_type: data_type_id,
            language: language_id,
        };

        self.term_to_id.get(&mapped_term).copied()
    }

    /// Loads a sorted RecordBatch of `(id, term)` into the memory mapping. Fails if the incoming
    /// Object IDs are not perfectly contiguous and sorted.
    pub fn add_batch(&mut self, batch: &RecordBatch) -> DFResult<()> {
        let id_col = batch
            .column_by_name("id")
            .expect("Missing 'id' column")
            .as_any()
            .downcast_ref::<Int64Array>()
            .ok_or_else(|| exec_datafusion_err!("Expected Int64Array for id column"))?;

        let term_col = batch.column_by_name("term").expect("Missing 'term' column");

        let plain_term_array = PlainTermArray::try_from(Arc::clone(term_col))?;
        let array_parts = plain_term_array.as_parts();

        for i in 0..batch.num_rows() {
            let id = id_col.value(i);

            // Strict contiguous validation
            if id != self.next_id {
                return exec_err!(
                    "Non-contiguous or unsorted object ID detected. Expected {}, found {}",
                    self.next_id,
                    id
                );
            }

            // Extract term parts
            let term_type = array_parts.term_type.value(i);
            let value = array_parts.value.value(i);
            let data_type = array_parts
                .data_type
                .is_valid(i)
                .then(|| array_parts.data_type.value(i));
            let language = array_parts
                .language_tag
                .is_valid(i)
                .then(|| array_parts.language_tag.value(i));

            // Intern strings
            let value_id = self.interner.get_or_intern(value);
            let data_type_id = data_type.map(|dt| self.interner.get_or_intern(dt));
            let language_id = language.map(|lang| self.interner.get_or_intern(lang));

            let mapped_term = MappedTerm {
                term_type,
                value: value_id,
                data_type: data_type_id,
                language: language_id,
            };

            // Because IDs are exactly `self.next_id`, the position in `self.terms`
            // naturally aligns with `id`.
            self.terms.push(mapped_term);
            self.term_to_id.insert(mapped_term, id);

            self.next_id += 1;
        }

        Ok(())
    }

    /// Helper method to safely unpack a MappedTerm and insert it into an ArrayBuilder.
    #[inline]
    fn append_term_to_builder(
        &self,
        term: &MappedTerm,
        builder: &mut PlainTermArrayElementBuilder,
    ) {
        let term_type = term.term_type;

        let value_str = self.interner.resolve(&term.value).unwrap_or("");
        let dt_str = term
            .data_type
            .as_ref()
            .and_then(|id| self.interner.resolve(id));
        let lang_str = term
            .language
            .as_ref()
            .and_then(|id| self.interner.resolve(id));

        builder.append_raw(term_type, value_str, dt_str, lang_str);
    }
}

/// Represents an interned RDF term. Now implements `Copy` since all internal types are `Copy`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MappedTerm {
    term_type: i8,
    value: InternedStr,
    data_type: Option<InternedStr>,
    language: Option<InternedStr>,
}

/// The maximum byte length for a string to be stored inline.
/// Strings larger than this will be allocated and interned in the HashMap.
const SMALL_STRING_SIZE: usize = 14;

/// Represents an interned string, either stored inline (if small) or by reference ID.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InternedStr {
    Small {
        len: u8,
        bytes: [u8; SMALL_STRING_SIZE],
    },
    Interned(u64),
}

impl InternedStr {
    /// Creates a new `Small` variant from a string slice.
    /// Panics if the string length exceeds `SMALL_STRING_SIZE`.
    #[inline]
    fn new_small(s: &str) -> Self {
        debug_assert!(s.len() <= SMALL_STRING_SIZE);
        let mut bytes = [0u8; SMALL_STRING_SIZE];
        bytes[..s.len()].copy_from_slice(s.as_bytes());
        Self::Small {
            len: s.len() as u8,
            bytes,
        }
    }
}

/// A simple string interner to deduplicate String allocations, with inline small string optimization.
#[derive(Debug, Default)]
pub struct StringInterner {
    str_to_id: HashMap<Arc<str>, u64, ahash::RandomState>,
    id_to_str: HashMap<u64, Arc<str>, ahash::RandomState>,
    next_id: u64,
}

impl StringInterner {
    /// Creates a new empty [`StringInterner`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the `InternedStr` for a string. If it's small, it's inlined.
    /// Otherwise, it's interned in the HashMap if it doesn't already exist.
    pub fn get_or_intern(&mut self, s: &str) -> InternedStr {
        if s.len() <= SMALL_STRING_SIZE {
            return InternedStr::new_small(s);
        }

        if let Some(&id) = self.str_to_id.get(s) {
            return InternedStr::Interned(id);
        }

        let id = self.next_id;
        self.next_id += 1;

        let arc_str: Arc<str> = Arc::from(s);
        self.str_to_id.insert(Arc::clone(&arc_str), id);
        self.id_to_str.insert(id, arc_str);

        InternedStr::Interned(id)
    }

    /// Gets the `InternedStr` for a string if it exists or if it's small enough to be inlined.
    pub fn get_id(&self, s: &str) -> Option<InternedStr> {
        if s.len() <= SMALL_STRING_SIZE {
            return Some(InternedStr::new_small(s));
        }
        self.str_to_id.get(s).copied().map(InternedStr::Interned)
    }

    /// Resolves an `InternedStr` back to its string slice.
    pub fn resolve<'a>(&'a self, interned: &'a InternedStr) -> Option<&'a str> {
        match interned {
            InternedStr::Small { len, bytes } => {
                // Safe because we only construct it from valid utf8 strings in `new_small`
                unsafe { Some(std::str::from_utf8_unchecked(&bytes[..*len as usize])) }
            }
            InternedStr::Interned(id) => self.id_to_str.get(id).map(|arc| &**arc),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::error::Result as DFResult;
    use rdf_fusion_encoding::TermEncoding;
    use rdf_fusion_encoding::plain_term::{
        PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder,
    };

    #[test]
    fn test_interned_str_size() {
        assert_eq!(size_of::<InternedStr>(), 16);
    }

    #[test]
    fn test_encode_return_correct_type() -> DFResult<()> {
        let mut mapping = ObjectIdInMemoryMapping::empty();

        let input_array = create_test_array();

        let encoded_ids = mapping.encode_array(&input_array)?;
        let resolved_array_ref = mapping.resolve_plain_terms(&encoded_ids)?;

        assert_eq!(
            resolved_array_ref.data_type(),
            PLAIN_TERM_ENCODING.data_type()
        );
        Ok(())
    }

    #[test]
    fn test_encode_and_resolve_terms() -> DFResult<()> {
        let mut mapping = ObjectIdInMemoryMapping::empty();

        let input_array = create_test_array();

        let encoded_ids = mapping.encode_array(&input_array)?;
        let resolved_array_ref = mapping.resolve_plain_terms(&encoded_ids)?;

        assert_eq!(input_array.inner().as_ref(), resolved_array_ref.as_ref(),);
        Ok(())
    }

    #[test]
    fn test_encode_twice_get_same_result() -> DFResult<()> {
        let mut mapping = ObjectIdInMemoryMapping::empty();

        let input_array = create_test_array();

        let encoded_ids1 = mapping.encode_array(&input_array)?;
        let encoded_ids2 = mapping.encode_array(&input_array)?;

        assert_eq!(encoded_ids1, encoded_ids2,);
        Ok(())
    }
    #[test]
    fn test_add_batch_contiguous_success() -> DFResult<()> {
        let mut mapping = ObjectIdInMemoryMapping::empty();

        // 1. Create a contiguous ID array matching the empty mapping's next_id (0, 1)
        let id_array = Arc::new(Int64Array::from(vec![0, 1]));

        // 2. Create the corresponding PlainTermArray
        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_raw(1, "http://example.org/A", None, None);
        builder.append_raw(1, "http://example.org/B", None, None);
        let term_array = builder.finish().into_array_ref();

        // 3. Build the RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("term", PLAIN_TERM_ENCODING.data_type().clone(), true),
        ]));

        let batch = RecordBatch::try_new(schema, vec![id_array, term_array])?;

        // 4. Test adding the batch
        let result = mapping.add_batch(&batch);
        assert!(result.is_ok(), "Failed to add valid contiguous batch");
        assert_eq!(
            mapping.next_id(),
            2,
            "next_id was not incremented correctly"
        );

        Ok(())
    }

    #[test]
    fn test_add_batch_rejects_non_contiguous_ids() -> DFResult<()> {
        let mut mapping = ObjectIdInMemoryMapping::empty();

        // 1. Create an ID array with a gap (0, 2) - skipping 1
        let id_array = Arc::new(Int64Array::from(vec![0, 2]));

        // 2. Create the corresponding PlainTermArray
        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_raw(1, "http://example.org/A", None, None);
        builder.append_raw(1, "http://example.org/C", None, None);
        let term_array = builder.finish().into_array_ref();

        // 3. Build the RecordBatch
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("term", PLAIN_TERM_ENCODING.data_type().clone(), true),
        ]));

        let batch = RecordBatch::try_new(schema, vec![id_array, term_array])?;

        // 4. Test adding the batch - this should FAIL due to strict contiguous validation
        let result = mapping.add_batch(&batch);
        assert!(
            result.is_err(),
            "add_batch should have failed on non-contiguous IDs"
        );

        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("Expected 1, found 2"),
            "Error message did not contain expected validation details: {err_msg}"
        );

        Ok(())
    }

    fn create_test_array() -> PlainTermArray {
        let mut builder = PlainTermArrayElementBuilder::new();
        builder.append_raw(1, "http://example.org/Alice", None, None);
        builder.append_raw(2, "b0", None, None);
        builder.append_raw(3, "Hello", None, Some("en"));
        builder.append_raw(
            3,
            "42",
            Some("http://www.w3.org/2001/XMLSchema#integer"),
            None,
        );
        builder.finish()
    }
}
