use datafusion::arrow::array::{Array, ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::compute::{concat, interleave};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use datafusion::error::Result as DFResult;
use deltalake::arrow::array::Int64Builder;
use hashbrown::{Equivalent, HashMap};
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArray, PlainTermArrayElementBuilder,
    PlainTermArrayParts, PlainTermScalar,
};
use rdf_fusion_encoding::typed_family::TypedFamilyEncodingRef;
use std::sync::Arc;

/// Determines how large the chunks are in [`ObjectIdInMemoryMapping`].
const CHUNK_SIZE: usize = 8192;

/// Implements an in-memory mapping for ObjectIds. This struct does not handle persistence at all.
#[derive(Debug)]
pub struct ObjectIdInMemoryMapping {
    /// The plain term arrays loaded in memory.
    plain_terms: Vec<ArrayRef>,
    /// The typed family arrays loaded in memory.
    typed_family: Vec<ArrayRef>,
    /// Mapping from Terms to IDs for fast encoding using hashbrown.
    term_to_id: HashMap<MappedTerm, i64>,
    /// The next available Object ID.
    next_id: i64,
    /// Encoding for typed values.
    typed_family_encoding: TypedFamilyEncodingRef,
}

impl ObjectIdInMemoryMapping {
    /// Creates a new object id dictionary.
    pub fn empty(typed_family_encoding: TypedFamilyEncodingRef) -> Self {
        let plain_terms = vec![PLAIN_TERM_ENCODING.create_null_array(1).into_array_ref()];

        let typed_family = vec![
            typed_family_encoding
                .create_null_array(1)
                .unwrap()
                .into_array_ref(),
        ];

        Self {
            term_to_id: HashMap::new(),
            plain_terms,
            typed_family,
            next_id: 0,
            typed_family_encoding,
        }
    }

    /// Encodes RDF Terms into Object IDs, assigning new IDs if necessary.
    pub fn encode_array(&mut self, array: &PlainTermArray) -> DFResult<Int64Array> {
        let array_parts = array.as_parts();
        let mut result_ids = Int64Builder::with_capacity(array.len());

        let mut new_terms = PlainTermArrayElementBuilder::new();

        for idx in 0..array.len() {
            if array.inner().is_null(idx) {
                result_ids.append_null();
                continue;
            }

            let term_ref = MappedTermRef::from_parts(&array_parts, idx);
            if let Some(id) = self.term_to_id.get(&term_ref) {
                result_ids.append_value(*id);
            } else {
                let id = self.next_id;
                self.next_id += 1;

                self.term_to_id.insert(term_ref.to_mapped_term(), id);

                new_terms.append_raw(
                    term_ref.term_type,
                    term_ref.value,
                    term_ref.datatype,
                    term_ref.language,
                );

                result_ids.append_value(id);
            }
        }

        let new_terms = new_terms.finish();
        if !new_terms.is_empty() {
            let new_typed_array = self
                .typed_family_encoding
                .cast_from_plain_term_array(&new_terms)?
                .into_array_ref();
            self.append_to_chunks(new_terms.into_array_ref(), new_typed_array)?;
        }

        Ok(result_ids.finish())
    }

    /// Appends plain terms and typed arrays to exactly CHUNK_SIZE blocks
    fn append_to_chunks(
        &mut self,
        new_plain: ArrayRef,
        new_typed: ArrayRef,
    ) -> DFResult<()> {
        let mut offset = 0;
        let total_new = new_typed.len();

        while offset < total_new {
            let last_idx = self.typed_family.len() - 1;
            let last_len = self.typed_family[last_idx].len();
            let remaining = total_new - offset;

            if last_idx == 0 || last_len == CHUNK_SIZE {
                let take_len = remaining.min(CHUNK_SIZE);

                self.plain_terms.push(new_plain.slice(offset, take_len));
                self.typed_family.push(new_typed.slice(offset, take_len));

                offset += take_len;
            } else {
                let available = CHUNK_SIZE - last_len;
                let take_len = remaining.min(available);

                // Concat plain_term
                let slice_plain = new_plain.slice(offset, take_len);
                let combined_plain =
                    concat(&[&self.plain_terms[last_idx], &slice_plain])?;
                self.plain_terms[last_idx] = combined_plain;

                // Concat typed chunk
                let slice_typed = new_typed.slice(offset, take_len);
                let combined_typed =
                    concat(&[&self.typed_family[last_idx], &slice_typed])?;
                self.typed_family[last_idx] = combined_typed;

                offset += take_len;
            }
        }
        Ok(())
    }

    /// Resolves a bulk of Object IDs into their corresponding RDF Plain Terms
    pub fn resolve_plain_terms(&self, ids: &Int64Array) -> DFResult<ArrayRef> {
        let source: Vec<&dyn Array> =
            self.plain_terms.iter().map(|a| a.as_ref()).collect();
        let indices = self.compute_indices(ids)?;
        Ok(interleave(&source, &indices)?)
    }

    /// Resolves a bulk of Object IDs into their corresponding Typed Values.
    pub fn resolve_typed_values(&self, ids: &Int64Array) -> DFResult<ArrayRef> {
        let source: Vec<&dyn Array> =
            self.typed_family.iter().map(|a| a.as_ref()).collect();
        let indices = self.compute_indices(ids)?;
        Ok(interleave(&source, &indices)?)
    }

    /// Returns a list of RecordBatches for terms between start_id and self.next_id.
    /// Each batch corresponds to the internal CHUNK_SIZE for efficiency.
    pub fn read_batches_since_id(
        &self,
        start_id: i64,
        schema: &SchemaRef,
    ) -> DFResult<Vec<RecordBatch>> {
        let current_next_id = self.next_id;

        if start_id > current_next_id {
            return Ok(vec![]);
        }

        let mut batches = Vec::new();
        let mut current_id = start_id;
        while current_id < current_next_id {
            let id_usize = current_id as usize;
            // Map ID to internal storage (index 0 is null-chunk, so +1)
            let batch_idx = (id_usize / CHUNK_SIZE) + 1;
            let row_offset = id_usize % CHUNK_SIZE;

            let chunk = &self.plain_terms[batch_idx];
            let available_in_chunk = chunk.len() - row_offset;
            let total_remaining = (current_next_id - current_id) as usize;
            let take = available_in_chunk.min(total_remaining);

            // Slice the term array and generate matching IDs
            let ids_slice = Arc::new(Int64Array::from_iter_values(
                current_id..(current_id + take as i64),
            ));
            let terms_slice = chunk.slice(row_offset, take);
            let batch =
                RecordBatch::try_new(Arc::clone(schema), vec![ids_slice, terms_slice])?;

            batches.push(batch);
            current_id += take as i64;
        }

        Ok(batches)
    }

    /// Index computation for resolving chunk routing. A chunk index of 0 corresponds to the null
    /// chunk.
    fn compute_indices(&self, ids: &Int64Array) -> DFResult<Vec<(usize, usize)>> {
        let mut indices = Vec::with_capacity(ids.len());

        for i in 0..ids.len() {
            if ids.is_valid(i) {
                let id = ids.value(i);
                if id < self.next_id {
                    let id_usize = id as usize;
                    // + 1 because index 0 is the null chunk
                    let batch_idx = (id_usize / CHUNK_SIZE) + 1;
                    let row_idx = id_usize % CHUNK_SIZE;
                    indices.push((batch_idx, row_idx));
                } else {
                    return exec_err!("Object ID {} not found in dictionary", id);
                }
            } else {
                indices.push((0, 0));
            }
        }
        Ok(indices)
    }

    pub fn next_id(&self) -> i64 {
        self.next_id
    }

    pub fn get_id_by_term(&self, term: &PlainTermScalar) -> Option<i64> {
        let parts = term.as_parts()?;

        let term_ref = MappedTermRef {
            term_type: parts.term_type,
            value: parts.value,
            datatype: parts.data_type,
            language: parts.language_tag,
        };

        self.term_to_id.get(&term_ref).copied()
    }
}

/// Represents an owned, allocated encoded term for storage in the HashMap.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct MappedTerm {
    term_type: i8,
    value: String,
    datatype: Option<String>,
    language: Option<String>,
}

/// Represents a zero-allocation view into the Arrow arrays for a single row.
#[derive(Debug, Hash, PartialEq, Eq)]
struct MappedTermRef<'a> {
    term_type: i8,
    value: &'a str,
    datatype: Option<&'a str>,
    language: Option<&'a str>,
}

impl<'a> MappedTermRef<'a> {
    #[inline]
    fn from_parts(array: &'a PlainTermArrayParts<'_>, idx: usize) -> Self {
        Self {
            term_type: array.term_type.value(idx),
            value: array.value.value(idx),
            datatype: array
                .data_type
                .is_valid(idx)
                .then(|| array.data_type.value(idx)),
            language: array
                .language_tag
                .is_valid(idx)
                .then(|| array.language_tag.value(idx)),
        }
    }

    /// Converts the borrowed parts into an owned [`MappedTerm`].
    fn to_mapped_term(&self) -> MappedTerm {
        MappedTerm {
            term_type: self.term_type,
            value: self.value.to_owned(),
            datatype: self.datatype.map(|s| s.to_owned()),
            language: self.language.map(|s| s.to_owned()),
        }
    }
}

/// THE MAGIC: Tells hashbrown how to compare our borrowed TermRef against an owned MappedTerm.
impl<'a> Equivalent<MappedTerm> for MappedTermRef<'a> {
    #[inline]
    fn equivalent(&self, key: &MappedTerm) -> bool {
        self.term_type == key.term_type
            && self.value == key.value
            && self.datatype == key.datatype.as_deref()
            && self.language == key.language.as_deref()
    }
}
