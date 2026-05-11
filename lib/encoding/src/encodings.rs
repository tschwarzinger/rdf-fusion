use crate::object_id::ObjectIdEncodingRef;
use crate::plain_term::{PlainTermEncoding, PlainTermEncodingRef};
use crate::sortable_term::SortableTermEncodingRef;
use crate::string::{StringEncoding, StringEncodingRef};
use crate::typed_family::TypedFamilyEncodingRef;
use crate::{EncodingName, TermEncoding};
use datafusion::arrow::datatypes::DataType;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Holds a configuration instance for each RDF Fusion encoding.
///
/// This is an instance (as opposed to a type) as some encodings can be configured. At least
/// this is planned for the future. For each RDF Fusion instance, the encodings are fixed once it
/// is created.
///
/// # Equality
///
/// The equality and hashing functions check for pointer equality of the underlying encodings.
#[derive(Debug, Clone)]
pub struct RdfFusionEncodings {
    /// The [`PlainTermEncoding`] configuration.
    plain_term: PlainTermEncodingRef,
    /// The [`TypedFamilyEncoding`](crate::typed_family::TypedFamilyEncoding) configuration.
    typed_family: TypedFamilyEncodingRef,
    /// The [`ObjectIdEncoding`](crate::object_id::ObjectIdEncoding) configuration.
    object_id: Option<ObjectIdEncodingRef>,
    /// The [`SortableTermEncoding`](crate::sortable_term::SortableTermEncoding) configuration.
    sortable_term: SortableTermEncodingRef,
    /// The [`StringEncoding`] configuration.
    string_encoding: StringEncodingRef,
}

impl RdfFusionEncodings {
    /// Creates a new [RdfFusionEncodings].
    pub fn new(
        plain_term: PlainTermEncodingRef,
        typed_family: TypedFamilyEncodingRef,
        object_id: Option<ObjectIdEncodingRef>,
        sortable_term: SortableTermEncodingRef,
        string_encoding: StringEncodingRef,
    ) -> Self {
        Self {
            plain_term,
            typed_family,
            object_id,
            sortable_term,
            string_encoding,
        }
    }

    /// Provides a reference to the used [`PlainTermEncodingRef`].
    pub fn plain_term(&self) -> &PlainTermEncodingRef {
        &self.plain_term
    }

    /// Provides a reference to the used [`TypedFamilyEncodingRef`].
    pub fn typed_family(&self) -> &TypedFamilyEncodingRef {
        &self.typed_family
    }

    /// Provides a reference to the used [`ObjectIdEncodingRef`].
    pub fn object_id(&self) -> Option<&ObjectIdEncodingRef> {
        self.object_id.as_ref()
    }

    /// Provides a reference to the used [`SortableTermEncodingRef`].
    pub fn sortable_term(&self) -> &SortableTermEncodingRef {
        &self.sortable_term
    }

    /// Provides a reference to the used [`StringEncodingRef`].
    pub fn string_encoding(&self) -> &StringEncodingRef {
        &self.string_encoding
    }

    /// Returns a vector of [EncodingName] for the given `names`.
    ///
    /// If some encodings are not defined in this RDF Fusion instance (e.g., no object ID encoding),
    /// the corresponding [EncodingName] is ignored.
    pub fn get_data_types(&self, names: &[EncodingName]) -> Vec<DataType> {
        let mut result = Vec::new();

        if names.contains(&EncodingName::PlainTerm) {
            result.push(self.plain_term.data_type().clone());
        }

        if names.contains(&EncodingName::TypedFamily) {
            result.push(self.typed_family.data_type().clone());
        }

        if let Some(object_id) = self.object_id.as_ref()
            && names.contains(&EncodingName::ObjectId)
        {
            result.push(object_id.as_ref().data_type().clone());
        }

        if names.contains(&EncodingName::Sortable) {
            result.push(self.sortable_term.data_type().clone());
        }

        if names.contains(&EncodingName::String) {
            result.push(self.string_encoding.data_type().clone());
        }

        result
    }

    /// Tries to obtain an [EncodingName] from a [DataType]. As we currently only support built-in
    /// encodings this mapping is unique.
    ///
    /// In the future we might use a field here such that we can access metadata information.
    pub fn try_get_encoding_name(&self, data_type: &DataType) -> Option<EncodingName> {
        if data_type == PlainTermEncoding.data_type() {
            return Some(EncodingName::PlainTerm);
        }

        if data_type == self.typed_family.data_type() {
            return Some(EncodingName::TypedFamily);
        }

        if let Some(object_id) = self.object_id.as_ref()
            && data_type == object_id.data_type()
        {
            return Some(EncodingName::ObjectId);
        }

        if data_type == self.sortable_term.data_type() {
            return Some(EncodingName::Sortable);
        }

        if data_type == StringEncoding.data_type() {
            return Some(EncodingName::String);
        }

        None
    }
}

impl PartialEq for RdfFusionEncodings {
    fn eq(&self, other: &Self) -> bool {
        let object_id_equal = match (&self.object_id, &other.object_id) {
            (Some(a), Some(b)) => Arc::ptr_eq(a, b),
            (None, None) => true,
            _ => false,
        };

        object_id_equal
            && Arc::ptr_eq(&self.plain_term, &other.plain_term)
            && Arc::ptr_eq(&self.typed_family, &other.typed_family)
            && Arc::ptr_eq(&self.sortable_term, &other.sortable_term)
            && Arc::ptr_eq(&self.string_encoding, &other.string_encoding)
    }
}

impl Eq for RdfFusionEncodings {}

impl Hash for RdfFusionEncodings {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_usize(Arc::as_ptr(&self.plain_term) as usize);
        state.write_usize(Arc::as_ptr(&self.typed_family) as usize);
        if let Some(object_id) = &self.object_id {
            state.write_usize(Arc::as_ptr(object_id) as usize);
        }
        state.write_usize(Arc::as_ptr(&self.sortable_term) as usize);
        state.write_usize(Arc::as_ptr(&self.string_encoding) as usize);
    }
}
