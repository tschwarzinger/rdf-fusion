use crate::object_id::ObjectIdEncodingRef;
use crate::plain_term::PlainTermEncodingRef;
use crate::string::StringEncodingRef;
use crate::typed_family::TypedFamilyEncodingRef;
use crate::{EncodingName, TermEncoding};
use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::ScalarValue;
use datafusion::common::metadata::FieldMetadata;
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
    /// The [`PlainTermEncoding`](crate::plain_term::PlainTermEncoding) configuration.
    plain_term: PlainTermEncodingRef,
    /// The [`TypedFamilyEncoding`](crate::typed_family::TypedFamilyEncoding) configuration.
    typed_family: TypedFamilyEncodingRef,
    /// The [`ObjectIdEncoding`](crate::object_id::ObjectIdEncoding) configuration.
    object_id: Option<ObjectIdEncodingRef>,
    /// The [`StringEncoding`](crate::string::StringEncoding) configuration.
    string_encoding: StringEncodingRef,
}

impl RdfFusionEncodings {
    /// Creates a new [RdfFusionEncodings].
    pub fn new(
        plain_term: PlainTermEncodingRef,
        typed_family: TypedFamilyEncodingRef,
        object_id: Option<ObjectIdEncodingRef>,
        string_encoding: StringEncodingRef,
    ) -> Self {
        Self {
            plain_term,
            typed_family,
            object_id,
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

        if names.contains(&EncodingName::String) {
            result.push(self.string_encoding.data_type().clone());
        }

        result
    }

    /// Returns the [DataType] of the encoding called `name`.
    ///
    /// Returns `None` if the encoding is not configured in this RDF Fusion instance (e.g., no
    /// object ID encoding).
    pub fn data_type(&self, name: EncodingName) -> Option<&DataType> {
        match name {
            EncodingName::ObjectId => self.object_id.as_ref().map(|oid| oid.data_type()),
            EncodingName::PlainTerm => Some(self.plain_term.data_type()),
            EncodingName::TypedFamily => Some(self.typed_family.data_type()),
            EncodingName::String => Some(self.string_encoding.data_type()),
        }
    }

    /// Creates a [Field] called `field_name` that holds RDF terms of the encoding `name`.
    ///
    /// The field is annotated with the extension type of the encoding. Returns `None` if the
    /// encoding is not configured in this RDF Fusion instance.
    pub fn field(
        &self,
        name: EncodingName,
        field_name: impl Into<String>,
        nullable: bool,
    ) -> Option<Field> {
        let data_type = self.data_type(name)?;
        Some(
            Field::new(field_name, data_type.clone(), nullable)
                .with_metadata(name.field_metadata()),
        )
    }

    /// Tries to obtain the [EncodingName] of a [Field].
    ///
    /// The encoding is determined by the extension type of the field (see
    /// [EncodingName::extension_type_name]). Returns `None` if the field is not annotated with an
    /// RDF Fusion extension type, if the encoding is not configured in this instance, or if the
    /// data type of the field does not match the data type of the configured encoding.
    pub fn try_get_encoding_name(&self, field: &Field) -> Option<EncodingName> {
        let name = EncodingName::try_from_field(field)?;
        (self.data_type(name)? == field.data_type()).then_some(name)
    }

    /// Tries to obtain the [EncodingName] of a literal `value` with the given `metadata` (e.g., from
    /// an `Expr::Literal`).
    ///
    /// See [Self::try_get_encoding_name].
    pub fn try_get_literal_encoding_name(
        &self,
        value: &ScalarValue,
        metadata: Option<&FieldMetadata>,
    ) -> Option<EncodingName> {
        let field = Field::new("", value.data_type(), true);
        let field = match metadata {
            None => field,
            Some(metadata) => metadata.add_to_field(field),
        };
        self.try_get_encoding_name(&field)
    }

    /// Returns the encodings (configured in this instance) whose data type is `data_type`.
    ///
    /// This should only be used in places where no [Field] is available (e.g., DataFusion type
    /// signatures). Prefer [Self::try_get_encoding_name] whenever possible.
    pub fn encodings_with_data_type(&self, data_type: &DataType) -> Vec<EncodingName> {
        EncodingName::ALL
            .into_iter()
            .filter(|name| self.data_type(*name) == Some(data_type))
            .collect()
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
        state.write_usize(Arc::as_ptr(&self.string_encoding) as usize);
    }
}
