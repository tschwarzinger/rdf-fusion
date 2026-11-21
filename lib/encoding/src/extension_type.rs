//! Arrow extension types that mark columns holding RDF terms.
//!
//! Every [`TermEncoding`] has an associated Arrow extension type (e.g.,
//! `rdf-fusion.encoding.plain-term`). Fields that hold RDF terms carry this extension type in their
//! metadata (`ARROW:extension:name`). This allows the query engine to look up the encoding of a
//! column from its [`Field`], instead of guessing the encoding from the [`DataType`].
//!
//! In addition, this module provides [`RdfTermExtensionType`], an implementation of DataFusion's
//! [`DFExtensionType`], that can be registered in a DataFusion session (see
//! [`rdf_term_extension_types`]) to customize how RDF terms are pretty-printed.

use crate::plain_term::decoders::DefaultPlainTermDecoder;
use crate::plain_term::{PlainTermArray, PlainTermEncoding};
use crate::{EncodingName, TermDecoder, TermEncoding};
use datafusion::arrow::array::Array;
use datafusion::arrow::datatypes::{DataType, Field, FieldRef, Schema};
use datafusion::arrow::util::display::{
    ArrayFormatter, DisplayIndex, FormatOptions, FormatResult,
};
use datafusion::common::exec_err;
use datafusion::common::types::{DFExtensionType, DFExtensionTypeRef};
use datafusion::logical_expr::registry::{
    ExtensionTypeRegistration, ExtensionTypeRegistrationRef,
};
use rdf_fusion_common::DFResult;
use std::collections::HashMap;
use std::fmt::Write;
use std::sync::Arc;

/// The metadata key used by Arrow to store the name of an extension type.
///
/// Mirrors `arrow_schema::extension::EXTENSION_TYPE_NAME_KEY`, which is not re-exported by
/// DataFusion.
pub const EXTENSION_TYPE_NAME_KEY: &str = "ARROW:extension:name";

/// The name of the extension type of the [Object ID encoding](crate::object_id::ObjectIdEncoding).
pub const OBJECT_ID_EXTENSION_TYPE_NAME: &str = "rdf-fusion.encoding.object-id";
/// The name of the extension type of the [Plain Term encoding](crate::plain_term::PlainTermEncoding).
pub const PLAIN_TERM_EXTENSION_TYPE_NAME: &str = "rdf-fusion.encoding.plain-term";
/// The name of the extension type of the [Typed Family encoding](crate::typed_family::TypedFamilyEncoding).
pub const TYPED_FAMILY_EXTENSION_TYPE_NAME: &str = "rdf-fusion.encoding.typed-family";
/// The name of the extension type of the [String encoding](crate::string::StringEncoding).
pub const STRING_EXTENSION_TYPE_NAME: &str = "rdf-fusion.encoding.string";

impl EncodingName {
    /// All encoding names.
    pub const ALL: [EncodingName; 4] = [
        EncodingName::ObjectId,
        EncodingName::PlainTerm,
        EncodingName::TypedFamily,
        EncodingName::String,
    ];

    /// Returns the name of the Arrow extension type that marks fields of this encoding.
    pub fn extension_type_name(self) -> &'static str {
        match self {
            EncodingName::ObjectId => OBJECT_ID_EXTENSION_TYPE_NAME,
            EncodingName::PlainTerm => PLAIN_TERM_EXTENSION_TYPE_NAME,
            EncodingName::TypedFamily => TYPED_FAMILY_EXTENSION_TYPE_NAME,
            EncodingName::String => STRING_EXTENSION_TYPE_NAME,
        }
    }

    /// Tries to obtain an [EncodingName] from an Arrow extension type name.
    pub fn try_from_extension_type_name(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|encoding| encoding.extension_type_name() == name)
    }

    /// Reads the [EncodingName] from the extension type metadata of `field`.
    ///
    /// Returns `None` if the field has no RDF Fusion extension type. Note that this function does
    /// not validate whether the data type of the field matches the encoding. Use
    /// [RdfFusionEncodings::try_get_encoding_name](crate::RdfFusionEncodings::try_get_encoding_name)
    /// for this.
    pub fn try_from_field(field: &Field) -> Option<Self> {
        field
            .extension_type_name()
            .and_then(Self::try_from_extension_type_name)
    }

    /// Returns the field metadata that marks a field as an RDF term of this encoding.
    pub fn field_metadata(self) -> HashMap<String, String> {
        HashMap::from([(
            EXTENSION_TYPE_NAME_KEY.to_owned(),
            self.extension_type_name().to_owned(),
        )])
    }
}

/// Creates a new [Field] called `name` for RDF terms in the given `encoding`.
///
/// The field is annotated with the extension type of the encoding.
pub fn rdf_term_field<TEncoding: TermEncoding + ?Sized>(
    name: impl Into<String>,
    encoding: &TEncoding,
    nullable: bool,
) -> Field {
    with_rdf_term_extension_type(
        Field::new(name, encoding.data_type().clone(), nullable),
        encoding.name(),
    )
}

/// Creates a new [FieldRef] called `name` for RDF terms in the given `encoding`.
///
/// See [rdf_term_field].
pub fn rdf_term_field_ref<TEncoding: TermEncoding + ?Sized>(
    name: impl Into<String>,
    encoding: &TEncoding,
    nullable: bool,
) -> FieldRef {
    Arc::new(rdf_term_field(name, encoding, nullable))
}

/// Annotates an existing `field` with the extension type of `encoding`, replacing a previous RDF
/// Fusion extension type (if any). Other metadata is retained.
pub fn with_rdf_term_extension_type(field: Field, encoding: EncodingName) -> Field {
    let mut metadata = field.metadata().clone();
    metadata.insert(
        EXTENSION_TYPE_NAME_KEY.to_owned(),
        encoding.extension_type_name().to_owned(),
    );
    field.with_metadata(metadata)
}

/// Removes the RDF Fusion extension type from `field` (if any). Other metadata is retained.
///
/// RDF Fusion does not persist the extension types in files. Storage layers use this function to
/// obtain the schema of the stored data.
pub fn without_rdf_term_extension_type(field: Field) -> Field {
    if EncodingName::try_from_field(&field).is_none() {
        return field;
    }
    let mut metadata = field.metadata().clone();
    metadata.remove(EXTENSION_TYPE_NAME_KEY);
    field.with_metadata(metadata)
}

/// Removes the RDF Fusion extension types from all fields of `schema`.
///
/// See [without_rdf_term_extension_type].
pub fn without_rdf_term_extension_types(schema: &Schema) -> Schema {
    let fields = schema
        .fields()
        .iter()
        .map(|f| without_rdf_term_extension_type(f.as_ref().clone()))
        .collect::<Vec<_>>();
    Schema::new_with_metadata(fields, schema.metadata().clone())
}

/// DataFusion's extension type implementation for RDF terms.
///
/// Currently, this only customizes the pretty-printing of RDF terms. For example, plain terms are
/// printed in their N-Triples representation instead of their raw struct representation.
#[derive(Debug, Clone)]
pub struct RdfTermExtensionType {
    /// The encoding of the field.
    encoding: EncodingName,
    /// The storage type of the field.
    storage_type: DataType,
}

impl RdfTermExtensionType {
    /// Creates a new [RdfTermExtensionType].
    pub fn new(encoding: EncodingName, storage_type: DataType) -> Self {
        Self {
            encoding,
            storage_type,
        }
    }
}

impl DFExtensionType for RdfTermExtensionType {
    fn storage_type(&self) -> DataType {
        self.storage_type.clone()
    }

    fn serialize_metadata(&self) -> Option<String> {
        None
    }

    fn create_array_formatter<'fmt>(
        &self,
        array: &'fmt dyn Array,
        options: &FormatOptions<'fmt>,
    ) -> DFResult<Option<ArrayFormatter<'fmt>>> {
        match self.encoding {
            EncodingName::PlainTerm => {
                if *array.data_type() != PlainTermEncoding::data_type() {
                    return exec_err!(
                        "Unexpected data type for plain term array: {}",
                        array.data_type()
                    );
                }
                let array = PlainTermArray::new_unchecked(array.slice(0, array.len()));
                let values = DefaultPlainTermDecoder::decode_terms(&array)
                    .map(|term| term.ok().map(|term| term.to_string()))
                    .collect();
                let display_index = PlainTermDisplayIndex {
                    values,
                    null_str: options.null(),
                };
                Ok(Some(ArrayFormatter::new(
                    Box::new(display_index),
                    options.safe(),
                )))
            }
            // The other encodings use the default formatting for now.
            EncodingName::ObjectId | EncodingName::TypedFamily | EncodingName::String => {
                Ok(None)
            }
        }
    }
}

/// Pretty printer for plain term arrays.
///
/// The values are formatted eagerly, as this is only used for displaying (small) results.
struct PlainTermDisplayIndex<'a> {
    values: Vec<Option<String>>,
    null_str: &'a str,
}

impl DisplayIndex for PlainTermDisplayIndex<'_> {
    fn write(&self, idx: usize, f: &mut dyn Write) -> FormatResult {
        match &self.values[idx] {
            Some(term) => f.write_str(term)?,
            None => f.write_str(self.null_str)?,
        }
        Ok(())
    }
}

/// Returns the [ExtensionTypeRegistration]s for all RDF Fusion encodings. These can be registered
/// in a DataFusion session (e.g., via
/// [`SessionStateBuilder::with_extension_type_registry`](datafusion::execution::SessionStateBuilder)).
pub fn rdf_term_extension_types() -> Vec<ExtensionTypeRegistrationRef> {
    EncodingName::ALL
        .into_iter()
        .map(|encoding| {
            ExtensionTypeRegistration::new_arc(
                encoding.extension_type_name(),
                move |storage_type: &DataType,
                      _metadata: Option<&str>|
                      -> DFResult<DFExtensionTypeRef> {
                    Ok(Arc::new(RdfTermExtensionType::new(
                        encoding,
                        storage_type.clone(),
                    )))
                },
            )
        })
        .collect()
}
