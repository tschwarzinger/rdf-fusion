//! Persists the RDF term extension types in the schemas of the Delta tables.
//!
//! Delta schemas support arbitrary field metadata, which is carried over to the Arrow schema of the
//! table (and the Parquet files written by delta-rs). Hence, storing the Arrow extension type name
//! in the field metadata of the Delta schema allows reading the encoding of the columns back from
//! the table.

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use deltalake::{DataType as DeltaDataType, StructField};
use rdf_fusion_encoding::{
    EXTENSION_TYPE_NAME_KEY, EncodingName, QuadStorageEncoding,
    with_rdf_term_extension_type,
};
use rdf_fusion_extensions::storage::infer_encoding_from_storage_type;
use std::sync::Arc;

/// Creates a Delta [`StructField`] for a column holding RDF terms in the given `encoding`.
///
/// The field metadata contains the name of the extension type of the encoding.
pub(crate) fn rdf_term_struct_field(
    name: &str,
    data_type: DeltaDataType,
    nullable: bool,
    encoding: &QuadStorageEncoding,
) -> StructField {
    StructField::new(name, data_type, nullable).with_metadata([(
        EXTENSION_TYPE_NAME_KEY,
        encoding.encoding_name().extension_type_name(),
    )])
}

/// Ensures that the `term_columns` of a schema read from a Delta table carry the RDF term
/// extension types.
///
/// Tables written by this version of RDF Fusion store the extension types in their schema (see
/// [`rdf_term_struct_field`]). For tables created by older versions, the encoding is inferred from
/// the data type.
pub(crate) fn annotate_loaded_schema(
    schema: &SchemaRef,
    term_columns: &[&str],
) -> SchemaRef {
    let fields = schema
        .fields()
        .iter()
        .map(|field| {
            let is_term_column = term_columns.contains(&field.name().as_str());
            if !is_term_column || EncodingName::try_from_field(field).is_some() {
                return field.as_ref().clone();
            }
            match infer_encoding_from_storage_type(field) {
                Some(encoding) => {
                    with_rdf_term_extension_type(field.as_ref().clone(), encoding)
                }
                None => field.as_ref().clone(),
            }
        })
        .collect::<Vec<Field>>();
    Arc::new(Schema::new_with_metadata(fields, schema.metadata().clone()))
}
