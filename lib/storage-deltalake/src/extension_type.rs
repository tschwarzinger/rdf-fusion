//! Attaches the RDF term extension types to the schemas read from the Delta tables.
//!
//! RDF Fusion does not persist the extension types in the Delta tables or their Parquet files. The
//! encoding of a Delta storage is determined by the data type of its term columns (see
//! `DeltaQuadsStorage::try_load`). Hence, the extension types are attached when the schema of a
//! table is read.

use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use rdf_fusion_encoding::{EncodingName, with_rdf_term_extension_type};
use rdf_fusion_extensions::storage::infer_encoding_from_storage_type;
use std::sync::Arc;

/// Attaches the RDF term extension types to the `term_columns` of a `schema` read from a Delta
/// table.
///
/// The encoding of each column is determined by its data type, in the same way as the encoding of
/// the storage is detected when loading it.
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
