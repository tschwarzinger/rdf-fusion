use datafusion::arrow::datatypes::{DataType, Field};
use datafusion::common::metadata::FieldMetadata;
use datafusion::dataframe::DataFrame;
use datafusion::logical_expr::{Cast, Expr};
use datafusion::prelude::col;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::plain_term::PlainTermEncoding;
use rdf_fusion_encoding::string::StringEncoding;
use rdf_fusion_encoding::{EncodingName, with_rdf_term_extension_type};
use std::sync::Arc;

/// Ensures that all columns of `df` carry an RDF term extension type.
///
/// Within the query engine, RDF terms are always annotated with an Arrow extension type (see
/// [`EncodingName::extension_type_name`]). However, data that enters a storage layer from the
/// outside (e.g., a [`DataFrame`] created by a user or data read from Parquet files without
/// extension type metadata) may not carry this information. This function infers the encoding
/// of unannotated columns from their data type.
///
/// This is the only place where the encoding is inferred from the data type, which is why it should
/// only be used at the boundary of a storage layer.
pub fn ensure_rdf_term_extension_types(df: DataFrame) -> DFResult<DataFrame> {
    let needs_annotation = df
        .schema()
        .fields()
        .iter()
        .any(|f| EncodingName::try_from_field(f).is_none());
    if !needs_annotation {
        return Ok(df);
    }

    let exprs = df
        .schema()
        .fields()
        .iter()
        .map(|field| {
            let name = field.name();
            let expr = col(datafusion::common::Column::new_unqualified(name));
            match (
                EncodingName::try_from_field(field),
                infer_encoding_from_storage_type(field),
            ) {
                // The cast (to the same storage type) ensures that the physical plan carries the
                // extension type, as it survives optimizations such as merging projections. The
                // alias ensures that the logical plan carries the extension type.
                (None, Some(encoding)) => {
                    let field =
                        with_rdf_term_extension_type(field.as_ref().clone(), encoding);
                    let metadata = FieldMetadata::from(field.metadata());
                    Expr::Cast(Cast::new_from_field(Box::new(expr), Arc::new(field)))
                        .alias_with_metadata(name, Some(metadata))
                }
                _ => expr,
            }
        })
        .collect::<Vec<_>>();
    df.select(exprs)
}

/// Infers the encoding of a stored column from its data type.
///
/// Only the encodings that can be used by quad storages are considered.
pub fn infer_encoding_from_storage_type(field: &Field) -> Option<EncodingName> {
    match field.data_type() {
        dt if *dt == PlainTermEncoding::data_type() => Some(EncodingName::PlainTerm),
        dt if *dt == StringEncoding::data_type() => Some(EncodingName::String),
        DataType::Int32 | DataType::Int64 | DataType::FixedSizeBinary(_) => {
            Some(EncodingName::ObjectId)
        }
        _ => None,
    }
}
