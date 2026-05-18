//! Contains implementations related to querying data dumps (e.g., Turtle, Parquet).

mod manager;
mod planner;
mod rdf;
mod scan;
mod snapshot;
mod storage;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
pub use manager::*;
pub use planner::*;
pub use rdf::*;
use rdf_fusion_common::DFResult;
use rdf_fusion_common::quads::COL_SUBJECT;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{QuadStorageEncoding, TermEncoding};
pub use storage::*;

/// Detects the encoding from the given schema.
pub(crate) fn detect_encoding_from_schema(
    schema: &SchemaRef,
) -> DFResult<QuadStorageEncoding> {
    let subject_field = schema.field_with_name(COL_SUBJECT)?;
    let dt = subject_field.data_type();
    if dt == PLAIN_TERM_ENCODING.data_type() {
        Ok(QuadStorageEncoding::PlainTerm)
    } else if dt == STRING_ENCODING.data_type() {
        Ok(QuadStorageEncoding::String)
    } else {
        exec_err!("Unsupported encoding data type: {:?}", dt)
    }
}
