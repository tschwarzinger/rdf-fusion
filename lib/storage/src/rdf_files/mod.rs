//! Contains implementations related to querying RDF data (e.g., Turtle).

pub mod rdf;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
pub use rdf::*;
use rdf_fusion_common::DFResult;
use rdf_fusion_common::quads::COL_SUBJECT;
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{QuadStorageEncoding, TermEncoding};

/// Detects the encoding from the given schema.
pub fn detect_encoding_from_schema(schema: &SchemaRef) -> DFResult<QuadStorageEncoding> {
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
