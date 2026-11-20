mod kleene_plus;
mod path_node;

use datafusion::arrow::datatypes::{Fields, SchemaRef};
use datafusion::common::{DFSchema, DFSchemaRef};
pub use kleene_plus::*;
pub use path_node::*;
use rdf_fusion_encoding::QuadStorageEncoding;
use std::clone::Clone;
use std::collections::HashMap;
use std::sync::{Arc, LazyLock};

pub const COL_PATH_GRAPH: &str = "_graph";
pub const COL_PATH_SOURCE: &str = "_source";
pub const COL_PATH_TARGET: &str = "_target";

/// Currently, property paths only work in the plain term encoding, as the physical Kleene node
/// operator only works for this encoding.
///
/// If the storage layer uses object ids, they are immediately decoded after loading.
pub static PATH_TABLE_SCHEMA: LazyLock<SchemaRef> =
    LazyLock::new(|| Arc::clone(PATH_TABLE_DFSCHEMA.inner()));

pub static PATH_TABLE_DFSCHEMA: LazyLock<DFSchemaRef> =
    LazyLock::new(|| create_path_table_schema(&QuadStorageEncoding::PlainTerm));

#[allow(clippy::expect_used)]
fn create_path_table_schema(storage_encoding: &QuadStorageEncoding) -> DFSchemaRef {
    Arc::new(
        DFSchema::from_unqualified_fields(
            Fields::from(vec![
                storage_encoding.term_field(COL_PATH_GRAPH, true),
                storage_encoding.term_field(COL_PATH_SOURCE, false),
                storage_encoding.term_field(COL_PATH_TARGET, false),
            ]),
            HashMap::new(),
        )
        .expect("Names fixed"),
    )
}
