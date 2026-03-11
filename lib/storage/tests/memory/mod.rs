mod mem_quad_storage;
mod parquet;

use rdf_fusion_encoding::RdfFusionEncodings;
use rdf_fusion_encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::sortable_term::SORTABLE_TERM_ENCODING;
use rdf_fusion_encoding::typed_value::TypedValueEncoding;
use rdf_fusion_extensions::functions::RdfFusionFunctionRegistryRef;
use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion_storage::memory::{MemObjectIdMapping, MemQuadStorage};
use std::sync::Arc;

fn create_storage() -> MemQuadStorage {
    let mapping = Arc::new(MemObjectIdMapping::new());
    let encoding = Arc::new(ObjectIdEncoding::new(
        Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
    ));
    MemQuadStorage::try_new(encoding, 10).unwrap()
}

fn create_function_registry(
    object_id_mapping: Arc<dyn ObjectIdMapping>,
) -> RdfFusionFunctionRegistryRef {
    let encoding = RdfFusionEncodings::new(
        Arc::clone(&PLAIN_TERM_ENCODING),
        Arc::new(TypedValueEncoding::default()),
        Some(Arc::new(ObjectIdEncoding::new(object_id_mapping))),
        Arc::clone(&SORTABLE_TERM_ENCODING),
    );
    Arc::new(DefaultRdfFusionFunctionRegistry::new(encoding))
}
