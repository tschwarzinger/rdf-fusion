use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::prelude::SessionContext;
use deltalake::logstore::{IORuntime, LogStore, StorageConfig, logstore_with};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use rdf_fusion_common::NamedNodeRef;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::plain_term::{PlainTermArrayElementBuilder, PlainTermEncoding};
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_storage::delta::DeltaQuadStorage;
use std::sync::Arc;
use tokio::runtime::Handle;
use url::Url;

mod persistence;
mod refresh;

fn create_test_log_store() -> Arc<dyn LogStore> {
    let object_store = Arc::new(InMemory::new());
    let base_url = Url::parse("memory:///").unwrap();
    logstore_with(
        Arc::clone(&object_store) as Arc<dyn ObjectStore>,
        &base_url,
        StorageConfig::default().with_io_runtime(IORuntime::RT(Handle::current())),
    )
    .unwrap()
}

fn create_test_quads(ctx: &SessionContext, s: &str) -> datafusion::dataframe::DataFrame {
    let data_type = PlainTermEncoding::data_type().clone();
    let schema = Arc::new(Schema::new(vec![
        Field::new(COL_GRAPH, data_type.clone(), true),
        Field::new(COL_SUBJECT, data_type.clone(), true),
        Field::new(COL_PREDICATE, data_type.clone(), true),
        Field::new(COL_OBJECT, data_type, true),
    ]));
    let mut graph_builder = PlainTermArrayElementBuilder::new();
    let mut subject_builder = PlainTermArrayElementBuilder::new();
    let mut predicate_builder = PlainTermArrayElementBuilder::new();
    let mut object_builder = PlainTermArrayElementBuilder::new();

    graph_builder.append_null();
    subject_builder.append_named_node(NamedNodeRef::new_unchecked(s));
    predicate_builder
        .append_named_node(NamedNodeRef::new_unchecked("http://example.org/p"));
    object_builder.append_named_node(NamedNodeRef::new_unchecked("http://example.org/o"));

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(graph_builder.finish().into_array_ref()),
            Arc::new(subject_builder.finish().into_array_ref()),
            Arc::new(predicate_builder.finish().into_array_ref()),
            Arc::new(object_builder.finish().into_array_ref()),
        ],
    )
    .unwrap();
    ctx.read_batch(batch).unwrap()
}

fn create_context(storage: Arc<dyn QuadStorage>) -> SessionContext {
    RdfFusionContextBuilder::new(storage)
        .build()
        .unwrap()
        .session_context()
        .clone()
}

async fn populate_storage(storage: Arc<DeltaQuadStorage>, s: &str) {
    let ctx = create_context(Arc::clone(&storage) as Arc<dyn QuadStorage>);
    let transaction = storage.begin_transaction(&ctx.state()).await.unwrap();
    let quad = create_test_quads(&ctx, s);
    transaction.insert(quad).await.unwrap();
    transaction.commit().await.unwrap();
}
