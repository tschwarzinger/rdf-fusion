use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use deltalake::logstore::{logstore_with, LogStore, StorageConfig};
use object_store::memory::InMemory;
use object_store::ObjectStore;
use rdf_fusion_encoding::plain_term::{PlainTermArrayElementBuilder, PlainTermEncoding};
use rdf_fusion_encoding::EncodingArray;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::NamedNodeRef;
use rdf_fusion_storage::delta::DeltaQuadStorage;
use rdf_fusion_storage::delta::DeltaQuadStorageBuilder;
use rdf_fusion_storage::index::IndexComponents;
use std::sync::Arc;
use url::Url;

#[tokio::test]
async fn test_reload_storage_plain_term() {
    let log_store = create_test_log_store();
    let session = SessionStateBuilder::new().build();

    // 1. Create and populate storage
    {
        let storage = Arc::new(
            DeltaQuadStorageBuilder::new()
                .with_log_store(Arc::clone(&log_store))
                .with_encoding(QuadStorageEncodingName::PlainTerm)
                .build()
                .await
                .unwrap(),
        );

        populate_storage(storage, "http://example.org/s1").await;
    }

    // 2. Reload and verify
    {
        let storage = DeltaQuadStorage::try_load(&session, Arc::clone(&log_store))
            .await
            .unwrap();

        assert_eq!(storage.log().version().await, 1);
    }
}

#[tokio::test]
async fn test_reload_storage_with_index_and_optimize() {
    let log_store = create_test_log_store();
    let session = SessionStateBuilder::new().build();

    // 1. Create storage with indexes
    {
        let storage = Arc::new(
            DeltaQuadStorageBuilder::new()
                .with_log_store(Arc::clone(&log_store))
                .with_encoding(QuadStorageEncodingName::PlainTerm)
                .with_indexes(vec![IndexComponents::GSPO])
                .build()
                .await
                .unwrap(),
        );

        populate_storage(Arc::clone(&storage), "http://example.org/s1").await;

        let ctx = create_context(Arc::clone(&storage) as Arc<dyn QuadStorage>);
        storage.optimize(&ctx.state()).await.unwrap();
    }

    // 2. Reload, add more data, and optimize again
    {
        let storage = Arc::new(
            DeltaQuadStorage::try_load(&session, Arc::clone(&log_store))
                .await
                .unwrap(),
        );

        populate_storage(Arc::clone(&storage), "http://example.org/s2").await;

        let ctx = create_context(Arc::clone(&storage) as Arc<dyn QuadStorage>);
        storage.optimize(&ctx.state()).await.unwrap();
        assert_eq!(storage.log().version().await, 2);
    }
}

#[tokio::test]
async fn test_reload_storage_object_id() {
    let log_store = create_test_log_store();
    let session = SessionStateBuilder::new().build();

    // 1. Create and populate storage
    {
        let storage = Arc::new(
            DeltaQuadStorageBuilder::new()
                .with_log_store(Arc::clone(&log_store))
                .with_encoding(QuadStorageEncodingName::ObjectId)
                .build()
                .await
                .unwrap(),
        );

        populate_storage(Arc::clone(&storage), "http://example.org/s1").await;

        storage
            .delta_object_id_mapping()
            .unwrap()
            .flush()
            .await
            .unwrap();
    }

    // 2. Reload and verify
    {
        let storage = DeltaQuadStorage::try_load(&session, Arc::clone(&log_store))
            .await
            .unwrap();

        assert!(storage.delta_object_id_mapping().is_some());
        assert_eq!(storage.log().version().await, 1);
    }
}

fn create_test_log_store() -> Arc<dyn LogStore> {
    let object_store = Arc::new(InMemory::new());
    let base_url = Url::parse("memory:///").unwrap();
    logstore_with(
        Arc::clone(&object_store) as Arc<dyn ObjectStore>,
        &base_url,
        StorageConfig::default(),
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
