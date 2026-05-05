use crate::delta::{create_context, create_test_log_store, populate_storage};
use datafusion::execution::SessionStateBuilder;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_storage::delta::DeltaQuadStorage;
use rdf_fusion_storage::delta::DeltaQuadStorageBuilder;
use rdf_fusion_storage::index::IndexComponents;
use std::sync::Arc;

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
async fn test_load_storage_object_id() {
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
