use crate::delta::{create_test_log_store, populate_storage};
use datafusion::execution::SessionStateBuilder;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_storage::delta::{DeltaQuadStorage, DeltaQuadStorageBuilder, LoadMode};
use std::sync::Arc;
use std::time::Duration;

#[tokio::test]
async fn test_stale_refresh() {
    // max_age = 0 triggers a refresh on every query
    let (storage1, storage2) = setup_test_storages(Some(Duration::from_millis(0))).await;

    // 1. storage1 writes a quad
    populate_storage(Arc::clone(&storage1), "http://example.org/s1").await;
    assert_eq!(storage1.log().version().await, 1);

    // 2. storage2 should still see version 0 locally before a snapshot
    assert_eq!(storage2.log().version().await, 0);

    // 3. storage2 takes a snapshot, triggering a refresh
    let _snapshot = storage2.snapshot().await.unwrap();
    assert_eq!(storage2.log().version().await, 1);

    // 4. storage1 writes another quad
    populate_storage(Arc::clone(&storage1), "http://example.org/s2").await;
    assert_eq!(storage1.log().version().await, 2);

    // 5. storage2 takes another snapshot
    let _snapshot2 = storage2.snapshot().await.unwrap();
    assert_eq!(storage2.log().version().await, 2);
}

#[tokio::test]
async fn test_staleness_tolerance() {
    let (storage1, storage2) = setup_test_storages(Some(Duration::from_secs(3600))).await;

    // Trigger an initial refresh so storage2's `last_success` is initialized to NOW
    let _ = storage2.snapshot().await.unwrap();

    // storage1 writes a new version
    populate_storage(Arc::clone(&storage1), "http://example.org/s1").await;
    assert_eq!(storage1.log().version().await, 1);

    // storage2 takes a snapshot returns the STALE version (0).
    let _snapshot = storage2.snapshot().await.unwrap();
    assert_eq!(storage2.log().version().await, 0);
}

#[tokio::test]
async fn test_concurrent_refreshes() {
    let (storage1, storage2) = setup_test_storages(Some(Duration::from_millis(0))).await;

    // storage1 writes a new version
    populate_storage(Arc::clone(&storage1), "http://example.org/s1").await;
    assert_eq!(storage1.log().version().await, 1);

    // storage2 is hit with 100 simultaneous snapshot requests
    let mut tasks = Vec::new();
    for _ in 0..100 {
        let s2_clone = Arc::clone(&storage2);
        tasks.push(tokio::spawn(
            async move { s2_clone.snapshot().await.unwrap() },
        ));
    }

    // Wait for all concurrent snapshots to finish.
    for task in tasks {
        let _ = task.await.unwrap();
    }

    // storage2 should have updated the log
    assert_eq!(storage2.log().version().await, 1);
}

#[tokio::test]
async fn test_disabled_refresh() {
    let (storage1, storage2) = setup_test_storages(None).await;

    populate_storage(Arc::clone(&storage1), &format!("http://example.org/s{}", 1)).await;
    assert_eq!(storage1.log().version().await, 1);

    let _snapshot = storage2.snapshot().await.unwrap();
    assert_eq!(storage2.log().version().await, 0);
}

/// Helper function to create two connected storage nodes sharing a log store.
async fn setup_test_storages(
    max_age_second_storage: Option<Duration>,
) -> (Arc<DeltaQuadStorage>, Arc<DeltaQuadStorage>) {
    let log_store = create_test_log_store();

    let storage1 = Arc::new(
        DeltaQuadStorageBuilder::new()
            .with_log_store(Arc::clone(&log_store))
            .with_encoding(QuadStorageEncodingName::PlainTerm)
            .build()
            .await
            .unwrap(),
    );

    let storage2 = Arc::new(
        DeltaQuadStorageBuilder::new()
            .with_log_store(Arc::clone(&log_store))
            .with_encoding(QuadStorageEncodingName::PlainTerm)
            .with_load_mode(LoadMode::Load(Box::new(SessionStateBuilder::new().build())))
            .build()
            .await
            .unwrap(),
    );

    storage2
        .set_transaction_max_age(max_age_second_storage)
        .await;

    // Sanity check initial state
    assert_eq!(storage1.log().version().await, 0);
    assert_eq!(storage2.log().version().await, 0);

    (storage1, storage2)
}
