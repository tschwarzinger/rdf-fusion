use anyhow::{Error, Result};
use datafusion::prelude::SessionConfig;
use rdf_fusion::api::storage::QuadStorage;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::storage::delta::DeltaQuadsStorage;
use rdf_fusion::storage::quad_tables::QuadTableName;
use rdf_fusion_testsuite::storage::StorageTestSuiteBuilder;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn delta_storage_testsuite_without_quad_table() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_plain_term_encoding(vec![]).await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn delta_storage_testsuite_with_quad_table() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_plain_term_encoding(vec![
            QuadTableName::GSPO,
            QuadTableName::GPOS,
            QuadTableName::GOSP,
        ])
        .await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn delta_storage_object_id_testsuite_without_quad_table() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_object_id(vec![]).await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn delta_storage_object_id_testsuite_with_quad_table() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_object_id(vec![
            QuadTableName::GSPO,
            QuadTableName::GPOS,
            QuadTableName::GOSP,
        ])
        .await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn delta_storage_string_testsuite_without_quad_table() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_string(vec![]).await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn delta_storage_string_testsuite_with_quad_table() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_string(vec![
            QuadTableName::GSPO,
            QuadTableName::GPOS,
            QuadTableName::GOSP,
        ])
        .await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

async fn create_delta_storage_with_plain_term_encoding(
    quad_tables: Vec<QuadTableName>,
) -> Result<Arc<dyn QuadStorage>, Error> {
    let mut config = SessionConfig::default();
    config.options_mut().execution.target_partitions = 1;

    let storage =
        DeltaQuadsStorage::new_in_memory(QuadStorageEncodingName::PlainTerm, quad_tables)
            .await;
    Ok(Arc::new(storage) as Arc<dyn QuadStorage>)
}

async fn create_delta_storage_with_object_id(
    quad_tables: Vec<QuadTableName>,
) -> Result<Arc<dyn QuadStorage>, Error> {
    let storage =
        DeltaQuadsStorage::new_in_memory(QuadStorageEncodingName::ObjectId, quad_tables)
            .await;
    Ok(Arc::new(storage) as Arc<dyn QuadStorage>)
}

async fn create_delta_storage_with_string(
    quad_tables: Vec<QuadTableName>,
) -> Result<Arc<dyn QuadStorage>, Error> {
    let storage =
        DeltaQuadsStorage::new_in_memory(QuadStorageEncodingName::String, quad_tables)
            .await;
    Ok(Arc::new(storage) as Arc<dyn QuadStorage>)
}
