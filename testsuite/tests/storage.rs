use anyhow::{Error, Result};
use datafusion::prelude::SessionConfig;
use rdf_fusion::api::storage::QuadStorage;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion::storage::delta::DeltaQuadStorage;
use rdf_fusion::storage::index::IndexComponents;
use rdf_fusion_testsuite::storage::StorageTestSuiteBuilder;
use std::sync::Arc;

#[tokio::test]
async fn mem_storage_testsuite_without_index() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_plain_term_encoding(vec![]).await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn mem_storage_testsuite_with_index() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_plain_term_encoding(vec![
            IndexComponents::GSPO,
            IndexComponents::GPOS,
            IndexComponents::GOSP,
        ])
        .await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn mem_storage_object_id_testsuite_without_index() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_object_id(vec![]).await
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn mem_storage_object_id_testsuite_with_index() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        create_delta_storage_with_object_id(vec![
            IndexComponents::GSPO,
            IndexComponents::GPOS,
            IndexComponents::GOSP,
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
    indexes: Vec<IndexComponents>,
) -> Result<Arc<dyn QuadStorage>, Error> {
    let mut config = SessionConfig::default();
    config.options_mut().execution.target_partitions = 1;

    let storage = DeltaQuadStorage::new_default_in_memory(
        indexes,
        Arc::new(TypedFamilyEncoding::default()),
    )
    .await;
    Ok(Arc::new(storage) as Arc<dyn QuadStorage>)
}

async fn create_delta_storage_with_object_id(
    indexes: Vec<IndexComponents>,
) -> Result<Arc<dyn QuadStorage>, Error> {
    let storage = DeltaQuadStorage::new_in_memory(
        QuadStorageEncodingName::ObjectId,
        indexes,
        Arc::new(TypedFamilyEncoding::default()),
    )
    .await;
    Ok(Arc::new(storage) as Arc<dyn QuadStorage>)
}
