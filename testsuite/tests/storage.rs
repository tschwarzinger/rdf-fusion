use anyhow::Result;
use rdf_fusion::api::storage::QuadStorage;
use rdf_fusion::encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
use rdf_fusion::storage::memory::{MemObjectIdMapping, MemQuadStorage};
use rdf_fusion_testsuite::storage::StorageTestSuiteBuilder;
use std::sync::Arc;

#[tokio::test]
async fn mem_storage_testsuite() -> Result<()> {
    StorageTestSuiteBuilder::new(|| async {
        let mapping = Arc::new(MemObjectIdMapping::new());
        let encoding = Arc::new(ObjectIdEncoding::new(
            Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
        ));
        let storage = Arc::new(MemQuadStorage::try_new(encoding, 10).unwrap());
        Ok(storage as Arc<dyn QuadStorage>)
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}
