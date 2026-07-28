use anyhow::Result;
use rdf_fusion::storage::local_object_ids::{
    InMemoryObjectIdDictionary, LmdbObjectIdDictionary, LocalObjectIdDictionary,
    StaticObjectIdClaimer,
};
use rdf_fusion_testsuite::dictionary::DictionaryTestSuiteBuilder;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn dictionary_testsuite_in_memory() -> Result<()> {
    DictionaryTestSuiteBuilder::new(|| async {
        let dict: Arc<dyn LocalObjectIdDictionary> = Arc::new(
            InMemoryObjectIdDictionary::new(Arc::new(StaticObjectIdClaimer)),
        );
        Ok(dict)
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[allow(deprecated)]
async fn dictionary_testsuite_lmdb() -> Result<()> {
    DictionaryTestSuiteBuilder::new(|| async {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.into_path();
        let dict: Arc<dyn LocalObjectIdDictionary> =
            Arc::new(LmdbObjectIdDictionary::try_new(
                path,
                1_000_000,
                Arc::new(StaticObjectIdClaimer),
            )?);
        Ok(dict)
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
#[allow(deprecated)]
async fn dictionary_testsuite_lmdb_no_cache() -> Result<()> {
    DictionaryTestSuiteBuilder::new(|| async {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.into_path();
        let dict: Arc<dyn LocalObjectIdDictionary> = Arc::new(
            LmdbObjectIdDictionary::try_new(path, 0, Arc::new(StaticObjectIdClaimer))?,
        );
        Ok(dict)
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}
