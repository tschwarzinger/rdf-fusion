use anyhow::Result;
use rdf_fusion::storage::local_object_ids::{
    LocalObjectIdDictionary, RedbObjectIdDictionaryBuilder, StaticObjectIdClaimer,
};
use rdf_fusion_testsuite::dictionary::DictionaryTestSuiteBuilder;
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn dictionary_testsuite_redb_in_memory_no_cache() -> Result<()> {
    DictionaryTestSuiteBuilder::new(|| async {
        let dict: Arc<dyn LocalObjectIdDictionary> = Arc::new(
            RedbObjectIdDictionaryBuilder::new_in_memory()
                .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
                .finish()?,
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
async fn dictionary_testsuite_redb_on_disk_with_cache() -> Result<()> {
    DictionaryTestSuiteBuilder::new(|| async {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("dict.redb");
        let dict: Arc<dyn LocalObjectIdDictionary> = Arc::new(
            RedbObjectIdDictionaryBuilder::new_on_disk(path)
                .with_cache_size(Some(1_000_000))
                .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
                .finish()?,
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
async fn dictionary_testsuite_redb_on_disk_no_cache() -> Result<()> {
    DictionaryTestSuiteBuilder::new(|| async {
        let temp_dir = tempfile::tempdir()?;
        let path = temp_dir.path().join("dict.redb");
        let dict: Arc<dyn LocalObjectIdDictionary> = Arc::new(
            RedbObjectIdDictionaryBuilder::new_on_disk(path)
                .with_claimer(Some(Arc::new(StaticObjectIdClaimer)))
                .finish()?,
        );
        Ok(dict)
    })
    .build()
    .run()
    .await
    .assert_success();

    Ok(())
}
