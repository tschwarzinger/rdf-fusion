use crate::test::{Test, TestOutcome};
use crate::testsuite::{TestSuite, TestSuiteBuilder};
use anyhow::Result;
use async_trait::async_trait;
use rdf_fusion::storage::local_object_ids::LocalObjectIdDictionary;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

mod tests;
use tests::*;

pub struct DictionaryTestSuiteBuilder {
    builder: TestSuiteBuilder,
    factory: DictionaryFactory,
}

impl DictionaryTestSuiteBuilder {
    pub fn new<F, Fut>(factory: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn LocalObjectIdDictionary>>> + Send + 'static,
    {
        let mut builder = TestSuiteBuilder::new();
        builder.with_name("Dictionary Compliance Test Suite");
        let mut suite_builder = Self {
            builder,
            factory: Arc::new(move || Box::pin(factory())),
        };

        suite_builder.add_test("encode_and_resolve_terms", encode_and_resolve_terms);
        suite_builder.add_test(
            "add_global_batch_non_contiguous",
            add_global_batch_non_contiguous,
        );
        suite_builder.add_test("synced_version", synced_version);
        suite_builder.add_test("transaction_commit", transaction_commit);
        suite_builder.add_test("transaction_abort", transaction_abort);

        suite_builder
    }

    pub fn add_test<F, Fut>(&mut self, id: &str, run_fn: F)
    where
        F: Fn(Arc<dyn LocalObjectIdDictionary>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.builder.add_test(Box::new(DictionaryTest {
            id: id.to_string(),
            factory: Arc::clone(&self.factory),
            run_fn: Arc::new(move |s| Box::pin(run_fn(s))),
        }));
    }

    pub fn build(self) -> TestSuite {
        self.builder.build()
    }
}

type DictionaryFactory = Arc<
    dyn Fn() -> Pin<
            Box<dyn Future<Output = Result<Arc<dyn LocalObjectIdDictionary>>> + Send>,
        > + Send
        + Sync,
>;

type DictionaryTestFn = Arc<
    dyn Fn(
            Arc<dyn LocalObjectIdDictionary>,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

struct DictionaryTest {
    id: String,
    factory: DictionaryFactory,
    run_fn: DictionaryTestFn,
}

#[async_trait]
impl Test for DictionaryTest {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> Option<&str> {
        Some(&self.id)
    }

    async fn run(&self) -> Result<TestOutcome> {
        let dict = (self.factory)().await?;
        let result = (self.run_fn)(dict).await;
        Ok(match result {
            Ok(_) => TestOutcome::Success,
            Err(e) => TestOutcome::Failed(e),
        })
    }
}
