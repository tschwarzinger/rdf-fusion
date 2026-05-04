use crate::test::{Test, TestOutcome};
use crate::testsuite::{TestSuite, TestSuiteBuilder};
use anyhow::Result;
use async_trait::async_trait;
use rdf_fusion::api::storage::QuadStorage;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

mod tests;
use tests::*;

/// A builder for constructing a test suite for testing compliance with the [`QuadStorage`] trait.
pub struct StorageTestSuiteBuilder {
    builder: TestSuiteBuilder,
    factory: StorageFactory,
}

impl StorageTestSuiteBuilder {
    pub fn new<F, Fut>(factory: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Arc<dyn QuadStorage>>> + Send + 'static,
    {
        let mut builder = TestSuiteBuilder::new();
        builder.with_name("Storage Compliance Test Suite");
        let mut suite_builder = Self {
            builder,
            factory: Arc::new(move || Box::pin(factory())),
        };

        suite_builder.add_test("insert_quad", insert_quad);
        suite_builder.add_test(
            "insert_quad_and_query_within_transaction",
            insert_quad_and_query_within_transaction,
        );
        suite_builder.add_test(
            "insert_and_remove_quad_and_query_within_transaction",
            insert_and_remove_quad_and_query_within_transaction,
        );
        suite_builder.add_test(
            "insert_duplicate_quads_no_effect",
            insert_duplicate_quads_no_effect,
        );
        suite_builder.add_test(
            "insert_duplicate_quads_in_same_operation",
            insert_duplicate_quads_in_same_operation,
        );
        suite_builder.add_test(
            "named_graph_insertion_and_query",
            named_graph_insertion_and_query,
        );
        suite_builder.add_test("remove_quad", remove_quad);
        suite_builder.add_test("clear_graph", clear_graph);
        suite_builder.add_test("insert_named_graph", insert_named_graph);
        suite_builder.add_test("remove_named_graph", remove_named_graph);
        suite_builder.add_test("clear_all", clear_all);
        suite_builder.add_test("optimize", optimize);
        suite_builder.add_test("optimize_empty", optimize_empty);
        suite_builder.add_test("empty_insert", empty_insert);
        suite_builder.add_test("empty_remove", empty_remove);

        suite_builder
    }

    pub fn add_test<F, Fut>(&mut self, id: &str, run_fn: F)
    where
        F: Fn(Arc<dyn QuadStorage>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        self.builder.add_test(Box::new(StorageTest {
            id: id.to_string(),
            factory: Arc::clone(&self.factory),
            run_fn: Arc::new(move |s| Box::pin(run_fn(s))),
        }));
    }

    pub fn build(self) -> TestSuite {
        self.builder.build()
    }

    pub fn ignore_test(mut self, id: impl Into<String>) -> Self {
        self.builder.ignore_test(id);
        self
    }

    pub fn only_test(mut self, id: impl Into<String>) -> Self {
        self.builder.only_test(id);
        self
    }
}

/// Creates a new [`QuadStorage`] for each test.
type StorageFactory = Arc<
    dyn Fn() -> Pin<Box<dyn Future<Output = Result<Arc<dyn QuadStorage>>> + Send>>
        + Send
        + Sync,
>;

/// Asserts some behavior on a [`QuadStorage`] for each test.
type StorageTestFn = Arc<
    dyn Fn(Arc<dyn QuadStorage>) -> Pin<Box<dyn Future<Output = Result<()>> + Send>>
        + Send
        + Sync,
>;

struct StorageTest {
    id: String,
    factory: StorageFactory,
    run_fn: StorageTestFn,
}

#[async_trait]
impl Test for StorageTest {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> Option<&str> {
        Some(&self.id)
    }

    async fn run(&self) -> Result<TestOutcome> {
        let storage = (self.factory)().await?;
        let result = (self.run_fn)(Arc::clone(&storage)).await;
        Ok(match result {
            Ok(_) => TestOutcome::Success,
            Err(e) => TestOutcome::Failed(e),
        })
    }
}
