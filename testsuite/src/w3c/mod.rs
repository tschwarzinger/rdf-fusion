mod evaluation;
pub mod files;
pub mod manifest;
pub mod report;
mod syntax;
mod update;
mod utils;

use crate::test::UnsupportedTest;
use crate::testsuite::{TestSuite, TestSuiteBuilder};
use crate::w3c::evaluation::W3CSparqlEvaluationTest;
use crate::w3c::files::{TEST_RUNTIME_ENV, W3CTestRuntime};
use crate::w3c::syntax::W3CSparqlSyntaxTest;
use crate::w3c::update::W3CSparqlUpdateEvaluationTest;
use anyhow::{Context, Result, bail};
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::future::BoxFuture;
use manifest::TestManifests;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::storage::delta::DeltaQuadStorageBuilder;
use rdf_fusion::store::Store;
use std::sync::Arc;

pub type StoreFactory =
    Arc<dyn Fn(Arc<RuntimeEnv>) -> BoxFuture<'static, Store> + Send + Sync>;

pub struct W3CSparqlTestSuiteBuilder {
    builder: TestSuiteBuilder,
    manifest_tests: Vec<manifest::Test>,
    optimize_after_load: bool,
    store_factory: Option<StoreFactory>,
}

impl W3CSparqlTestSuiteBuilder {
    pub async fn load_manifest(manifest_url: impl Into<String>) -> Result<Self> {
        let manifest_url = manifest_url.into();
        let mut manifest_tests = Vec::new();
        let mut manifest = TestManifests::new([&manifest_url]);
        let runtime = W3CTestRuntime::new(TEST_RUNTIME_ENV.clone());
        while let Some(test) = manifest.next(&runtime).await {
            manifest_tests.push(test?);
        }
        let mut builder = TestSuiteBuilder::new();
        builder.with_name(format!("W3C SPARQL Test Suite - {manifest_url}"));
        Ok(Self {
            builder,
            manifest_tests,
            optimize_after_load: false,
            store_factory: None,
        })
    }

    pub fn ignore_test(mut self, id: impl Into<String>) -> Self {
        self.builder.ignore_test(id);
        self
    }

    pub fn ignore_tests<I, S>(mut self, ids: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.builder.ignore_tests(ids);
        self
    }

    pub fn only_test(mut self, id: impl Into<String>) -> Self {
        self.builder.only_test(id);
        self
    }

    pub fn with_optimize_after_load(mut self, optimize: bool) -> Self {
        self.optimize_after_load = optimize;
        self
    }

    pub fn with_store_factory(mut self, factory: StoreFactory) -> Self {
        self.store_factory = Some(factory);
        self
    }

    pub async fn build(mut self) -> Result<TestSuite> {
        let test_ids: std::collections::HashSet<_> = self
            .manifest_tests
            .iter()
            .map(|t| t.id.as_str().to_string())
            .collect();

        for id in &self.builder.ignored_tests {
            if !test_ids.contains(id) {
                bail!("Ignored test {id} not found in manifest");
            }
        }

        if let Some(id) = &self.builder.only_test {
            if !test_ids.contains(id) {
                bail!("Only test {id} not found in manifest");
            }
        }

        let store_factory = self.store_factory.unwrap_or_else(|| {
            Arc::new(|runtime_env| {
                Box::pin(async {
                    let delta_storage = DeltaQuadStorageBuilder::new()
                        .with_encoding(QuadStorageEncodingName::ObjectId)
                        .build()
                        .await
                        .unwrap();

                    let context = RdfFusionContextBuilder::new(Arc::new(delta_storage))
                        .with_runtime_env(Some(runtime_env))
                        .with_single_partition_session_config()
                        .build()
                        .unwrap();
                    Store::new(context)
                })
            })
        });

        for test in self.manifest_tests {
            let kind = test.kind.as_str();

            match kind {
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest"
                | "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveSyntaxTest11"
                | "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest"
                | "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeSyntaxTest11"
                | "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#PositiveUpdateSyntaxTest11"
                | "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#NegativeUpdateSyntaxTest11" =>
                {
                    let is_positive = kind.contains("Positive");
                    let is_update = kind.contains("Update");
                    let action_file = test
                        .action
                        .as_deref()
                        .context("No action found for syntax test")?
                        .to_string();

                    let w3c_test = W3CSparqlSyntaxTest {
                        id: test.id.clone().into_string(),
                        name: test.name.clone(),
                        action_file,
                        is_positive,
                        is_update,
                        runtime: W3CTestRuntime::new(TEST_RUNTIME_ENV.clone()),
                    };
                    self.builder.add_test(Box::new(w3c_test));
                }
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#QueryEvaluationTest" =>
                {
                    let w3c_test = W3CSparqlEvaluationTest {
                        id: test.id.clone().into_string(),
                        name: test.name.clone(),
                        test_data: test,
                        optimize_after_load: self.optimize_after_load,
                        store_factory: Arc::clone(&store_factory),
                        runtime: W3CTestRuntime::new(TEST_RUNTIME_ENV.clone()),
                    };
                    self.builder.add_test(Box::new(w3c_test));
                }
                "http://www.w3.org/2001/sw/DataAccess/tests/test-manifest#UpdateEvaluationTest" =>
                {
                    let w3c_test = W3CSparqlUpdateEvaluationTest {
                        id: test.id.clone().into_string(),
                        name: test.name.clone(),
                        test_data: test,
                        optimize_after_load: self.optimize_after_load,
                        store_factory: Arc::clone(&store_factory),
                        runtime: W3CTestRuntime::new(TEST_RUNTIME_ENV.clone()),
                    };
                    self.builder.add_test(Box::new(w3c_test));
                }
                _ => {
                    self.builder.add_test(Box::new(UnsupportedTest {
                        id: test.id.into_string(),
                        name: test.name,
                        kind: kind.to_string(),
                    }));
                }
            }
        }

        Ok(self.builder.build())
    }
}
