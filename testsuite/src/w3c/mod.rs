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
use crate::w3c::syntax::W3CSparqlSyntaxTest;
use crate::w3c::update::W3CSparqlUpdateEvaluationTest;
use anyhow::{Context, Result, bail};
use manifest::TestManifest;

pub struct W3CSparqlTestSuiteBuilder {
    builder: TestSuiteBuilder,
    manifest_tests: Vec<manifest::Test>,
    optimize_after_load: bool,
}

impl W3CSparqlTestSuiteBuilder {
    pub fn load_manifest(manifest_url: impl Into<String>) -> Result<Self> {
        let manifest_url = manifest_url.into();
        let manifest_tests = TestManifest::new([&manifest_url])
            .collect::<Result<Vec<_>>>()
            .with_context(|| format!("Failed to load manifest at {manifest_url}"))?;
        let mut builder = TestSuiteBuilder::new();
        builder.with_name(format!("W3C SPARQL Test Suite - {manifest_url}"));
        Ok(Self {
            builder,
            manifest_tests,
            optimize_after_load: false,
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
