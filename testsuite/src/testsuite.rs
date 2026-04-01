use crate::test::{Test, TestOutcome, TestRun};
use datafusion::common::instant::Instant;
use std::collections::{BTreeMap, HashSet};
use time::OffsetDateTime;

/// Represents a collection of tests to be executed.
pub struct TestSuite {
    /// The name of the test suite.
    pub name: String,
    /// The list of tests to run.
    pub tests: Vec<Box<dyn Test>>,
    /// The set of test identifiers to ignore.
    pub ignored_tests: HashSet<String>,
    /// If set, only the test with this identifier will be run.
    pub only_test: Option<String>,
}

/// The outcome of executing an entire test suite.
pub struct TestSuiteResult {
    /// The name of the test suite that was executed.
    pub suite_name: String,
    /// A map from test identifier to its execution run.
    pub results: BTreeMap<String, TestRun>,
    /// The timestamp when the suite execution started.
    pub start_time: OffsetDateTime,
    /// The timestamp when the suite execution finished.
    pub end_time: OffsetDateTime,
}

impl TestSuiteResult {
    /// Asserts that all tests in the suite were successful.
    ///
    /// Prints a summary of the suite's execution to standard output.
    /// Panics if any test failed.
    pub fn assert_success(&self) {
        let mut passed = 0;
        let mut failed = 0;
        let mut ignored = 0;
        let mut failures = Vec::new();

        for (id, run) in &self.results {
            match &run.outcome {
                TestOutcome::Success => passed += 1,
                TestOutcome::Failed(e) => {
                    failed += 1;
                    failures.push((id, e));
                }
                TestOutcome::Ignored => ignored += 1,
            }
        }

        failures.sort_by_key(|(id, _)| *id);

        let duration = self.end_time - self.start_time;
        println!("\n");
        println!("Test Suite: {}", self.suite_name);
        println!("========================================");
        println!("Duration: {}ms", duration.whole_milliseconds());
        println!("Passed:   {passed}");
        println!("Failed:   {failed}");
        println!("Ignored:  {ignored}");

        if failed > 0 {
            let mut error_msg = format!(
                "{} tests failed in suite \"{}\":\n",
                failed, self.suite_name
            );
            for (id, err) in failures {
                error_msg.push_str(&format!("\n--- FAILURE: {id} ---\n{err:?}\n"));
            }
            panic!("{}", error_msg);
        }
    }
}

impl TestSuite {
    /// Runs all tests in the suite and returns the result.
    pub async fn run(&self) -> TestSuiteResult {
        let start_time = OffsetDateTime::now_utc();
        let mut results = BTreeMap::new();

        for test in &self.tests {
            let id = test.id();

            let is_ignored = if let Some(only_id) = &self.only_test {
                id != only_id
            } else {
                self.ignored_tests.contains(id)
            };

            let start = Instant::now();
            let outcome = if is_ignored {
                TestOutcome::Ignored
            } else {
                test.run().await.unwrap_or_else(TestOutcome::Failed)
            };

            let run = TestRun {
                outcome,
                duration: start.elapsed(),
            };

            results.insert(id.to_string(), run);
        }

        let end_time = OffsetDateTime::now_utc();
        TestSuiteResult {
            suite_name: self.name.clone(),
            results,
            start_time,
            end_time,
        }
    }
}

/// A builder for constructing a [`TestSuite`].
#[derive(Default)]
pub struct TestSuiteBuilder {
    pub name: String,
    pub tests: Vec<Box<dyn Test>>,
    pub ignored_tests: HashSet<String>,
    pub only_test: Option<String>,
}

impl TestSuiteBuilder {
    /// Creates a new builder with an empty name.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the name of the test suite.
    pub fn with_name(&mut self, name: impl Into<String>) -> &mut Self {
        self.name = name.into();
        self
    }

    /// Adds a single test to the suite.
    pub fn add_test(&mut self, test: Box<dyn Test>) -> &mut Self {
        self.tests.push(test);
        self
    }

    /// Adds multiple tests to the suite.
    pub fn add_tests<I>(&mut self, tests: I) -> &mut Self
    where
        I: IntoIterator<Item = Box<dyn Test>>,
    {
        self.tests.extend(tests);
        self
    }

    /// Adds a test identifier to the ignore list.
    pub fn ignore_test(&mut self, id: impl Into<String>) -> &mut Self {
        self.ignored_tests.insert(id.into());
        self
    }

    /// Adds multiple test identifiers to the ignore list.
    pub fn ignore_tests<I, S>(&mut self, ids: I) -> &mut Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.ignored_tests.extend(ids.into_iter().map(Into::into));
        self
    }

    /// Sets the single test to run.
    pub fn only_test(&mut self, id: impl Into<String>) -> &mut Self {
        self.only_test = Some(id.into());
        self
    }

    /// Builds the [`TestSuite`].
    pub fn build(self) -> TestSuite {
        TestSuite {
            name: self.name,
            tests: self.tests,
            ignored_tests: self.ignored_tests,
            only_test: self.only_test,
        }
    }
}
