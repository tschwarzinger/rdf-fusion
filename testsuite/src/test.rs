use anyhow::Result;
use std::time::Duration;

#[async_trait::async_trait]
pub trait Test: Send + Sync {
    /// The unique identifier of the test.
    fn id(&self) -> &str;

    /// The name of the test.
    fn name(&self) -> Option<&str>;

    /// Run the test.
    async fn run(&self) -> Result<TestOutcome>;
}

pub enum TestOutcome {
    Success,
    Failed(anyhow::Error),
    Panicked(String),
    Ignored,
}

pub struct TestRun {
    pub outcome: TestOutcome,
    pub duration: Duration,
}

pub struct UnsupportedTest {
    pub id: String,
    pub name: Option<String>,
    pub kind: String,
}

#[async_trait::async_trait]
impl Test for UnsupportedTest {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    async fn run(&self) -> Result<TestOutcome> {
        Ok(TestOutcome::Failed(anyhow::anyhow!(
            "Unsupported test kind: {}",
            self.kind
        )))
    }
}
