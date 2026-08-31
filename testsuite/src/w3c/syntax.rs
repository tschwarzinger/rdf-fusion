use crate::test::{Test, TestOutcome};
use crate::w3c::files::W3CTestRuntime;
use anyhow::{Context, ensure};
use rdf_fusion::common::sparql::SparqlParser;

pub struct W3CSparqlSyntaxTest {
    pub id: String,
    pub name: Option<String>,
    pub action_file: String,
    pub is_positive: bool,
    pub is_update: bool,
    pub runtime: W3CTestRuntime,
}

#[async_trait::async_trait]
impl Test for W3CSparqlSyntaxTest {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    async fn run(&self) -> anyhow::Result<TestOutcome> {
        let content = self.runtime.read_file_to_string(&self.action_file).await?;

        let parser = SparqlParser::new()
            .with_base_iri(&self.action_file)
            .expect("Invalid base IRI for SPARQL parser.");
        let result = if self.is_positive {
            if self.is_update {
                let update = parser
                    .clone()
                    .parse_update(&content)
                    .context("Not able to parse positive update syntax test")?;
                parser
                    .parse_update(&update.to_string())
                    .map(|_| ())
                    .with_context(|| format!("Failure to deserialize \"{update}\""))
            } else {
                let query = parser
                    .clone()
                    .parse_query(&content)
                    .context("Not able to parse positive syntax test")?;
                parser
                    .parse_query(&query.to_string())
                    .map(|_| ())
                    .with_context(|| format!("Failure to deserialize \"{query}\""))
            }
        } else {
            let res = if self.is_update {
                parser.parse_update(&content).map(|_| ())
            } else {
                parser.parse_query(&content).map(|_| ())
            };
            ensure!(
                res.is_err(),
                "Negative syntax test {} parsed even if it should not.",
                self.id
            );
            Ok(())
        };

        Ok(match result {
            Ok(_) => TestOutcome::Success,
            Err(e) => TestOutcome::Failed(e),
        })
    }
}
