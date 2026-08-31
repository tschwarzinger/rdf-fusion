use crate::runs::BenchmarkRun;
use futures::StreamExt;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::execution::sparql::{QueryExplanation, QueryOptions};
use rdf_fusion::store::Store;

/// A SPARQL operation (a named query) that gets executed during a benchmark run.
#[derive(Clone)]
pub struct SparqlOperation {
    name: String,
    text: String,
}

impl SparqlOperation {
    /// Creates a new [`SparqlOperation`] from a query name and its SPARQL text.
    pub fn new(name: impl Into<String>, text: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            text: text.into(),
        }
    }

    /// Returns the name of the operation.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the SPARQL query text.
    pub fn text(&self) -> &str {
        &self.text
    }

    /// Executes the operation against the given store and returns the measured run along with the
    /// query explanation and the number of results.
    pub async fn run(
        &self,
        store: &Store,
    ) -> anyhow::Result<(BenchmarkRun, QueryExplanation, usize)> {
        let start = datafusion::common::instant::Instant::now();

        let mut num_results = 0;
        let options = QueryOptions::default();
        let (result, explanation) = store.explain_query_opt(self.text(), options).await?;
        match result {
            QueryResults::Boolean(_) => (),
            QueryResults::Solutions(s) => {
                let mut stream = s.into_record_batch_stream()?;
                while let Some(s) = stream.next().await {
                    num_results += s?.num_rows();
                }
            }
            QueryResults::Graph(mut g) => {
                while let Some(t) = g.next().await {
                    t?;
                    num_results += 1;
                }
            }
        }

        let duration = start.elapsed();
        Ok((BenchmarkRun { duration }, explanation, num_results))
    }
}
