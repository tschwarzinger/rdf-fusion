use crate::runs::BenchmarkRun;
use futures::StreamExt;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::execution::sparql::{QueryExplanation, QueryOptions, RdfFusionQuery};
use rdf_fusion::store::Store;

#[derive(Clone)]
pub enum SparqlRawOperation<QueryName> {
    Query(QueryName, String),
}

impl<QueryName: Clone> SparqlRawOperation<QueryName> {
    pub fn query_name(&self) -> QueryName {
        match self {
            SparqlRawOperation::Query(name, _) => name.clone(),
        }
    }

    pub fn text(&self) -> &str {
        match self {
            SparqlRawOperation::Query(_, text) => text.as_ref(),
        }
    }

    pub fn parse(&self) -> anyhow::Result<SparqlOperation<QueryName>> {
        match self {
            SparqlRawOperation::Query(query_name, query) => {
                let query = query.parse()?;
                Ok(SparqlOperation::Query(query_name.clone(), query))
            }
        }
    }
}

#[derive(Clone)]
pub enum SparqlOperation<QueryName> {
    Query(QueryName, RdfFusionQuery),
}

impl<QueryName> SparqlOperation<QueryName> {
    pub fn query(&self) -> &RdfFusionQuery {
        match self {
            SparqlOperation::Query(_, query) => query,
        }
    }

    pub async fn run(
        &self,
        store: &Store,
    ) -> anyhow::Result<(BenchmarkRun, QueryExplanation, usize)> {
        let start = datafusion::common::instant::Instant::now();

        let mut num_results = 0;
        let options = QueryOptions::default();
        let explanation = match &self {
            SparqlOperation::Query(_, q) => {
                let (result, explanation) =
                    store.explain_query_opt(q.clone(), options.clone()).await?;
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
                explanation
            }
        };

        let duration = start.elapsed();
        Ok((BenchmarkRun { duration }, explanation, num_results))
    }
}

impl<QueryName: Clone> SparqlOperation<QueryName> {
    pub fn query_name(&self) -> QueryName {
        match self {
            SparqlOperation::Query(name, _) => name.clone(),
        }
    }
}
