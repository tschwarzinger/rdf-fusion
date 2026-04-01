use crate::test::{Test, TestOutcome};
use crate::w3c::files::read_file_to_string;
use crate::w3c::utils::{
    are_query_results_isomorphic, load_sparql_query_result, load_to_store, results_diff,
};
use anyhow::{Context, bail, ensure};
use datafusion::physical_plan::displayable;
use futures::StreamExt;
use rdf_fusion::execution::sparql::{Query, QueryOptions};
use rdf_fusion::model::{GraphName, NamedOrBlankNode};
use rdf_fusion::store::Store;

pub struct W3CSparqlEvaluationTest {
    pub id: String,
    pub name: Option<String>,
    pub test_data: crate::w3c::manifest::Test,
    pub optimize_after_load: bool,
}

#[async_trait::async_trait]
impl Test for W3CSparqlEvaluationTest {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    async fn run(&self) -> anyhow::Result<TestOutcome> {
        let result = self.execute().await;
        Ok(match result {
            Ok(_) => TestOutcome::Success,
            Err(e) => TestOutcome::Failed(e),
        })
    }
}

impl W3CSparqlEvaluationTest {
    async fn execute(&self) -> anyhow::Result<()> {
        let store = Store::default();
        if let Some(data) = &self.test_data.data {
            load_to_store(data, &store, GraphName::DefaultGraph).await?;
        }
        for (name, value) in &self.test_data.graph_data {
            load_to_store(value, &store, name.clone()).await?;
        }

        if self.optimize_after_load {
            store.optimize().await?;
        }
        store.validate().await?;

        let query_file = self.test_data.query.as_deref().context("No action found")?;
        let options = QueryOptions::default();
        let query = Query::parse(&read_file_to_string(query_file)?, Some(query_file))
            .context("Failure to parse query")?;

        // We check parsing roundtrip
        Query::parse(&query.to_string(), None)
            .with_context(|| format!("Failure to deserialize \"{query}\""))?;

        // FROM and FROM NAMED support. We make sure the data is in the store
        if !query.dataset().is_default_dataset() {
            for graph_name in query.dataset().default_graph_graphs().unwrap_or(&[]) {
                let GraphName::NamedNode(graph_name) = graph_name else {
                    bail!("Invalid FROM in query {query}");
                };
                load_to_store(graph_name.as_str(), &store, graph_name.as_ref()).await?;
            }
            for graph_name in query.dataset().available_named_graphs().unwrap_or(&[]) {
                let NamedOrBlankNode::NamedNode(graph_name) = graph_name else {
                    bail!("Invalid FROM NAMED in query {query}");
                };
                load_to_store(graph_name.as_str(), &store, graph_name.as_ref()).await?;
            }
        }

        let expected_results =
            load_sparql_query_result(self.test_data.result.as_ref().unwrap())
                .await
                .context("Error constructing expected graph")?;

        let (actual_results, explanation) = store
            .explain_query_opt(query.clone(), options)
            .await
            .context("Failure to execute query")?;

        ensure!(
            are_query_results_isomorphic(&expected_results, actual_results).await,
            "Not isomorphic results.\n{}\nParsed query:\n{}\nData:\n{:?}\n\nExecution Plan:\n{}\n",
            results_diff(expected_results, store.query(query.clone()).await?).await,
            Query::parse(&read_file_to_string(query_file)?, Some(query_file))?,
            {
                let mut data = Vec::new();
                let mut stream = store.stream().await?;
                while let Some(q) = stream.next().await {
                    data.push(q?);
                }
                data
            },
            displayable(explanation.execution_plan.as_ref()).indent(false),
        );
        Ok(())
    }
}
