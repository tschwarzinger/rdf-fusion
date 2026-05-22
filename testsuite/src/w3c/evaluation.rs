use crate::test::{Test, TestOutcome};
use crate::w3c::files::{W3CTestRuntime, guess_rdf_format};
use crate::w3c::utils::{W3CTestUtils, are_query_results_isomorphic, results_diff};
use crate::w3c::{StoreConfig, StoreFactory};
use anyhow::{Context, bail, ensure};
use datafusion::physical_plan::displayable;
use futures::StreamExt;
use rdf_fusion::common::{GraphName, NamedOrBlankNode};
use rdf_fusion::execution::sparql::{QueryOptions, RdfFusionQuery};
use rdf_fusion::storage::rdf_files::RdfFileSourceConfig;
use std::collections::HashSet;
use std::sync::Arc;

pub struct W3CSparqlEvaluationTest {
    pub id: String,
    pub name: Option<String>,
    pub test_data: crate::w3c::manifest::Test,
    pub optimize_after_load: bool,
    pub store_factory: StoreFactory,
    pub runtime: W3CTestRuntime,
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
        let query_file = self.test_data.query.as_deref().context("No action found")?;
        let options = QueryOptions::default();
        let query = RdfFusionQuery::parse(
            &self.runtime.read_file_to_string(query_file).await?,
            Some(query_file),
        )
        .context("Failure to parse query")?;

        let mut default_graphs = Vec::new();
        let mut seen_graphs = HashSet::new();
        if let Some(data) = &self.test_data.data {
            default_graphs.push((
                GraphName::DefaultGraph,
                RdfFileSourceConfig {
                    url: url::Url::parse(data)?,
                    format: guess_rdf_format(data)?,
                },
            ));
            seen_graphs.insert((GraphName::DefaultGraph, data.clone()));
        }

        let mut named_graphs = Vec::new();
        let mut seen_named_graphs = HashSet::new();
        for (name, value) in &self.test_data.graph_data {
            named_graphs.push((
                name.clone(),
                RdfFileSourceConfig {
                    url: url::Url::parse(value)?,
                    format: guess_rdf_format(value)?,
                },
            ));
            seen_named_graphs.insert((name.clone(), value.clone()));
        }

        // FROM and FROM NAMED support.
        if !query.dataset().is_default_dataset() {
            for graph_name in query.dataset().default_graph_graphs().unwrap_or(&[]) {
                let GraphName::NamedNode(graph_node) = graph_name else {
                    bail!("Invalid FROM in query {query}");
                };
                let url = graph_node.as_str().to_string();
                if !seen_graphs.contains(&(graph_name.clone(), url.clone())) {
                    default_graphs.push((
                        graph_name.clone(),
                        RdfFileSourceConfig {
                            url: url::Url::parse(&url)?,
                            format: guess_rdf_format(&url)?,
                        },
                    ));
                    seen_graphs.insert((graph_name.clone(), url));
                }
            }
            for graph_name in query.dataset().available_named_graphs().unwrap_or(&[]) {
                let NamedOrBlankNode::NamedNode(graph_node) = graph_name else {
                    bail!("Invalid FROM NAMED in query {query}");
                };
                let url = graph_node.as_str().to_string();
                if !seen_named_graphs.contains(&(graph_node.clone(), url.clone())) {
                    named_graphs.push((
                        graph_node.clone(),
                        RdfFileSourceConfig {
                            url: url::Url::parse(&url)?,
                            format: guess_rdf_format(&url)?,
                        },
                    ));
                    seen_named_graphs.insert((graph_node.clone(), url));
                }
            }
        }

        let store = (self.store_factory)(StoreConfig {
            runtime_env: self.runtime.fresh_env(),
            default_graphs,
            named_graphs,
        })
        .await?;

        if self.optimize_after_load {
            store.optimize().await?;
        }
        store.validate().await?;

        let utils = W3CTestUtils::new(W3CTestRuntime::new(Arc::clone(&self.runtime.env)));
        let expected_results = utils
            .load_sparql_query_result(self.test_data.result.as_ref().unwrap())
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
            RdfFusionQuery::parse(
                &self.runtime.read_file_to_string(query_file).await?,
                Some(query_file)
            )?,
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
