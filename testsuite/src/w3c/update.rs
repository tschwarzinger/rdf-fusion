use crate::test::{Test, TestOutcome};
use crate::w3c::files::{W3CTestRuntime, guess_rdf_format};
use crate::w3c::report::dataset_diff;
use crate::w3c::{StoreConfig, StoreFactory};
use anyhow::{Context, ensure};
use futures::StreamExt;
use rdf_fusion::common::dataset::CanonicalizationAlgorithm;
use rdf_fusion::common::{Dataset, GraphName};
use rdf_fusion::execution::sparql::RdfFusionUpdate;
use rdf_fusion::storage::rdf_files::RdfFileSourceConfig;

pub struct W3CSparqlUpdateEvaluationTest {
    pub id: String,
    pub name: Option<String>,
    pub test_data: crate::w3c::manifest::Test,
    pub optimize_after_load: bool,
    pub store_factory: StoreFactory,
    pub runtime: W3CTestRuntime,
}

#[async_trait::async_trait]
impl Test for W3CSparqlUpdateEvaluationTest {
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

impl W3CSparqlUpdateEvaluationTest {
    async fn execute(&self) -> anyhow::Result<()> {
        let mut default_graphs = Vec::new();
        if let Some(data) = &self.test_data.data {
            default_graphs.push((
                GraphName::DefaultGraph,
                RdfFileSourceConfig {
                    url: data.clone(),
                    format: guess_rdf_format(data)?,
                },
            ));
        }
        let mut named_graphs = Vec::new();
        for (name, value) in &self.test_data.graph_data {
            named_graphs.push((
                name.clone(),
                RdfFileSourceConfig {
                    url: value.clone(),
                    format: guess_rdf_format(value)?,
                },
            ));
        }

        let store = (self.store_factory)(StoreConfig {
            runtime_env: self.runtime.fresh_env(),
            default_graphs,
            named_graphs,
        })
        .await?;

        let mut result_default_graphs = Vec::new();
        if let Some(data) = &self.test_data.result {
            result_default_graphs.push((
                GraphName::DefaultGraph,
                RdfFileSourceConfig {
                    url: data.clone(),
                    format: guess_rdf_format(data)?,
                },
            ));
        }
        let mut result_named_graphs = Vec::new();
        for (name, value) in &self.test_data.result_graph_data {
            result_named_graphs.push((
                name.clone(),
                RdfFileSourceConfig {
                    url: value.clone(),
                    format: guess_rdf_format(value)?,
                },
            ));
        }

        let result_store = (self.store_factory)(StoreConfig {
            runtime_env: self.runtime.fresh_env(),
            default_graphs: result_default_graphs,
            named_graphs: result_named_graphs,
        })
        .await?;

        if self.optimize_after_load {
            store.optimize().await?;
            result_store.optimize().await?;
        }

        let update_file = self
            .test_data
            .update
            .as_deref()
            .context("No action found")?;
        let update = RdfFusionUpdate::parse(
            &self.runtime.read_file_to_string(update_file).await?,
            Some(update_file),
        )
        .context("Failure to parse update")?;

        // We check parsing roundtrip
        RdfFusionUpdate::parse(&update.to_string(), None)
            .with_context(|| format!("Failure to deserialize \"{update}\""))?;

        store
            .update(update)
            .await
            .context("Failure to execute update")?;
        let mut store_dataset = Dataset::new();
        let mut stream = store.stream().await?;
        while let Some(q) = stream.next().await {
            store_dataset.insert(&q?);
        }
        store_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);

        let mut result_store_dataset = Dataset::new();
        let mut stream = result_store.stream().await?;
        while let Some(q) = stream.next().await {
            result_store_dataset.insert(&q?);
        }
        result_store_dataset.canonicalize(CanonicalizationAlgorithm::Unstable);

        ensure!(
            store_dataset == result_store_dataset,
            "Not isomorphic result dataset.\nDiff:\n{}\nParsed update:\n{}\n",
            dataset_diff(&result_store_dataset, &store_dataset),
            RdfFusionUpdate::parse(
                &self.runtime.read_file_to_string(update_file).await?,
                Some(update_file)
            )
            .unwrap(),
        );
        Ok(())
    }
}
