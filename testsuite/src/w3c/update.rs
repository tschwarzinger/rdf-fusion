use crate::test::{Test, TestOutcome};
use crate::w3c::files::read_file_to_string;
use crate::w3c::report::dataset_diff;
use crate::w3c::utils::load_to_store;
use anyhow::{Context, ensure};
use futures::StreamExt;
use rdf_fusion::execution::sparql::RdfFusionUpdate;
use rdf_fusion::model::dataset::CanonicalizationAlgorithm;
use rdf_fusion::model::{Dataset, GraphName};
use rdf_fusion::store::Store;

pub struct W3CSparqlUpdateEvaluationTest {
    pub id: String,
    pub name: Option<String>,
    pub test_data: crate::w3c::manifest::Test,
    pub optimize_after_load: bool,
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
        let store = Store::default();
        if let Some(data) = &self.test_data.data {
            load_to_store(data, &store, GraphName::DefaultGraph).await?;
        }
        for (name, value) in &self.test_data.graph_data {
            load_to_store(value, &store, name.clone()).await?;
        }

        let result_store = Store::default();
        if let Some(data) = &self.test_data.result {
            load_to_store(data, &result_store, GraphName::DefaultGraph).await?;
        }
        for (name, value) in &self.test_data.result_graph_data {
            load_to_store(value, &result_store, name.clone()).await?;
        }

        if self.optimize_after_load {
            store.optimize().await?;
            result_store.optimize().await?;
        }

        let update_file = self
            .test_data
            .update
            .as_deref()
            .context("No action found")?;
        let update =
            RdfFusionUpdate::parse(&read_file_to_string(update_file)?, Some(update_file))
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
            RdfFusionUpdate::parse(&read_file_to_string(update_file)?, Some(update_file))
                .unwrap(),
        );
        Ok(())
    }
}
