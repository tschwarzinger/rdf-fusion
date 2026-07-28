use rdf_fusion::common::{GraphNameRef, NamedNode, Quad, RdfFormat};
use rdf_fusion::execution::sparql::{QueryExplanation, QueryOptions};
use rdf_fusion::store::Store;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_storage::delta::DeltaQuadsStorage;
use rdf_fusion_storage::quad_tables::QuadTableName;
use rdf_fusion_storage::rdf_files::RdfFileScanOptions;
use std::sync::Arc;
use tokio::fs::File;

mod bgp;
mod exists;

/// Encapsulates all setup and data manipulation via the public `Store` API.
pub struct StoreTestContext {
    pub store: Store,
}

impl StoreTestContext {
    /// Bootstraps a fresh, empty in-memory Store.
    pub async fn new() -> Self {
        let storage = DeltaQuadsStorage::new_in_memory(
            QuadStorageEncodingName::ObjectId,
            vec![QuadTableName::GPOS],
        )
        .await;

        let ctx = RdfFusionContextBuilder::new(Arc::new(storage))
            .with_register_in_memory_store(true)
            .with_single_partition_session_config()
            .build()
            .unwrap();

        Self {
            store: Store::new(ctx),
        }
    }

    /// Inserts a slice of quads into the store.
    pub async fn insert(&self, quads: &[Quad]) -> &Self {
        for quad in quads {
            self.store.insert(quad.as_ref()).await.unwrap();
        }
        self
    }

    /// Loads data from a Turtle file.
    pub async fn load_ttl(&self, path: &str) -> &Self {
        self.store
            .load_from_reader(
                File::open(path).await.unwrap(),
                RdfFileScanOptions::with_format(RdfFormat::Turtle),
            )
            .await
            .unwrap();
        self
    }

    /// Triggers store optimization (e.g., applying updates/deltas to quad tables).
    pub async fn optimize(&self) -> &Self {
        self.store.optimize().await.unwrap();
        self
    }

    /// Retrieves the logical and physical query plans as trimmed strings.
    pub async fn get_query_plans(&self, query: &str) -> (String, String) {
        let explanation = self.explain(query).await;

        let logical_str = format!("{}", explanation.optimized_logical_plan)
            .lines()
            .map(str::trim_end)
            .collect::<Vec<_>>()
            .join("\n");

        let physical_str =
            datafusion::physical_plan::displayable(explanation.execution_plan.as_ref())
                .indent(true)
                .to_string()
                .lines()
                .map(str::trim_end)
                .collect::<Vec<_>>()
                .join("\n");

        (logical_str, physical_str)
    }

    /// Helper: Generates the plan explanation for a given SPARQL query.
    pub async fn explain(&self, query: &str) -> QueryExplanation {
        let (_, explanation) = self
            .store
            .explain_query_opt(query, QueryOptions::default())
            .await
            .unwrap();
        explanation
    }

    // ------------------------------------------------------------------------
    // Pre-configured Scenarios
    // ------------------------------------------------------------------------

    /// Creates a store, inserts a standard single quad, and optimizes it.
    pub async fn setup_basic() -> Self {
        let ctx = Self::new().await;
        ctx.insert(&[
            Quad::new(
                NamedNode::new_unchecked("http://example.org/s1"),
                NamedNode::new_unchecked("http://example.org/p1"),
                NamedNode::new_unchecked("http://example.org/o1"),
                GraphNameRef::DefaultGraph,
            ),
            Quad::new(
                NamedNode::new_unchecked("http://example.org/o1"),
                NamedNode::new_unchecked("http://example.org/p2"),
                NamedNode::new_unchecked("http://example.org/o2"),
                GraphNameRef::DefaultGraph,
            ),
        ])
        .await;
        ctx.optimize().await;
        ctx
    }

    /// Creates a store, loads a Turtle file, and optimizes it.
    pub async fn setup_with_ttl(path: &str) -> Self {
        let ctx = Self::new().await;
        ctx.load_ttl(path).await;
        ctx.optimize().await;
        ctx
    }
}

#[macro_export]
macro_rules! assert_plan_snapshot {
    ($plan:expr, @$snapshot:literal) => {
        insta::with_settings!({filters => vec![
            (r"part-[0-9a-f-]+\.snappy\.parquet", "<file>"),
            (r"part-[0-9a-f-]+\.parquet", "<file>.parquet"),
        ]}, {
            insta::assert_snapshot!($plan, @$snapshot);
        });
    };
}
