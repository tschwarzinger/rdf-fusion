use super::*;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::plan_err;
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::{Extension, LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::displayable;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::{SessionConfig, SessionContext};
use rdf_fusion_common::{NamedNode, Quad, StorageError, TermPattern, TriplePattern};
use rdf_fusion_encoding::{QuadStorageEncodingName, quads_to_plain_term_dataframe};
use rdf_fusion_execution::RdfFusionPlanner;
use rdf_fusion_extensions::storage::{QuadStorage, QuadStorageSnapshot};
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use rdf_fusion_storage::quad_tables::QuadTableName;
use std::sync::Arc;

/// Automatically applies standard filters for Parquet file names before snapshotting.
macro_rules! assert_plan_snapshot {
        ($plan_str:expr, @$snapshot:literal) => {
            let plan_str = $plan_str;

            insta::with_settings!({filters => vec![
                (r"part-.*\.parquet", "part-<file>.parquet"),
            ]}, {
                insta::assert_snapshot!(plan_str, @$snapshot);
            });
        };
    }

#[tokio::test]
async fn test_planner_skips_apply_changeset_when_versions_match() {
    let ctx = PlannerTestContext::new(
        QuadStorageEncodingName::ObjectId,
        vec![QuadTableName::GSPO],
        1,
    )
    .await;
    assert_plan_snapshot!(ctx.get_plan_string().await, @"EmptyExec");
}

#[tokio::test]
async fn test_planner_pushes_down_filter_string_encoding() {
    let ctx = PlannerTestContext::new(
        QuadStorageEncodingName::String,
        vec![QuadTableName::GSPO],
        1,
    )
    .await;
    assert_plan_snapshot!(ctx.get_plan_string().await, @"EmptyExec");
}

#[tokio::test]
async fn test_no_quad_table_no_change() {
    let ctx = PlannerTestContext::new(QuadStorageEncodingName::ObjectId, vec![], 1).await;
    assert_plan_snapshot!(ctx.get_plan_string().await, @"EmptyExec");
}

#[tokio::test]
async fn test_no_quad_table_with_change() {
    let ctx = PlannerTestContext::new(QuadStorageEncodingName::ObjectId, vec![], 1).await;

    ctx.insert(&[test_quad(
        "https://my.com/s",
        "https://my.com/p",
        "https://my.com/o",
        "https://my.com/g",
    )])
    .await;

    assert_plan_snapshot!(ctx.get_plan_string().await, @r"
    ProjectionExec: expr=[predicate@0 as p, object@1 as o]
      FilterExec: graph@0 IS NULL AND subject@1 = 1, projection=[predicate@2, object@3]
        DataSourceExec: partitions=1, partition_sizes=[1]
    ");
}

#[tokio::test]
async fn test_planner_with_additions() {
    let ctx = PlannerTestContext::new(
        QuadStorageEncodingName::ObjectId,
        vec![QuadTableName::GSPO],
        1,
    )
    .await
    .with_existing_quads(&[test_quad(
        "https://my.com/base_s",
        "https://my.com/base_p",
        "https://my.com/base_o",
        "https://my.com/base_g",
    )])
    .await;

    ctx.insert(&[test_quad(
        "https://my.com/s",
        "https://my.com/p",
        "https://my.com/o",
        "https://my.com/g",
    )])
    .await;

    assert_plan_snapshot!(
        ctx.get_plan_string().await,
        @"
    ProjectionExec: expr=[predicate@0 as p, object@1 as o]
      FilterExec: graph@0 IS NULL AND subject@1 = 5, projection=[predicate@2, object@3]
        DataSourceExec: partitions=1, partition_sizes=[1]
    "
    );
}

#[tokio::test]
async fn test_planner_with_deletions_inserts_anti_join() {
    let ctx = PlannerTestContext::new(
        QuadStorageEncodingName::ObjectId,
        vec![QuadTableName::GSPO],
        1,
    )
    .await
    .with_existing_quads(&[test_quad(
        "https://my.com/s",
        "https://my.com/p",
        "https://my.com/o",
        "https://my.com/g",
    )])
    .await;

    ctx.remove(&[test_quad(
        "https://my.com/s",
        "https://my.com/p",
        "https://my.com/o",
        "https://my.com/g",
    )])
    .await;

    assert_plan_snapshot!(ctx.get_plan_string().await, @"EmptyExec");
}

#[tokio::test]
async fn test_planner_with_additions_and_deletions() {
    let ctx = PlannerTestContext::new(
        QuadStorageEncodingName::ObjectId,
        vec![QuadTableName::GSPO],
        1,
    )
    .await
    .with_existing_quads(&[test_quad(
        "https://my.com/s",
        "https://my.com/p",
        "https://my.com/o",
        "https://my.com/g",
    )])
    .await;

    ctx.insert(&[test_quad(
        "https://my.com/s1",
        "https://my.com/p1",
        "https://my.com/o1",
        "https://my.com/g1",
    )])
    .await;
    ctx.remove(&[test_quad(
        "https://my.com/s2",
        "https://my.com/p2",
        "https://my.com/o2",
        "https://my.com/g2",
    )])
    .await;

    assert_plan_snapshot!(ctx.get_plan_string().await, @"
    ProjectionExec: expr=[predicate@0 as p, object@1 as o]
      FilterExec: graph@0 IS NULL AND subject@1 = 1, projection=[predicate@2, object@3]
        DataSourceExec: partitions=1, partition_sizes=[1]
    ");
}

#[tokio::test]
async fn test_planner_with_additions_multiple_partitions() {
    let ctx = PlannerTestContext::new(
        QuadStorageEncodingName::ObjectId,
        vec![QuadTableName::GSPO],
        2,
    )
    .await
    .with_existing_quads(&[test_quad(
        "https://my.com/base_s",
        "https://my.com/base_p",
        "https://my.com/base_o",
        "https://my.com/base_g",
    )])
    .await;

    ctx.insert(&[test_quad(
        "https://my.com/s",
        "https://my.com/p",
        "https://my.com/o",
        "https://my.com/g",
    )])
    .await;

    assert_plan_snapshot!(ctx.get_plan_string().await, @"
    ProjectionExec: expr=[predicate@0 as p, object@1 as o]
      FilterExec: graph@0 IS NULL AND subject@1 = 5, projection=[predicate@2, object@3]
        DataSourceExec: partitions=1, partition_sizes=[1]
    ");
}

#[tokio::test]
async fn test_generic_planner_installed_by_default_and_buffers_small_scans() {
    let ctx = PlannerTestContext::new(
        QuadStorageEncodingName::String,
        vec![QuadTableName::GSPO],
        1,
    )
    .await
    .with_existing_quads(&[test_quad(
        "https://my.com/s",
        "https://my.com/p",
        "https://my.com/o",
        "https://my.com/g",
    )])
    .await;

    let plan = ctx.get_plan_string().await;
    assert_plan_snapshot!(
        plan,
        @"ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.com/s> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[quad-tables/GSPO/part-<file>.parquet]]}, projection=[predicate@2 as p, object@3 as o], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = <https://my.com/s>, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= <https://my.com/s> AND <https://my.com/s> <= subject_max@2, required_guarantees=[subject in (<https://my.com/s>)]"
    );

    // With an eager buffering threshold the small matching scan gets buffered.
    ctx.session
        .state()
        .config_mut()
        .clone()
        .set_str("rdf_fusion.execution.small_scan_buffering_threshold", "10K");
    let plan = ctx.get_plan_string().await;
    assert_plan_snapshot!(
        plan,
        @"ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.com/s> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[quad-tables/GSPO/part-<file>.parquet]]}, projection=[predicate@2 as p, object@3 as o], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = <https://my.com/s>, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= <https://my.com/s> AND <https://my.com/s> <= subject_max@2, required_guarantees=[subject in (<https://my.com/s>)]"
    );
}

/// Ensures that the storage planner takes precedence over the generic planner (which should panic).
#[tokio::test]
async fn test_storage_planner_takes_precedence_over_generic() {
    let ctx = PlannerTestContext::new(QuadStorageEncodingName::String, vec![], 1).await;
    let rdf_ctx =
        RdfFusionContextBuilder::new(Arc::clone(&ctx.storage) as Arc<dyn QuadStorage>)
            .build()
            .unwrap();

    let session_state = SessionStateBuilder::from(rdf_ctx.session_context().state())
        .with_query_planner(Arc::new(RdfFusionPlanner::new_with_snapshot(
            rdf_ctx.create_view(),
            Arc::new(TestSnapshot),
        )))
        .build();

    let logical_plan = LogicalPlan::Extension(Extension {
        node: Arc::new(ctx.node.clone()),
    });
    let plan = session_state
        .create_physical_plan(&logical_plan)
        .await
        .unwrap_err();
    assert_eq!(
        plan.to_string(),
        "Error during planning: TestPlanner was executed!"
    );
}

// ------------------------------------------------------------------------
// Test Context Fixture
// ------------------------------------------------------------------------

/// Encapsulates all setup and data manipulation for testing the planner.
struct PlannerTestContext {
    session: SessionContext,
    storage: Arc<DeltaQuadsStorage>,
    node: QuadPatternNode,
}

impl PlannerTestContext {
    /// Creates a new context with a configurable number of partitions.
    async fn new(
        encoding: QuadStorageEncodingName,
        quad_tables: Vec<QuadTableName>,
        partitions: usize,
    ) -> Self {
        let mut config = SessionConfig::new().with_target_partitions(partitions);
        let options = config.options_mut();
        options.optimizer.enable_dynamic_filter_pushdown = true;
        options.execution.parquet.pushdown_filters = true;

        let storage =
            Arc::new(DeltaQuadsStorage::new_in_memory(encoding, quad_tables).await);

        let context =
            RdfFusionContextBuilder::new(Arc::clone(&storage) as Arc<dyn QuadStorage>)
                .with_session_config(Some(config))
                .build()
                .unwrap();

        let node = QuadPatternNode::new(
            context.storage().encoding(),
            ActiveGraph::DefaultGraph,
            None,
            TriplePattern {
                subject: TermPattern::NamedNode(NamedNode::new_unchecked(
                    "https://my.com/s",
                )),
                predicate: rdf_fusion_common::Variable::new_unchecked("p").into(),
                object: rdf_fusion_common::Variable::new_unchecked("o").into(),
            },
        );

        Self {
            session: context.session_context().clone(),
            storage,
            node,
        }
    }

    /// Inserts quads directly into the storage as a new transaction and optimizes the storage.
    async fn with_existing_quads(self, quads: &[Quad]) -> Self {
        self.insert(quads).await;
        self.storage.optimize(&self.session.state()).await.unwrap();
        self
    }

    /// Inserts quads directly into the storage as a new transaction.
    async fn insert(&self, quads: &[Quad]) {
        let df = quads_to_plain_term_dataframe(&self.session, quads);
        let transaction = self
            .storage
            .begin_transaction(&self.session.state())
            .await
            .unwrap();
        transaction.insert(df).await.unwrap();
        transaction.commit().await.unwrap();
    }

    /// Removes quads from the storage as a new transaction.
    async fn remove(&self, quads: &[Quad]) {
        let df = quads_to_plain_term_dataframe(&self.session, quads);
        let transaction = self
            .storage
            .begin_transaction(&self.session.state())
            .await
            .unwrap();
        transaction.remove(df).await.unwrap();
        transaction.commit().await.unwrap();
    }

    /// Returns the formatted string representation of the physical plan.
    async fn get_plan_string(&self) -> String {
        let context = RdfFusionContextBuilder::new(
            Arc::clone(&self.storage) as Arc<dyn QuadStorage>
        )
        .with_session_config(Some(self.session.copied_config()))
        .with_runtime_env(Some(self.session.runtime_env()))
        .build()
        .unwrap();
        let session_context = context.session_context();

        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(self.node.clone()),
        });
        let execution_plan = session_context
            .state()
            .create_physical_plan(&logical_plan)
            .await
            .unwrap();

        displayable(execution_plan.as_ref())
            .indent(false)
            .to_string()
    }
}

/// Helper to cleanly instantiate a test quad.
fn test_quad(s: &str, p: &str, o: &str, g: &str) -> Quad {
    Quad::new(
        NamedNode::new_unchecked(s),
        NamedNode::new_unchecked(p),
        NamedNode::new_unchecked(o),
        NamedNode::new_unchecked(g),
    )
}

struct TestSnapshot;

#[async_trait]
impl QuadStorageSnapshot for TestSnapshot {
    async fn planners(
        &self,
        _context: &rdf_fusion_extensions::RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        vec![Arc::new(TestPlanner)]
    }

    async fn scan_quad_pattern(
        &self,
        _pattern: &rdf_fusion_common::QuadPattern,
        _projection: Option<Vec<usize>>,
        _session_state: &datafusion::execution::SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        unreachable!("storage planner must handle the quad pattern")
    }

    async fn named_graphs(
        &self,
        _state: &datafusion::execution::SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        unimplemented!()
    }

    async fn len(
        &self,
        _state: &datafusion::execution::SessionState,
    ) -> Result<usize, StorageError> {
        unimplemented!()
    }
}

struct TestPlanner;

#[async_trait]
impl ExtensionPlanner for TestPlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session: &dyn Session,
        _planning_ctx: &PhysicalPlanningContext,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(_) = node.as_any().downcast_ref::<QuadPatternNode>() else {
            return Ok(None);
        };

        plan_err!("TestPlanner was executed!")
    }
}
