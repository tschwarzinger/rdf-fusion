use super::*;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::displayable;
use datafusion::prelude::{SessionConfig, SessionContext};
use rdf_fusion_common::{NamedNode, Quad, TermPattern, TriplePattern};
use rdf_fusion_encoding::{QuadStorageEncodingName, quads_to_plain_term_dataframe};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use rdf_fusion_storage::quad_tables::QuadTableName;

/// Automatically applies standard filters for Parquet file names before snapshotting.
macro_rules! assert_plan_snapshot {
        ($plan_str:expr, @$snapshot:literal) => {
            let plan_str = $plan_str;

            insta::with_settings!({filters => vec![
                (r"part-[0-9a-f-]+\.snappy\.parquet", "<file>"),
                (r"part-[0-9a-f-]+\.parquet", "<file>.parquet"),
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
        ProjectionExec: expr=[predicate@2 as p, object@3 as o]
          SortedDistinctExec: [graph@0 ASC, predicate@2 ASC, object@3 ASC]
            SortPreservingMergeExec: [graph@0 ASC, predicate@2 ASC, object@3 ASC]
              UnionExec
                ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.com/s> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[quad-tables/GSPO/<file>.parquet]]}, projection=[graph, subject, predicate, object], output_ordering=[graph@0 ASC, subject@1 ASC, predicate@2 ASC, object@3 ASC], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = 5, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 5 AND 5 <= subject_max@2, required_guarantees=[subject in (5)]
                SortExec: expr=[graph@0 ASC, predicate@2 ASC, object@3 ASC], preserve_partitioning=[false]
                  FilterExec: graph@0 IS NULL AND subject@1 = 5
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

    assert_plan_snapshot!(ctx.get_plan_string().await, @"
        ProjectionExec: expr=[predicate@2 as p, object@3 as o]
          SortMergeJoinExec: join_type=RightAnti, on=[(graph@0, graph@0), (subject@1, subject@1), (predicate@2, predicate@2), (object@3, object@3)], NullsEqual: true
            SortExec: expr=[graph@0 ASC, predicate@2 ASC, object@3 ASC], preserve_partitioning=[false]
              FilterExec: graph@0 IS NULL AND subject@1 = 1
                DataSourceExec: partitions=1, partition_sizes=[1]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.com/s> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[quad-tables/GSPO/<file>.parquet]]}, projection=[graph, subject, predicate, object], output_ordering=[graph@0 ASC, subject@1 ASC, predicate@2 ASC, object@3 ASC], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = 1, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 1 AND 1 <= subject_max@2, required_guarantees=[subject in (1)]
        ");
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
        ProjectionExec: expr=[predicate@2 as p, object@3 as o]
          SortedDistinctExec: [graph@0 ASC, predicate@2 ASC, object@3 ASC]
            SortPreservingMergeExec: [graph@0 ASC, predicate@2 ASC, object@3 ASC]
              UnionExec
                SortMergeJoinExec: join_type=RightAnti, on=[(graph@0, graph@0), (subject@1, subject@1), (predicate@2, predicate@2), (object@3, object@3)], NullsEqual: true
                  SortExec: expr=[graph@0 ASC, predicate@2 ASC, object@3 ASC], preserve_partitioning=[false]
                    FilterExec: graph@0 IS NULL AND subject@1 = 1
                      DataSourceExec: partitions=1, partition_sizes=[1]
                  ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.com/s> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[quad-tables/GSPO/<file>.parquet]]}, projection=[graph, subject, predicate, object], output_ordering=[graph@0 ASC, subject@1 ASC, predicate@2 ASC, object@3 ASC], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = 1, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 1 AND 1 <= subject_max@2, required_guarantees=[subject in (1)]
                SortExec: expr=[graph@0 ASC, predicate@2 ASC, object@3 ASC], preserve_partitioning=[false]
                  FilterExec: graph@0 IS NULL AND subject@1 = 1
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
        ProjectionExec: expr=[predicate@2 as p, object@3 as o]
          SortedDistinctExec: [graph@0 ASC, predicate@2 ASC, object@3 ASC]
            SortPreservingMergeExec: [graph@0 ASC, predicate@2 ASC, object@3 ASC]
              UnionExec
                ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.com/s> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[quad-tables/GSPO/<file>.parquet]]}, projection=[graph, subject, predicate, object], output_ordering=[graph@0 ASC, subject@1 ASC, predicate@2 ASC, object@3 ASC], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = 5, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 5 AND 5 <= subject_max@2, required_guarantees=[subject in (5)]
                SortExec: expr=[graph@0 ASC, predicate@2 ASC, object@3 ASC], preserve_partitioning=[false]
                  FilterExec: graph@0 IS NULL AND subject@1 = 5
                    DataSourceExec: partitions=1, partition_sizes=[1]
        ");
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
