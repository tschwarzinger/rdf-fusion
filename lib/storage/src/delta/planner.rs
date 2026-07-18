use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::objectids::EncodeAsObjectIdDeltaExec;
use crate::delta::scan_plan_builder::DeltaQuadsStorageScanPlanBuilder;
use crate::delta::snapshot::DeltaQuadsStorageSnapshot;
use async_trait::async_trait;
use datafusion::common::plan_err;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::DFResult;
use rdf_fusion_logical::encoding::object_id::EncodeAsObjectIdNode;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::sync::Arc;

/// A planner for converting logical quad scans into physical plans that are realized with the
/// [`DeltaQuadsStorageSnapshot`].
pub struct DeltaQuadsStoragePlanner {
    /// The storage snapshot
    snapshot: DeltaQuadsStorageSnapshot,
}

impl DeltaQuadsStoragePlanner {
    /// Creates a new [`DeltaQuadsStoragePlanner`].
    pub fn new(snapshot: DeltaQuadsStorageSnapshot) -> Self {
        Self { snapshot }
    }

    /// Implements the plan building process.
    async fn plan_scan(
        &self,
        session_state: &SessionState,
        node: &QuadPatternNode,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadsStorageError> {
        let mut builder = DeltaQuadsStorageScanPlanBuilder::new(
            session_state.clone(),
            node.quad_pattern().clone(),
            self.snapshot.encoding().clone(),
        )
        .with_best_index(self.snapshot.indexes())?
        .with_changeset_for_log(self.snapshot.log(), Some(self.snapshot.version()))
        .await?
        .with_projection_indices(node.projection.clone());

        if let Some(transactional) = self.snapshot.transactional_changeset() {
            builder = builder.with_changeset(Arc::clone(transactional));
        }

        builder.build().await.map(|r| r.scan)
    }

    /// Tries to plan a [`QuadPatternNode`].
    async fn try_plan_quad_pattern_scan(
        &self,
        session_state: &SessionState,
        node: &dyn UserDefinedLogicalNode,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() else {
            return Ok(None);
        };

        let scan_plan = self
            .plan_scan(session_state, node)
            .await
            .map_err(|err| DataFusionError::Plan(err.to_string()))?;

        Ok(Some(scan_plan))
    }

    /// Tries to plan a [`EncodeAsObjectIdNode`].
    async fn try_plan_encode_as_object_id(
        &self,
        node: &dyn UserDefinedLogicalNode,
        physical_inputs: &[Arc<dyn ExecutionPlan>],
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<EncodeAsObjectIdNode>() else {
            return Ok(None);
        };

        let Some(mapping) = self.snapshot.object_id_mapping() else {
            return plan_err!("Object ID mapping is not available for this storage");
        };

        let physical_plan = EncodeAsObjectIdDeltaExec::try_new(
            Arc::clone(&physical_inputs[0]),
            Arc::clone(mapping),
            Arc::clone(node.schema().inner()),
        )?
        .with_buffering_options(
            self.snapshot.options().max_buffered_rows,
            self.snapshot.options().max_buffered_ids,
        );

        Ok(Some(Arc::new(physical_plan)))
    }
}

#[async_trait]
impl ExtensionPlanner for DeltaQuadsStoragePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> DFResult<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(planned) =
            self.try_plan_quad_pattern_scan(session_state, node).await?
        {
            return Ok(Some(planned));
        }

        if let Some(planned) = self
            .try_plan_encode_as_object_id(node, physical_inputs)
            .await?
        {
            return Ok(Some(planned));
        }

        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::storage::DeltaQuadsStorage;
    use crate::index::IndexComponents;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_planner::DefaultPhysicalPlanner;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use rdf_fusion_common::{NamedNode, Quad, TermPattern, TriplePattern};
    use rdf_fusion_encoding::{QuadStorageEncodingName, quads_to_plain_term_dataframe};
    use rdf_fusion_execution::RdfFusionContextBuilder;
    use rdf_fusion_extensions::storage::QuadStorage;
    use rdf_fusion_logical::ActiveGraph;

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
            vec![IndexComponents::GSPO],
            1,
        )
        .await;
        assert_plan_snapshot!(ctx.get_plan_string().await, @"EmptyExec");
    }

    #[tokio::test]
    async fn test_planner_pushes_down_filter_string_encoding() {
        let ctx = PlannerTestContext::new(
            QuadStorageEncodingName::String,
            vec![IndexComponents::GSPO],
            1,
        )
        .await;
        assert_plan_snapshot!(ctx.get_plan_string().await, @"EmptyExec");
    }

    #[tokio::test]
    async fn test_no_index_no_change() {
        let ctx =
            PlannerTestContext::new(QuadStorageEncodingName::ObjectId, vec![], 1).await;
        assert_plan_snapshot!(ctx.get_plan_string().await, @"EmptyExec");
    }

    #[tokio::test]
    async fn test_no_index_with_change() {
        let ctx =
            PlannerTestContext::new(QuadStorageEncodingName::ObjectId, vec![], 1).await;

        ctx.insert(&[test_quad(
            "https://my.com/s",
            "https://my.com/p",
            "https://my.com/o",
            "https://my.com/g",
        )])
        .await;

        assert_plan_snapshot!(ctx.get_plan_string().await, @"
        ProjectionExec: expr=[predicate@2 as p, object@3 as o]
          FilterExec: graph@0 IS NULL AND subject@1 = 4
            DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    #[tokio::test]
    async fn test_planner_with_additions() {
        let ctx = PlannerTestContext::new(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
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
                ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.at/> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[indexes/GSPO/<file>.parquet]]}, projection=[graph, subject, predicate, object], output_ordering=[graph@0 ASC, subject@1 ASC, predicate@2 ASC, object@3 ASC], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = 8, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 8 AND 8 <= subject_max@2, required_guarantees=[subject in (8)]
                SortExec: expr=[graph@0 ASC, predicate@2 ASC, object@3 ASC], preserve_partitioning=[false]
                  FilterExec: graph@0 IS NULL AND subject@1 = 8
                    DataSourceExec: partitions=1, partition_sizes=[1]
        "
        );
    }

    #[tokio::test]
    async fn test_planner_with_deletions_inserts_anti_join() {
        let ctx = PlannerTestContext::new(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
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
              FilterExec: graph@0 IS NULL AND subject@1 = 4
                DataSourceExec: partitions=1, partition_sizes=[1]
            ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.at/> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[indexes/GSPO/<file>.parquet]]}, projection=[graph, subject, predicate, object], output_ordering=[graph@0 ASC, subject@1 ASC, predicate@2 ASC, object@3 ASC], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = 4, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 4 AND 4 <= subject_max@2, required_guarantees=[subject in (4)]
        ");
    }

    #[tokio::test]
    async fn test_planner_with_additions_and_deletions() {
        let ctx = PlannerTestContext::new(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
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
                    FilterExec: graph@0 IS NULL AND subject@1 = 12
                      DataSourceExec: partitions=1, partition_sizes=[1]
                  ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.at/> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[indexes/GSPO/<file>.parquet]]}, projection=[graph, subject, predicate, object], output_ordering=[graph@0 ASC, subject@1 ASC, predicate@2 ASC, object@3 ASC], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = 12, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 12 AND 12 <= subject_max@2, required_guarantees=[subject in (12)]
                SortExec: expr=[graph@0 ASC, predicate@2 ASC, object@3 ASC], preserve_partitioning=[false]
                  FilterExec: graph@0 IS NULL AND subject@1 = 12
                    DataSourceExec: partitions=1, partition_sizes=[1]
        ");
    }

    #[tokio::test]
    async fn test_planner_with_additions_multiple_partitions() {
        let ctx = PlannerTestContext::new(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
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
                ParquetQuadScanExec: active_graph=Default Graph, triple_pattern=[<https://my.at/> ?p ?o], blank_node_mode=Variable, file_groups={1 group: [[indexes/GSPO/<file>.parquet]]}, projection=[graph, subject, predicate, object], output_ordering=[graph@0 ASC, subject@1 ASC, predicate@2 ASC, object@3 ASC], file_type=parquet, predicate=graph@0 IS NULL AND subject@1 = 8, pruning_predicate=graph_null_count@0 > 0 AND subject_null_count@3 != row_count@4 AND subject_min@1 <= 8 AND 8 <= subject_max@2, required_guarantees=[subject in (8)]
                SortExec: expr=[graph@0 ASC, predicate@2 ASC, object@3 ASC], preserve_partitioning=[false]
                  FilterExec: graph@0 IS NULL AND subject@1 = 8
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
            indexes: Vec<IndexComponents>,
            partitions: usize,
        ) -> Self {
            let mut config = SessionConfig::new().with_target_partitions(partitions);
            let options = config.options_mut();
            options.optimizer.enable_dynamic_filter_pushdown = true;
            options.execution.parquet.pushdown_filters = true;

            let storage =
                Arc::new(DeltaQuadsStorage::new_in_memory(encoding, indexes).await);

            let context = RdfFusionContextBuilder::new(
                Arc::clone(&storage) as Arc<dyn QuadStorage>
            )
            .with_session_config(Some(config))
            .build()
            .unwrap();

            let node = QuadPatternNode::new(
                context.storage().encoding(),
                ActiveGraph::DefaultGraph,
                None,
                TriplePattern {
                    subject: TermPattern::NamedNode(NamedNode::new_unchecked(
                        "https://my.at/",
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
            let planner = DeltaQuadsStoragePlanner::new(
                self.storage.snapshot_impl().await.unwrap(),
            );
            let plan = planner
                .plan_extension(
                    &DefaultPhysicalPlanner::default(),
                    &self.node,
                    &[],
                    &[],
                    &self.session.state(),
                )
                .await
                .unwrap()
                .unwrap();

            displayable(plan.as_ref()).indent(false).to_string()
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
}
