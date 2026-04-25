use crate::delta::error::DeltaQuadStorageError;
use crate::delta::objectids::EncodeAsObjectIdDeltaExec;
use crate::delta::scan::DeltaQuadStorageScanExec;
use crate::delta::scan_plan_builder::DeltaQuadStorageScanPlanBuilder;
use crate::delta::storage::DeltaQuadStorage;
use async_trait::async_trait;
use datafusion::common::plan_err;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_logical::encoding::object_id::EncodeAsObjectIdNode;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// A planner for converting logical quad scans into physical plans that are realized with the
/// [`DeltaQuadStorage`].
pub struct DeltaQuadStoragePlanner {
    /// The storage
    storage: Arc<DeltaQuadStorage>,
}

impl DeltaQuadStoragePlanner {
    /// Creates a new [`DeltaQuadStoragePlanner`].
    pub fn new(storage: Arc<DeltaQuadStorage>) -> Self {
        Self { storage }
    }

    /// Implements the plan building process.
    async fn plan_scan(
        &self,
        session_state: &SessionState,
        node: &QuadPatternNode,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let scan_planning_result = DeltaQuadStorageScanPlanBuilder::new(
            session_state.clone(),
            node.quad_pattern().clone(),
            self.storage.encoding(),
        )
        .with_best_index(self.storage.indexes())
        .await?
        .with_changeset_for_log(self.storage.log(), None)
        .await?
        .build()
        .await?;

        Ok(Arc::new(DeltaQuadStorageScanExec::try_new(
            Arc::clone(self.storage.log()),
            node.quad_pattern().clone(),
            scan_planning_result.changeset_version_range,
            scan_planning_result.scan,
            scan_planning_result.chosen_index.map(|idx| idx.to_string()),
        )?))
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

        assert_eq!(
            node.schema().inner().as_ref(),
            scan_plan.schema().as_ref(),
            "Schema mismatch after planning quad pattern node"
        );

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

        let Some(mapping) = self.storage.delta_object_id_mapping() else {
            return plan_err!("Object ID mapping is not available for this storage");
        };

        let physical_plan = Arc::new(EncodeAsObjectIdDeltaExec::try_new(
            Arc::clone(&physical_inputs[0]),
            mapping,
            Arc::clone(node.schema().inner()),
        )?);

        assert_eq!(
            node.schema().inner().as_ref(),
            physical_plan.schema().as_ref(),
            "Schema mismatch after planning quad pattern node"
        );

        Ok(Some(physical_plan))
    }
}

#[async_trait]
impl ExtensionPlanner for DeltaQuadStoragePlanner {
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
    use crate::delta::storage::DeltaQuadStorage;
    use crate::index::IndexComponents;
    use datafusion::physical_plan::displayable;
    use datafusion::physical_planner::DefaultPhysicalPlanner;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use insta::assert_snapshot;
    use rdf_fusion_encoding::{QuadStorageEncodingName, quads_to_plain_term_dataframe};
    use rdf_fusion_execution::RdfFusionContextBuilder;
    use rdf_fusion_extensions::storage::QuadStorage;
    use rdf_fusion_logical::ActiveGraph;
    use rdf_fusion_model::{NamedNode, Quad, TermPattern, TriplePattern};

    #[tokio::test]
    async fn test_planner_skips_apply_changeset_when_versions_match() {
        let (session, _, planner, node) = setup(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
        )
        .await;
        let plan = plan_node(&planner, &node, &session).await;
        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
        ]}, {
            assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
                @r"
            ProjectionExec: expr=[predicate@<col> as p, object@<col> as o]
              DeltaScan
                DataSourceExec: file_groups={1 group: [[]]}, projection=[predicate, object], file_type=parquet, predicate=graph@<col> IS NULL AND subject@<col> = 0, pruning_predicate=graph_null_count@<col> > 0 AND subject_null_count@<col> != row_count@<col> AND subject_min@<col> <= 0 AND 0 <= subject_max@<col>, required_guarantees=[subject in (0)]
            "
            )
        });
    }

    #[tokio::test]
    async fn test_planner_pushes_down_filter_string_encoding() {
        let (session, _, planner, node) =
            setup(QuadStorageEncodingName::String, vec![IndexComponents::GSPO]).await;
        let plan = plan_node(&planner, &node, &session).await;
        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
        ]}, {
            assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
                @r"
            ProjectionExec: expr=[predicate@<col> as p, object@<col> as o]
              DeltaScan
                DataSourceExec: file_groups={1 group: [[]]}, projection=[predicate, object], file_type=parquet, predicate=graph@<col> IS NULL AND subject@<col> = <https://my.at/>, pruning_predicate=graph_null_count@<col> > 0 AND subject_null_count@<col> != row_count@<col> AND subject_min@<col> <= <https://my.at/> AND <https://my.at/> <= subject_max@<col>, required_guarantees=[subject in (<https://my.at/>)]
            "
            )
        });
    }

    #[tokio::test]
    async fn test_no_index_no_change() {
        let (session, _, planner, node) =
            setup(QuadStorageEncodingName::ObjectId, vec![]).await;
        let plan = plan_node(&planner, &node, &session).await;

        assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
            @"EmptyExec"
        )
    }

    #[tokio::test]
    async fn test_no_index_with_change() {
        let (session, storage, planner, node) =
            setup(QuadStorageEncodingName::ObjectId, vec![]).await;

        let df = quads_to_plain_term_dataframe(
            &session,
            &[Quad::new(
                NamedNode::new_unchecked("https://my.com/s"),
                NamedNode::new_unchecked("https://my.com/p"),
                NamedNode::new_unchecked("https://my.com/o"),
                NamedNode::new_unchecked("https://my.com/g"),
            )],
        );

        storage.insert(df).await.unwrap();

        let plan = plan_node(&planner, &node, &session).await;

        assert_snapshot!(
            print_scan_implementation(plan.as_ref()),
            @r"
        ProjectionExec: expr=[predicate@0 as p, object@1 as o]
          ProjectionExec: expr=[predicate@2 as predicate, object@3 as object]
            FilterExec: graph@0 IS NULL AND subject@1 = 4
              DataSourceExec: partitions=1, partition_sizes=[1]
        "
        )
    }

    #[tokio::test]
    async fn test_planner_with_additions() {
        let (session, storage, planner, node) = setup(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
        )
        .await;

        storage
            .insert(quads_to_plain_term_dataframe(
                &session,
                &[Quad::new(
                    NamedNode::new_unchecked("https://my.com/s"),
                    NamedNode::new_unchecked("https://my.com/p"),
                    NamedNode::new_unchecked("https://my.com/o"),
                    NamedNode::new_unchecked("https://my.com/g"),
                )],
            ))
            .await
            .unwrap();

        let plan = plan_node(&planner, &node, &session).await;
        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
        ]}, {
                assert_snapshot!(
                    print_scan_implementation(plan.as_ref()),
                        @r"
                ProjectionExec: expr=[predicate@<col> as p, object@<col> as o]
                  UnionExec
                    HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(predicate@<col>, predicate@<col>), (object@<col>, object@<col>)], NullsEqual: true
                      ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                        FilterExec: graph@<col> IS NULL AND subject@<col> = 4
                          DataSourceExec: partitions=1, partition_sizes=[1]
                      DeltaScan
                        DataSourceExec: file_groups={1 group: [[]]}, projection=[predicate, object], file_type=parquet, predicate=graph@<col> IS NULL AND subject@<col> = 4, pruning_predicate=graph_null_count@<col> > 0 AND subject_null_count@<col> != row_count@<col> AND subject_min@<col> <= 4 AND 4 <= subject_max@<col>, required_guarantees=[subject in (4)]
                    ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                      FilterExec: graph@<col> IS NULL AND subject@<col> = 4
                        DataSourceExec: partitions=1, partition_sizes=[1]
                "
                )
        });
    }

    #[tokio::test]
    async fn test_planner_with_deletions_inserts_anti_join() {
        let (session, storage, planner, node) = setup(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
        )
        .await;

        storage
            .remove(quads_to_plain_term_dataframe(
                &session,
                &[Quad::new(
                    NamedNode::new_unchecked("https://my.com/s"),
                    NamedNode::new_unchecked("https://my.com/p"),
                    NamedNode::new_unchecked("https://my.com/o"),
                    NamedNode::new_unchecked("https://my.com/g"),
                )],
            ))
            .await
            .unwrap();

        let plan = plan_node(&planner, &node, &session).await;

        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
        ]}, {
            assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
                @r"
            ProjectionExec: expr=[predicate@<col> as p, object@<col> as o]
              HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(predicate@<col>, predicate@<col>), (object@<col>, object@<col>)], NullsEqual: true
                ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                  FilterExec: graph@<col> IS NULL AND subject@<col> = 4
                    DataSourceExec: partitions=1, partition_sizes=[1]
                DeltaScan
                  DataSourceExec: file_groups={1 group: [[]]}, projection=[predicate, object], file_type=parquet, predicate=graph@<col> IS NULL AND subject@<col> = 4, pruning_predicate=graph_null_count@<col> > 0 AND subject_null_count@<col> != row_count@<col> AND subject_min@<col> <= 4 AND 4 <= subject_max@<col>, required_guarantees=[subject in (4)]
            "
            );
        });
    }

    #[tokio::test]
    async fn test_planner_with_additions_and_deletions() {
        let (session, storage, planner, node) = setup(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
        )
        .await;

        storage
            .insert(quads_to_plain_term_dataframe(
                &session,
                &[Quad::new(
                    NamedNode::new_unchecked("https://my.com/s"),
                    NamedNode::new_unchecked("https://my.com/p"),
                    NamedNode::new_unchecked("https://my.com/o"),
                    NamedNode::new_unchecked("https://my.com/g"),
                )],
            ))
            .await
            .unwrap();

        storage
            .remove(quads_to_plain_term_dataframe(
                &session,
                &[Quad::new(
                    NamedNode::new_unchecked("https://my.com/s2"),
                    NamedNode::new_unchecked("https://my.com/p2"),
                    NamedNode::new_unchecked("https://my.com/o2"),
                    NamedNode::new_unchecked("https://my.com/g2"),
                )],
            ))
            .await
            .unwrap();

        let plan = plan_node(&planner, &node, &session).await;

        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
        ]}, {
            assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
                @r"
            ProjectionExec: expr=[predicate@<col> as p, object@<col> as o]
              UnionExec
                HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(predicate@<col>, predicate@<col>), (object@<col>, object@<col>)], NullsEqual: true
                  CoalescePartitionsExec
                    UnionExec
                      ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                        FilterExec: graph@<col> IS NULL AND subject@<col> = 8
                          DataSourceExec: partitions=1, partition_sizes=[1]
                      ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                        FilterExec: graph@<col> IS NULL AND subject@<col> = 8
                          DataSourceExec: partitions=1, partition_sizes=[1]
                  DeltaScan
                    DataSourceExec: file_groups={1 group: [[]]}, projection=[predicate, object], file_type=parquet, predicate=graph@<col> IS NULL AND subject@<col> = 8, pruning_predicate=graph_null_count@<col> > 0 AND subject_null_count@<col> != row_count@<col> AND subject_min@<col> <= 8 AND 8 <= subject_max@<col>, required_guarantees=[subject in (8)]
                ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                  FilterExec: graph@<col> IS NULL AND subject@<col> = 8
                    DataSourceExec: partitions=1, partition_sizes=[1]
            "
            );
        });
    }

    async fn setup(
        encoding: QuadStorageEncodingName,
        indexes: Vec<IndexComponents>,
    ) -> (
        SessionContext,
        Arc<DeltaQuadStorage>,
        DeltaQuadStoragePlanner,
        QuadPatternNode,
    ) {
        let mut config = SessionConfig::new().with_target_partitions(1);
        let options = config.options_mut();
        options.optimizer.enable_dynamic_filter_pushdown = true;
        options.execution.parquet.pushdown_filters = true;

        let storage = Arc::new(
            DeltaQuadStorage::new_in_memory(
                encoding,
                indexes,
                Arc::new(Default::default()),
            )
            .await,
        );

        let storage = Arc::clone(&storage);
        let planner = DeltaQuadStoragePlanner::new(Arc::clone(&storage));
        let context =
            RdfFusionContextBuilder::new(Arc::clone(&storage) as Arc<dyn QuadStorage>)
                .with_single_partition_session_config()
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
                predicate: rdf_fusion_model::Variable::new_unchecked("p").into(),
                object: rdf_fusion_model::Variable::new_unchecked("o").into(),
            },
        );

        (context.session_context().clone(), storage, planner, node)
    }

    async fn plan_node(
        planner: &DeltaQuadStoragePlanner,
        node: &QuadPatternNode,
        session: &SessionContext,
    ) -> Arc<dyn ExecutionPlan> {
        planner
            .plan_extension(
                &DefaultPhysicalPlanner::default(),
                node,
                &[],
                &[],
                &session.state(),
            )
            .await
            .unwrap()
            .unwrap()
    }

    /// Provides the inner scan plan of an [`DeltaQuadStorageScanExec`] as a string.
    fn print_scan_implementation(plan: &dyn ExecutionPlan) -> String {
        let plan = plan
            .as_any()
            .downcast_ref::<DeltaQuadStorageScanExec>()
            .unwrap();
        displayable(plan.inner_scan().as_ref())
            .indent(false)
            .to_string()
    }
}
