use crate::delta::error::DeltaQuadStorageError;
use crate::delta::objectids::EncodeAsObjectIdDeltaExec;
use crate::delta::scan::DeltaQuadStorageScanExec;
use crate::delta::scan_plan_builder::DeltaQuadStorageScanPlanBuilder;
use crate::delta::snapshot::DeltaQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::common::plan_err;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{LogicalPlan, UserDefinedLogicalNode};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::Column as PhysicalColumn;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use rdf_fusion_common::DFResult;
use rdf_fusion_logical::encoding::object_id::EncodeAsObjectIdNode;
use rdf_fusion_logical::quad_pattern::QuadPatternNode;
use std::sync::Arc;

/// A planner for converting logical quad scans into physical plans that are realized with the
/// [`DeltaQuadStorageSnapshot`].
pub struct DeltaQuadStoragePlanner {
    /// The storage snapshot
    snapshot: DeltaQuadStorageSnapshot,
}

impl DeltaQuadStoragePlanner {
    /// Creates a new [`DeltaQuadStoragePlanner`].
    pub fn new(snapshot: DeltaQuadStorageSnapshot) -> Self {
        Self { snapshot }
    }

    /// Implements the plan building process.
    async fn plan_scan(
        &self,
        session_state: &SessionState,
        node: &QuadPatternNode,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let mut builder = DeltaQuadStorageScanPlanBuilder::new(
            session_state.clone(),
            node.quad_pattern().clone(),
            self.snapshot.encoding().clone(),
        )
        .with_best_index(self.snapshot.indexes())?
        .with_changeset_for_log(self.snapshot.log(), Some(self.snapshot.version()))
        .await?;

        if let Some(transactional) = self.snapshot.transactional_changeset() {
            builder = builder.with_changeset(Arc::clone(transactional));
        }

        let scan_planning_result = builder.build().await?;

        let mut exec: Arc<dyn ExecutionPlan> =
            Arc::new(DeltaQuadStorageScanExec::try_new(
                Arc::clone(self.snapshot.log()),
                node.quad_pattern().clone(),
                scan_planning_result.changeset_version_range,
                scan_planning_result.scan,
                scan_planning_result.chosen_index.map(|idx| idx.to_string()),
            )?);

        if let Some(projection) = &node.projection {
            let schema = exec.schema();
            let exprs = projection
                .iter()
                .map(|&i| {
                    let field = schema.field(i);
                    (
                        Arc::new(PhysicalColumn::new(field.name(), i))
                            as Arc<dyn PhysicalExpr>,
                        field.name().to_string(),
                    )
                })
                .collect::<Vec<_>>();

            let projection_exec = ProjectionExec::try_new(exprs, Arc::clone(&exec))?;
            if let Some(pushed) = exec.try_swapping_with_projection(&projection_exec)? {
                exec = pushed;
            } else {
                exec = Arc::new(projection_exec);
            }
        }

        Ok(exec)
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

        let Some(mapping) = self.snapshot.object_id_mapping() else {
            return plan_err!("Object ID mapping is not available for this storage");
        };

        let physical_plan = Arc::new(EncodeAsObjectIdDeltaExec::try_new(
            Arc::clone(&physical_inputs[0]),
            Arc::clone(mapping),
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
    use insta::{Settings, assert_snapshot};
    use rdf_fusion_common::{NamedNode, Quad, TermPattern, TriplePattern};
    use rdf_fusion_encoding::{QuadStorageEncodingName, quads_to_plain_term_dataframe};
    use rdf_fusion_execution::RdfFusionContextBuilder;
    use rdf_fusion_extensions::storage::QuadStorage;
    use rdf_fusion_logical::ActiveGraph;

    #[tokio::test]
    async fn test_planner_skips_apply_changeset_when_versions_match() {
        let (session, storage, node) = setup(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
        )
        .await;
        let planner =
            DeltaQuadStoragePlanner::new(storage.snapshot_impl().await.unwrap());
        let plan = plan_node(&planner, &node, &session).await;
        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
            (r"part-[0-9a-f-]+\.snappy\.parquet", "<file>"),
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
        let (session, storage, node) =
            setup(QuadStorageEncodingName::String, vec![IndexComponents::GSPO]).await;

        let planner =
            DeltaQuadStoragePlanner::new(storage.snapshot_impl().await.unwrap());
        let plan = plan_node(&planner, &node, &session).await;
        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
            (r"part-[0-9a-f-]+\.snappy\.parquet", "<file>"),
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
        let (session, storage, node) =
            setup(QuadStorageEncodingName::ObjectId, vec![]).await;
        let planner =
            DeltaQuadStoragePlanner::new(storage.snapshot_impl().await.unwrap());
        let plan = plan_node(&planner, &node, &session).await;

        assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
            @"EmptyExec"
        )
    }

    #[tokio::test]
    async fn test_no_index_with_change() {
        let (session, storage, node) =
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

        let transaction = storage.begin_transaction(&session.state()).await.unwrap();
        transaction.insert(df).await.unwrap();
        transaction.commit().await.unwrap();

        let planner =
            DeltaQuadStoragePlanner::new(storage.snapshot_impl().await.unwrap());
        let plan = plan_node(&planner, &node, &session).await;

        let mut settings = Settings::default();
        settings.add_filter(r"part-.*\.parquet", "<name>.parquet");
        settings.bind(|| {
            assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
                @"
            ProjectionExec: expr=[predicate@0 as p, object@1 as o]
              ProjectionExec: expr=[predicate@2 as predicate, object@3 as object]
                FilterExec: graph@0 IS NULL AND subject@1 = 4
                  DataSourceExec: partitions=1, partition_sizes=[1]
            "
            )
        })
    }

    #[tokio::test]
    async fn test_planner_with_additions() {
        let (session, storage, node) = setup(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
        )
        .await;

        let transaction = storage.begin_transaction(&session.state()).await.unwrap();
        transaction
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
        transaction.commit().await.unwrap();

        let planner =
            DeltaQuadStoragePlanner::new(storage.snapshot_impl().await.unwrap());
        let plan = plan_node(&planner, &node, &session).await;
        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
            (r"part-.*\.parquet", "<file>.parquet"),
        ]}, {
                assert_snapshot!(
                    print_scan_implementation(plan.as_ref()),
                        @"
                ProjectionExec: expr=[predicate@<col> as p, object@<col> as o]
                  ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                    UnionExec
                      HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(graph@<col>, graph@<col>), (subject@<col>, subject@<col>), (predicate@<col>, predicate@<col>), (object@<col>, object@<col>)], NullsEqual: true
                        FilterExec: graph@<col> IS NULL AND subject@<col> = 4
                          DataSourceExec: partitions=1, partition_sizes=[1]
                        DeltaScan
                          DataSourceExec: file_groups={1 group: [[]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@<col> IS NULL AND subject@<col> = 4, pruning_predicate=graph_null_count@<col> > 0 AND subject_null_count@<col> != row_count@<col> AND subject_min@<col> <= 4 AND 4 <= subject_max@<col>, required_guarantees=[subject in (4)]
                      FilterExec: graph@<col> IS NULL AND subject@<col> = 4
                        DataSourceExec: partitions=1, partition_sizes=[1]
                "
                )
        });
    }

    #[tokio::test]
    async fn test_planner_with_deletions_inserts_anti_join() {
        let (session, storage, node) = setup(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
        )
        .await;

        let transaction = storage.begin_transaction(&session.state()).await.unwrap();
        transaction
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
        transaction.commit().await.unwrap();

        let planner =
            DeltaQuadStoragePlanner::new(storage.snapshot_impl().await.unwrap());
        let plan = plan_node(&planner, &node, &session).await;

        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
            (r"part-.*\.parquet", "<file>.parquet"),
        ]}, {
            assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
                @"
            ProjectionExec: expr=[predicate@<col> as p, object@<col> as o]
              ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(graph@<col>, graph@<col>), (subject@<col>, subject@<col>), (predicate@<col>, predicate@<col>), (object@<col>, object@<col>)], NullsEqual: true
                  FilterExec: graph@<col> IS NULL AND subject@<col> = 4
                    DataSourceExec: partitions=1, partition_sizes=[1]
                  DeltaScan
                    DataSourceExec: file_groups={1 group: [[]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@<col> IS NULL AND subject@<col> = 4, pruning_predicate=graph_null_count@<col> > 0 AND subject_null_count@<col> != row_count@<col> AND subject_min@<col> <= 4 AND 4 <= subject_max@<col>, required_guarantees=[subject in (4)]
            "
            );
        });
    }

    #[tokio::test]
    async fn test_planner_with_additions_and_deletions() {
        let (session, storage, node) = setup(
            QuadStorageEncodingName::ObjectId,
            vec![IndexComponents::GSPO],
        )
        .await;

        let transaction = storage.begin_transaction(&session.state()).await.unwrap();
        transaction
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

        transaction
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
        transaction.commit().await.unwrap();

        let planner =
            DeltaQuadStoragePlanner::new(storage.snapshot_impl().await.unwrap());
        let plan = plan_node(&planner, &node, &session).await;

        insta::with_settings!({filters => vec![
            (r"@\d", "@<col>"),
            (r"part-.*\.parquet", "<file>.parquet"),
        ]}, {
            assert_snapshot!(
                print_scan_implementation(plan.as_ref()),
                @"
            ProjectionExec: expr=[predicate@<col> as p, object@<col> as o]
              ProjectionExec: expr=[predicate@<col> as predicate, object@<col> as object]
                UnionExec
                  HashJoinExec: mode=CollectLeft, join_type=RightAnti, on=[(graph@<col>, graph@<col>), (subject@<col>, subject@<col>), (predicate@<col>, predicate@<col>), (object@<col>, object@<col>)], NullsEqual: true
                    CoalescePartitionsExec
                      UnionExec
                        FilterExec: graph@<col> IS NULL AND subject@<col> = 8
                          DataSourceExec: partitions=1, partition_sizes=[1]
                        FilterExec: graph@<col> IS NULL AND subject@<col> = 8
                          DataSourceExec: partitions=1, partition_sizes=[1]
                    DeltaScan
                      DataSourceExec: file_groups={1 group: [[]]}, projection=[graph, subject, predicate, object], file_type=parquet, predicate=graph@<col> IS NULL AND subject@<col> = 8, pruning_predicate=graph_null_count@<col> > 0 AND subject_null_count@<col> != row_count@<col> AND subject_min@<col> <= 8 AND 8 <= subject_max@<col>, required_guarantees=[subject in (8)]
                  FilterExec: graph@<col> IS NULL AND subject@<col> = 8
                    DataSourceExec: partitions=1, partition_sizes=[1]
            "
                    );

        });
    }

    async fn setup(
        encoding: QuadStorageEncodingName,
        indexes: Vec<IndexComponents>,
    ) -> (SessionContext, Arc<DeltaQuadStorage>, QuadPatternNode) {
        let mut config = SessionConfig::new().with_target_partitions(1);
        let options = config.options_mut();
        options.optimizer.enable_dynamic_filter_pushdown = true;
        options.execution.parquet.pushdown_filters = true;

        let storage = Arc::new(DeltaQuadStorage::new_in_memory(encoding, indexes).await);

        let storage = Arc::clone(&storage);
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
                predicate: rdf_fusion_common::Variable::new_unchecked("p").into(),
                object: rdf_fusion_common::Variable::new_unchecked("o").into(),
            },
        );

        (context.session_context().clone(), storage, node)
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
