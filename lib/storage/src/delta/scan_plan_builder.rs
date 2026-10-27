use crate::block_cache::BlockCache;
use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::log::{
    ChangesetContext, DeltaQuadsStorageLog, DeltaQuadsStorageLogChangesetRef,
    DeltaStorageLogVersionRange,
};
use crate::delta::quad_table::{DeltaQuadsQuadTableSnapshot, add_to_object_meta};
use crate::parquet::switch::{QuadTableSwitch, SwitchableQuadTables};
use crate::parquet::{ParquetQuadScanBuilder, PushdownProjection};
use crate::quad_tables::QuadTableName;
use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, JoinType, NullEquality, Result as DFResult};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::expressions::Column as PhysColumn;
use datafusion::physical_expr::{LexOrdering, LexRequirement};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::ensure_requirements::EnsureRequirements;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext};
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use rdf_fusion_common::QuadComponent;
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::quad_pattern::{
    QuadPattern, compute_quad_pattern_filters, compute_quad_pattern_schema,
};
use rdf_fusion_physical::distinct::SortedDistinctExec;
use std::sync::Arc;

pub struct QuadPatternScanPlanningResult {
    /// The scan implementation
    pub scan: Arc<dyn ExecutionPlan>,
    /// The quad_table that was chosen to be scanned. [`None`], if no quad_table was scanned.
    pub chosen_quad_table: Option<QuadTableName>,
    /// The changese version range.
    pub changeset_version_range: Option<DeltaStorageLogVersionRange>,
}

/// A builder for constructing scan plans from an optional quad_table and an optional manual changeset.
pub struct DeltaQuadsStorageScanPlanBuilder {
    session_state: SessionState,
    pattern: QuadPattern,
    encoding: QuadStorageEncoding,
    quad_table: Option<DeltaQuadsQuadTableSnapshot>,
    /// The quad tables the scan of `quad_table` may switch to at execution time (including
    /// `quad_table`). See [`QuadTableSwitch`].
    switchable_quad_tables: Vec<DeltaQuadsQuadTableSnapshot>,
    changeset: Option<DeltaQuadsStorageLogChangesetRef>,
    projection_indices: Option<Vec<usize>>,
    cache: Option<Arc<BlockCache>>,
}

impl DeltaQuadsStorageScanPlanBuilder {
    pub fn new(
        session_state: SessionState,
        pattern: QuadPattern,
        encoding: QuadStorageEncoding,
    ) -> Self {
        Self {
            session_state,
            pattern,
            encoding,
            quad_table: None,
            switchable_quad_tables: Vec::new(),
            changeset: None,
            projection_indices: None,
            cache: None,
        }
    }

    /// Scans the quad table that is best suited for the pattern.
    ///
    /// The scan may switch to another quad table at execution time (see `QuadTableSwitch`).
    /// Only quad tables with the same log version as the chosen one are considered, as the same
    /// changeset is applied on top of them.
    pub fn with_best_quad_table(
        mut self,
        quad_tables: &[DeltaQuadsQuadTableSnapshot],
    ) -> Result<Self, DeltaQuadsStorageError> {
        let best_quad_table = quad_tables
            .iter()
            .max_by_key(|idx| {
                idx.compute_scan_score(
                    &self.pattern.active_graph,
                    &self.pattern.triple_pattern,
                    self.pattern.blank_node_mode,
                )
            })
            .cloned();
        let Some(best_quad_table) = best_quad_table else {
            return Ok(self);
        };
        let switchable_quad_tables = quad_tables
            .iter()
            .filter(|table| {
                table.log_transaction_version()
                    == best_quad_table.log_transaction_version()
            })
            .cloned()
            .collect();
        self = self.with_quad_table(best_quad_table);
        self.switchable_quad_tables = switchable_quad_tables;
        Ok(self)
    }

    pub fn with_quad_table(mut self, quad_table: DeltaQuadsQuadTableSnapshot) -> Self {
        self.quad_table = Some(quad_table);
        self.switchable_quad_tables = Vec::new();
        self
    }

    pub fn with_projection_indices(
        mut self,
        projection_indices: Option<Vec<usize>>,
    ) -> Self {
        self.projection_indices = projection_indices;
        self
    }

    pub fn with_cache(mut self, cache: Arc<BlockCache>) -> Self {
        self.cache = Some(cache);
        self
    }

    pub async fn with_changeset_for_log(
        self,
        log: &DeltaQuadsStorageLog,
        target_version: Option<u64>,
    ) -> Result<Self, DeltaQuadsStorageError> {
        let target_version = match target_version {
            None => log.version().await,
            Some(target_version) => target_version,
        };
        let quad_table_version = match self.quad_table.as_ref() {
            None => 0,
            Some(quad_table) => quad_table.log_transaction_version(),
        };

        if target_version < quad_table_version {
            return Err(DeltaQuadsStorageError::VersionError(
                "The target version is older than the quad_table version".to_string(),
            ));
        }

        match DeltaStorageLogVersionRange::try_new(quad_table_version, target_version) {
            None => Ok(self),
            Some(version_range) => {
                let changeset = log
                    .compute_changeset(&self.session_state, version_range)
                    .await?;
                Ok(self.with_changeset(changeset))
            }
        }
    }

    pub fn with_changeset(mut self, changeset: DeltaQuadsStorageLogChangesetRef) -> Self {
        self.changeset = Some(changeset);
        self
    }

    pub async fn build(
        &self,
    ) -> Result<QuadPatternScanPlanningResult, DeltaQuadsStorageError> {
        let initial_plan = match (&self.quad_table, &self.changeset) {
            (Some(quad_table), Some(changeset)) => {
                let base_scan = self
                    .scan_quad_table_physical(
                        quad_table,
                        QuadTableScanProjectionPushdown::No,
                    )
                    .await?;

                let Some(base_scan) = base_scan else {
                    return self.build_without_quad_table(changeset).await?;
                };

                let base_scan_stats = StatisticsContext::new()
                    .compute(base_scan.as_ref(), &StatisticsArgs::new())?;
                if base_scan_stats.num_rows == Precision::Exact(0) {
                    return self.build_without_quad_table(changeset).await?;
                }

                let applied_scan = self
                    .apply_changeset_data_physical(base_scan, changeset)
                    .await?;

                let scan = self.project_components_to_variables_physical(
                    applied_scan,
                    self.projection_indices.as_deref(),
                )?;

                Ok(QuadPatternScanPlanningResult {
                    scan,
                    chosen_quad_table: Some(quad_table.components()),
                    changeset_version_range: Some(changeset.version_range()),
                })
            }

            (Some(quad_table), None) => {
                let base_scan = self
                    .scan_quad_table_physical(
                        quad_table,
                        QuadTableScanProjectionPushdown::Yes(
                            self.projection_indices.clone(),
                        ),
                    )
                    .await?;
                Ok(QuadPatternScanPlanningResult {
                    scan: match base_scan {
                        None => self.build_empty_scan()?.scan,
                        Some(scan) => scan,
                    },
                    chosen_quad_table: Some(quad_table.components()),
                    changeset_version_range: None,
                })
            }

            (None, Some(changeset)) => self.build_without_quad_table(changeset).await?,

            (None, None) => self.build_empty_scan(),
        }?;

        let config = self.session_state.config_options();
        let rules = [
            Arc::new(EnsureRequirements::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(SanityCheckPlan::new()) as Arc<dyn PhysicalOptimizerRule>,
        ];
        let mut rewritten_plan = initial_plan.scan;
        for rule in rules {
            rewritten_plan = rule.optimize(rewritten_plan, config)?;
        }

        Ok(QuadPatternScanPlanningResult {
            scan: rewritten_plan,
            chosen_quad_table: initial_plan.chosen_quad_table,
            changeset_version_range: initial_plan.changeset_version_range,
        })
    }

    async fn build_without_quad_table(
        &self,
        changeset: &DeltaQuadsStorageLogChangesetRef,
    ) -> Result<
        Result<QuadPatternScanPlanningResult, DeltaQuadsStorageError>,
        DeltaQuadsStorageError,
    > {
        let filters = compute_quad_pattern_filters(&self.pattern, &self.encoding).await?;

        let context = ChangesetContext::default();
        let added_quads = changeset.added_quads(&context, &self.session_state).await?;

        let scan = match added_quads {
            None => self.build_empty_scan()?.scan,
            Some(added_quads) => {
                let filtered = self.filter_and_project(added_quads, None, &filters)?;
                self.project_components_to_variables_physical(
                    filtered,
                    self.projection_indices.as_deref(),
                )?
            }
        };

        Ok(Ok(QuadPatternScanPlanningResult {
            scan,
            chosen_quad_table: None,
            changeset_version_range: Some(changeset.version_range()),
        }))
    }

    fn build_empty_scan(
        &self,
    ) -> Result<QuadPatternScanPlanningResult, DeltaQuadsStorageError> {
        let schema = compute_quad_pattern_schema(&self.pattern, &self.encoding);
        let final_schema = match &self.projection_indices {
            None => Arc::clone(schema.inner()),
            Some(projection) => Arc::new(schema.inner().project(projection)?),
        };
        Ok(QuadPatternScanPlanningResult {
            scan: Arc::new(EmptyExec::new(final_schema)),
            chosen_quad_table: None,
            changeset_version_range: None,
        })
    }

    /// Applies the (Base \ (Deletes U Adds)) U Adds logic entirely in the physical layer
    async fn apply_changeset_data_physical(
        &self,
        base_scan: Arc<dyn ExecutionPlan>,
        changeset: &DeltaQuadsStorageLogChangesetRef,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadsStorageError> {
        let filters = compute_quad_pattern_filters(&self.pattern, &self.encoding).await?;
        let context = ChangesetContext {
            intended_sort_order: self.quad_table.as_ref().map(|i| i.components()),
        };

        let quad_table = self
            .quad_table
            .as_ref()
            .expect("QuadTable must exist if base_scan is not empty");
        let components = quad_table.components().inner().to_vec();
        let mut current_plan = base_scan;

        // 1. Handle Cleared Graphs (LeftAnti Join on COL_GRAPH)
        if let Some(cleared_plan) = changeset
            .cleared_graphs(&context, &self.session_state)
            .await?
        {
            let left_schema = current_plan.schema();
            let right_schema = cleared_plan.schema();

            let l_idx = left_schema.index_of(COL_GRAPH)?;
            let r_idx = right_schema.index_of(COL_GRAPH)?;

            let join_on = vec![(
                Arc::new(PhysColumn::new(COL_GRAPH, r_idx)) as Arc<dyn PhysicalExpr>,
                Arc::new(PhysColumn::new(COL_GRAPH, l_idx)) as Arc<dyn PhysicalExpr>,
            )];

            current_plan = Arc::new(HashJoinExec::try_new(
                cleared_plan,
                current_plan,
                join_on,
                None,
                &JoinType::RightAnti,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNull,
                false,
            )?);
        }

        // Handle removed quads
        let removed_plan = changeset
            .removed_quads(&context, &self.session_state)
            .await?
            .map(|df| self.filter_and_project(df, None, &filters))
            .transpose()?;
        if let Some(removed_plan) = removed_plan {
            let left_schema = removed_plan.schema();
            let right_schema = current_plan.schema();
            let mut join_on = Vec::new();
            for component in &components {
                let name = component.column_name();
                let l_idx = left_schema.index_of(name)?;
                let r_idx = right_schema.index_of(name)?;
                join_on.push((
                    Arc::new(PhysColumn::new(name, l_idx)) as Arc<dyn PhysicalExpr>,
                    Arc::new(PhysColumn::new(name, r_idx)) as Arc<dyn PhysicalExpr>,
                ));
            }

            // The removed quads are the build side, such that the memory is bounded by the
            // changeset instead of the quad table.
            current_plan = Arc::new(HashJoinExec::try_new(
                removed_plan,
                current_plan,
                join_on,
                None,
                &JoinType::RightAnti,
                None,
                PartitionMode::CollectLeft,
                NullEquality::NullEqualsNull,
                false,
            )?);
        }

        // Handle added quads.
        let added_plan = changeset
            .added_quads(&context, &self.session_state)
            .await?
            .map(|df| self.filter_and_project(df, None, &filters))
            .transpose()?;
        if let Some(adds_plan) = added_plan {
            let mut sort_exprs = Vec::new();
            let schema = current_plan.schema();

            for component in &components {
                let name = component.column_name();
                let idx = schema.index_of(name)?;
                sort_exprs.push(PhysicalSortExpr {
                    expr: Arc::new(PhysColumn::new(name, idx)) as Arc<dyn PhysicalExpr>,
                    options: SortOptions {
                        descending: false,
                        nulls_first: true,
                    },
                });
            }
            let ordering =
                LexOrdering::new(sort_exprs).expect("LexOrdering should be valid");

            current_plan = Arc::new(SortExec::new(ordering.clone(), current_plan));
            let adds_plan = Arc::new(SortExec::new(ordering.clone(), adds_plan));
            let adds_and_quad_table = UnionExec::try_new(vec![current_plan, adds_plan])
                .expect("Input not empty");

            let adds_and_quad_table_single_partition = Arc::new(
                SortPreservingMergeExec::new(ordering.clone(), adds_and_quad_table),
            );

            current_plan = Arc::new(SortedDistinctExec::new(
                adds_and_quad_table_single_partition,
                LexRequirement::from(ordering.clone()),
            ));
        }

        Ok(current_plan)
    }

    async fn scan_quad_table_physical(
        &self,
        quad_table: &DeltaQuadsQuadTableSnapshot,
        projection: QuadTableScanProjectionPushdown,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError> {
        if quad_table.active_files().is_empty() {
            return Ok(None);
        }

        let pushdown_projection = match projection {
            QuadTableScanProjectionPushdown::No => PushdownProjection::No,
            QuadTableScanProjectionPushdown::Yes(projections) => {
                PushdownProjection::Yes(projections)
            }
        };

        let quad_tables = DeltaSwitchableQuadTables {
            quad_tables: self.switchable_quad_tables.clone(),
            encoding: self.encoding.clone(),
            cache: self.cache.clone(),
        };
        let mut builder = quad_tables.scan_builder(
            quad_table,
            &self.pattern,
            pushdown_projection.clone(),
            &self.session_state,
            self.session_state
                .config_options()
                .execution
                .target_partitions,
        )?;
        let planned_table = self
            .switchable_quad_tables
            .iter()
            .position(|table| table.components() == quad_table.components());
        if let Some(planned_table) = planned_table
            && self.switchable_quad_tables.len() > 1
        {
            builder = builder.with_quad_table_switch(QuadTableSwitch::new(
                Arc::new(quad_tables),
                planned_table,
                pushdown_projection,
            ));
        }

        Ok(Some(builder.build().await?))
    }

    /// Physically maps the output columns to their requested variable names
    /// and applies projection quad_tables if defined.
    fn project_components_to_variables_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        projection_indices: Option<&[usize]>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadsStorageError> {
        let schema = plan.schema();
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
        let exprs = ParquetQuadScanBuilder::compute_projection_exprs(
            &self.session_state,
            &self.pattern,
            &df_schema,
            projection_indices,
        )?;
        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }

    /// Helper to apply filtering and projection on an ExecutionPlan
    fn filter_and_project(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        projections: Option<&[usize]>,
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadsStorageError> {
        let mut current_plan = plan;

        if let Some(filter_expr) = conjunction(filters.iter().cloned()) {
            let schema = current_plan.schema();
            let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
            let phys_filter = create_physical_expr(
                &filter_expr,
                &df_schema,
                self.session_state.execution_props(),
                &PhysicalPlanningContext::default(),
            )?;
            current_plan = Arc::new(FilterExec::try_new(phys_filter, current_plan)?);
        }

        if let Some(projections) = projections {
            let schema = current_plan.schema();
            let mut exprs = Vec::with_capacity(projections.len());
            for &idx in projections {
                let field = schema.field(idx);
                let expr =
                    Arc::new(PhysColumn::new(field.name(), idx)) as Arc<dyn PhysicalExpr>;
                exprs.push((expr, field.name().to_string()));
            }
            Ok(Arc::new(ProjectionExec::try_new(exprs, current_plan)?))
        } else {
            Ok(current_plan)
        }
    }
}

/// Determines how the projection should be handled during the quad_table scan.
enum QuadTableScanProjectionPushdown {
    /// No projection pushdown
    No,
    /// Pushdown the projection with optional additional projection quad_tables
    Yes(Option<Vec<usize>>),
}

/// The quad tables of a Delta storage that a scan can switch between (see [`QuadTableSwitch`]).
///
/// All quad tables have the same log version.
#[derive(Debug)]
struct DeltaSwitchableQuadTables {
    quad_tables: Vec<DeltaQuadsQuadTableSnapshot>,
    encoding: QuadStorageEncoding,
    cache: Option<Arc<BlockCache>>,
}

impl DeltaSwitchableQuadTables {
    /// Creates the builder for the scan of the `pattern` from the `quad_table`. The files are
    /// distributed over (at most) `num_partitions` groups.
    fn scan_builder<'a>(
        &self,
        quad_table: &DeltaQuadsQuadTableSnapshot,
        pattern: &QuadPattern,
        projection: PushdownProjection,
        session_state: &'a SessionState,
        num_partitions: usize,
    ) -> Result<ParquetQuadScanBuilder<'a>, DeltaQuadsStorageError> {
        let table_uri = quad_table.log_store().config().location().clone();
        let table_path = object_store::path::Path::from(table_uri.path());

        let mut file_groups = vec![Vec::new(); num_partitions.max(1)];
        let num_groups = file_groups.len();
        for (i, file) in quad_table.active_files().iter().enumerate() {
            let partitioned_file =
                PartitionedFile::new_from_meta(add_to_object_meta(&table_path, file)?);
            file_groups[i % num_groups].push(partitioned_file);
        }
        file_groups.retain(|g| !g.is_empty());
        let file_groups = file_groups.into_iter().map(FileGroup::new).collect();

        Ok(ParquetQuadScanBuilder::new(
            session_state,
            self.encoding.clone(),
            table_uri.as_object_store_url(),
            file_groups,
        )
        .with_quad_pattern(pattern.clone())
        .with_pushdown_projection(projection)
        .with_block_cache(self.cache.clone())
        .with_eager_pruning(true))
    }
}

#[async_trait]
impl SwitchableQuadTables for DeltaSwitchableQuadTables {
    fn native_sort_orders(&self) -> Vec<Option<Vec<QuadComponent>>> {
        self.quad_tables
            .iter()
            .map(|table| Some(table.components().inner().to_vec()))
            .collect()
    }

    async fn build_scan(
        &self,
        idx: usize,
        pattern: &QuadPattern,
        projection: PushdownProjection,
        state: &SessionState,
        filter: Expr,
        num_partitions: usize,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.scan_builder(
            &self.quad_tables[idx],
            pattern,
            projection,
            state,
            num_partitions,
        )
        .map_err(|e| datafusion::error::DataFusionError::External(Box::new(e)))?
        .with_additional_filter(filter)
        .build()
        .await
    }
}
