use crate::delta::error::DeltaQuadStorageError;
use crate::delta::index::{DeltaStorageQuadIndex, DeltaStorageQuadIndexSnapshot};
use crate::delta::log::changeset::DeltaStorageLogChangesetRef;
use crate::delta::log::{DeltaStorageLog, DeltaStorageLogVersionRange};
use std::collections::HashSet;

use crate::index::IndexComponents;
use datafusion::catalog::Session;
use datafusion::common::{DFSchema, JoinType, NullEquality};
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionState;
use datafusion::logical_expr::Expr;
use datafusion::logical_expr::utils::conjunction;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::expressions::{Column as PhysColumn, Column};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::enforce_distribution::EnforceDistribution;
use datafusion::physical_optimizer::enforce_sorting::EnforceSorting;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
use deltalake::kernel::Add;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use rdf_fusion_model::quads::COL_GRAPH;
use std::sync::Arc;

pub struct QuadPatternScanPlanningResult {
    pub scan: Arc<dyn ExecutionPlan>, // Swapped to Physical Plan
    pub chosen_index: Option<IndexComponents>,
    pub changeset_version_range: Option<DeltaStorageLogVersionRange>,
}

/// A builder for constructing scan plans from an optional index and an optional manual changeset.
pub struct DeltaQuadStorageScanPlanBuilder {
    session_state: SessionState,
    pattern: QuadPattern,
    encoding: QuadStorageEncoding,
    index: Option<Arc<DeltaStorageQuadIndexSnapshot>>,
    changeset: Option<DeltaStorageLogChangesetRef>,
}

impl DeltaQuadStorageScanPlanBuilder {
    pub fn new(
        session_state: SessionState,
        pattern: QuadPattern,
        encoding: QuadStorageEncoding,
    ) -> Self {
        Self {
            session_state,
            pattern,
            encoding,
            index: None,
            changeset: None,
        }
    }

    pub async fn with_best_index(
        self,
        indexes: &[Arc<DeltaStorageQuadIndex>],
    ) -> Result<Self, DeltaQuadStorageError> {
        let best_index = indexes
            .iter()
            .max_by_key(|idx| {
                idx.compute_scan_score(
                    &self.pattern.active_graph,
                    &self.pattern.triple_pattern,
                    self.pattern.blank_node_mode,
                )
            })
            .cloned();
        match best_index {
            None => Ok(self),
            Some(idx) => Ok(self.with_index(idx.snapshot().await?)),
        }
    }

    pub fn with_index(mut self, index: Arc<DeltaStorageQuadIndexSnapshot>) -> Self {
        self.index = Some(index);
        self
    }

    pub async fn with_changeset_for_log(
        self,
        log: &DeltaStorageLog,
        target_version: Option<u64>,
    ) -> Result<Self, DeltaQuadStorageError> {
        let target_version = match target_version {
            None => log.version().await,
            Some(target_version) => target_version,
        };
        let index_version = match self.index.as_ref() {
            None => 0,
            Some(index) => index.log_transaction_version(),
        };

        if target_version < index_version {
            return Err(DeltaQuadStorageError::VersionError(
                "The target version is older than the index version".to_string(),
            ));
        }

        match DeltaStorageLogVersionRange::try_new(index_version, target_version) {
            None => Ok(self),
            Some(version_range) => {
                let changeset = log
                    .compute_changeset(&self.session_state, version_range)
                    .await?;
                Ok(self.with_changeset(changeset))
            }
        }
    }

    pub fn with_changeset(mut self, changeset: DeltaStorageLogChangesetRef) -> Self {
        self.changeset = Some(changeset);
        self
    }

    pub async fn build(
        self,
    ) -> Result<QuadPatternScanPlanningResult, DeltaQuadStorageError> {
        let projection = compute_projection_indices(&self.pattern);
        let filters = self.pattern.compute_filters(&self.encoding)?;

        let initial_plan = match (&self.index, &self.changeset) {
            (Some(index), Some(changeset)) => {
                let base_scan = self
                    .scan_index_physical(index, &projection, &filters)
                    .await?;
                let applied_scan = self
                    .apply_changeset_data_physical(
                        base_scan,
                        changeset,
                        &projection,
                        &filters,
                    )
                    .await?;

                Ok(QuadPatternScanPlanningResult {
                    scan: self.rename_components_to_variables_physical(applied_scan)?,
                    chosen_index: Some(index.components()),
                    changeset_version_range: Some(changeset.version_range()),
                })
            }

            (Some(index), None) => {
                let base_scan = self
                    .scan_index_physical(index, &projection, &filters)
                    .await?;

                Ok(QuadPatternScanPlanningResult {
                    scan: self.rename_components_to_variables_physical(base_scan)?,
                    chosen_index: Some(index.components()),
                    changeset_version_range: None,
                })
            }

            (None, Some(changeset)) => {
                self.build_changeset_only_physical(changeset, &projection, &filters)
                    .await
            }

            (None, None) => self.build_empty_scan_physical(),
        }?;

        let rules = [
            Arc::new(EnforceDistribution::new()) as Arc<dyn PhysicalOptimizerRule>,
            Arc::new(EnforceSorting::new()) as Arc<dyn PhysicalOptimizerRule>,
        ];
        let config = self.session_state.config_options();

        let mut rewritten_plan = initial_plan.scan;
        for rule in rules {
            rewritten_plan = rule.optimize(rewritten_plan, config)?;
        }

        Ok(QuadPatternScanPlanningResult {
            scan: rewritten_plan,
            chosen_index: initial_plan.chosen_index,
            changeset_version_range: initial_plan.changeset_version_range,
        })
    }

    async fn build_changeset_only_physical(
        &self,
        changeset: &DeltaStorageLogChangesetRef,
        projection: &[usize],
        filters: &[Expr],
    ) -> Result<QuadPatternScanPlanningResult, DeltaQuadStorageError> {
        let Some(adds) = changeset.added_quads(&self.session_state).await? else {
            return self.build_empty_scan_physical();
        };

        let physical_plan = self.filter_and_project(adds, projection, filters)?;

        Ok(QuadPatternScanPlanningResult {
            scan: self.rename_components_to_variables_physical(physical_plan)?,
            chosen_index: None,
            changeset_version_range: Some(changeset.version_range()),
        })
    }

    fn build_empty_scan_physical(
        &self,
    ) -> Result<QuadPatternScanPlanningResult, DeltaQuadStorageError> {
        let schema = self.pattern.compute_schema(&self.encoding);
        let empty_exec = Arc::new(EmptyExec::new(Arc::clone(schema.inner())));

        Ok(QuadPatternScanPlanningResult {
            scan: empty_exec,
            chosen_index: None,
            changeset_version_range: None,
        })
    }

    /// Applies the (Base \ (Deletes U Adds)) U Adds logic entirely in the physical layer
    async fn apply_changeset_data_physical(
        &self,
        base_scan: Arc<dyn ExecutionPlan>,
        changeset: &DeltaStorageLogChangesetRef,
        projection: &[usize],
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let mut current_plan = base_scan;

        // 1. Handle Cleared Graphs (LeftAnti Join on COL_GRAPH)
        if let Some(cleared_plan) = changeset.cleared_graphs(&self.session_state).await? {
            let left_schema = current_plan.schema();
            let right_schema = cleared_plan.schema();

            // TODO: If the column is bound to a fixes value, we need to filter that.
            let l_idx = left_schema.index_of(COL_GRAPH).expect("Scan has column");
            let r_idx = right_schema.index_of(COL_GRAPH).expect("Scan has column");

            let join_on = vec![(
                Arc::new(PhysColumn::new(COL_GRAPH, l_idx)) as Arc<dyn PhysicalExpr>,
                Arc::new(PhysColumn::new(COL_GRAPH, r_idx)) as Arc<dyn PhysicalExpr>,
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

        // 2. Extract and format Logical DataFrames for Removed and Added
        let removed_plan = changeset
            .removed_quads(&self.session_state)
            .await?
            .map(|df| self.filter_and_project(df, projection, filters))
            .transpose()?;

        let added_plan = changeset
            .added_quads(&self.session_state)
            .await?
            .map(|df| self.filter_and_project(df, projection, filters))
            .transpose()?;

        // 3. Phase 1: The Masking Anti-Join -> (Base \ (Removed U Added))
        let masking_df = match (removed_plan, added_plan.clone()) {
            (Some(removed_plan), Some(added_plan)) => {
                Some(UnionExec::try_new(vec![removed_plan, added_plan])?)
            }
            (Some(r), None) => Some(r),
            (None, Some(a)) => Some(a),
            (None, None) => None,
        };

        if let Some(mask_plan) = masking_df {
            let left_schema = current_plan.schema();
            let right_schema = mask_plan.schema();
            let mut join_on = Vec::new();

            // Match on all projected columns dynamically
            for field in left_schema.fields() {
                let name = field.name();
                let l_idx = left_schema.index_of(name).expect("Schema known");
                let r_idx = right_schema.index_of(name).expect("Schema known");
                join_on.push((
                    Arc::new(PhysColumn::new(name, l_idx)) as Arc<dyn PhysicalExpr>,
                    Arc::new(PhysColumn::new(name, r_idx)) as Arc<dyn PhysicalExpr>,
                ));
            }

            current_plan = Arc::new(HashJoinExec::try_new(
                mask_plan,
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

        // 4. Phase 2: Append -> ... U Added
        if let Some(adds_plan) = added_plan {
            current_plan = UnionExec::try_new(vec![current_plan, adds_plan])
                .expect("Input not empty");
        }

        Ok(current_plan)
    }

    async fn scan_index_physical(
        &self,
        index: &DeltaStorageQuadIndexSnapshot,
        projections: &[usize],
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let relevant_files = prune_cached_files(
            &self.encoding,
            &self.session_state,
            index.active_files().as_ref(),
            filters,
            index,
        )?;

        let scan_config = DeltaScanConfig::new_from_session(&self.session_state);
        let table_provider = DeltaTableProvider::try_new(
            index.snapshot().clone(),
            Arc::clone(index.log_store()),
            scan_config,
        )?
        .with_files(relevant_files);

        // This is a bit of a hack. But if parquet pushdown filters are enabled, the filters will be
        // processed exactly by the scan. While the physical optimizer discovers this circumstance
        // and will remove the FilterExec, the projection pushdown is not yet implemented for Delta.
        // If the projection pushdown works for Delta, we can remove this hack and use
        // supports_filters_pushdown() to determine if the filters are pushed down exactly.
        let can_push_filters = match &self.encoding {
            QuadStorageEncoding::ObjectId(_) | QuadStorageEncoding::String => true,
            QuadStorageEncoding::PlainTerm => false,
        };
        let assume_filters_exact = self
            .session_state
            .config_options()
            .execution
            .parquet
            .pushdown_filters;

        let initial_scan = match (can_push_filters, assume_filters_exact) {
            (false, _) => {
                // If filters cannot be pushed down, we make a full scan.
                table_provider
                    .scan(&self.session_state, None, &[], None)
                    .await?
            }
            (true, true) => {
                table_provider
                    .scan(
                        &self.session_state,
                        Some(&projections.to_vec()),
                        filters,
                        None,
                    )
                    .await?
            }
            (true, false) => {
                // If filters are not exact, the projection cannot be pushed down as some filters may
                // require them.
                table_provider
                    .scan(&self.session_state, None, filters, None)
                    .await?
            }
        };

        return if can_push_filters && assume_filters_exact {
            // Projections and filters are pushed down to the DeltaTableProvider.
            Ok(initial_scan)
        } else {
            let schema = self.encoding.quad_schema();

            let filtered = if let Some(filter) = conjunction(filters.iter().cloned()) {
                let predicate = self
                    .session_state
                    .create_physical_expr(filter, schema.as_ref())?;
                Arc::new(FilterExec::try_new(predicate, initial_scan)?)
                    as Arc<dyn ExecutionPlan>
            } else {
                initial_scan
            };

            let projections = projections.iter().map(|&idx| {
                let field = schema.field(idx);
                ProjectionExpr::new(
                    Arc::new(Column::new(field.name(), idx)),
                    field.name(),
                )
            });
            let projected = Arc::new(ProjectionExec::try_new(projections, filtered)?)
                as Arc<dyn ExecutionPlan>;

            Ok(projected)
        };

        /// A basic manual pruner if you want to filter the Vec<Add> before creating the scan
        fn prune_cached_files(
            encoding: &QuadStorageEncoding,
            session: &dyn Session,
            files: &[Add],
            filters: &[Expr],
            index: &DeltaStorageQuadIndexSnapshot,
        ) -> Result<Vec<Add>, DeltaQuadStorageError> {
            // The pruning predicate currently does not handle structs / nested data structures.
            if matches!(encoding, QuadStorageEncoding::PlainTerm) {
                return Ok(files.to_vec());
            }

            let Some(filters) = conjunction(filters.iter().cloned()) else {
                return Ok(files.to_vec());
            };

            let schema = encoding.quad_schema();
            let predicate = session.create_physical_expr(filters, schema.as_ref())?;
            let pruning_predicate =
                PruningPredicate::try_new(predicate, Arc::clone(schema.inner()))?;
            let mask = pruning_predicate.prune(index.snapshot())?;

            assert_eq!(
                files.len(),
                mask.len(),
                "Files and mask should have equal length"
            );

            let result = files
                .iter()
                .zip(mask.iter())
                .flat_map(|(file, mask)| mask.then(|| file.clone()))
                .collect();
            Ok(result)
        }
    }

    /// Physically maps the output columns to their requested variable names
    fn rename_components_to_variables_physical(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let mut seen = HashSet::new();
        let mut exprs = Vec::new();
        let schema = plan.schema();

        // Convert the Arrow schema to a DataFusion DFSchema for expression parsing
        let df_schema = DFSchema::try_from(schema.as_ref().clone())?;

        for (logical_expr, name) in self.pattern.compute_projection() {
            if seen.contains(&name) {
                continue;
            }
            seen.insert(name.clone());

            let phys_expr = create_physical_expr(
                &logical_expr,
                &df_schema,
                self.session_state.execution_props(),
            )?;

            exprs.push((phys_expr, name));
        }

        Ok(Arc::new(ProjectionExec::try_new(exprs, plan)?))
    }

    /// Helper to apply filtering and projection on an ExecutionPlan
    fn filter_and_project(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        projections: &[usize],
        filters: &[Expr],
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let mut current_plan = plan;

        if let Some(filter_expr) = conjunction(filters.iter().cloned()) {
            let schema = current_plan.schema();
            let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
            let phys_filter = create_physical_expr(
                &filter_expr,
                &df_schema,
                self.session_state.execution_props(),
            )?;
            current_plan = Arc::new(FilterExec::try_new(phys_filter, current_plan)?);
        }

        let schema = current_plan.schema();
        let mut exprs = Vec::with_capacity(projections.len());
        for &idx in projections {
            let field = schema.field(idx);
            let expr =
                Arc::new(PhysColumn::new(field.name(), idx)) as Arc<dyn PhysicalExpr>;
            exprs.push((expr, field.name().to_string()));
        }

        Ok(Arc::new(ProjectionExec::try_new(exprs, current_plan)?))
    }
}

fn compute_projection_indices(pattern: &QuadPattern) -> Vec<usize> {
    let projection_indexes = pattern
        .compute_projected_components()
        .into_iter()
        .map(|(c, _)| c.gspo_index())
        .collect::<Vec<_>>();

    if projection_indexes.is_empty() {
        vec![0]
    } else {
        projection_indexes
    }
}
