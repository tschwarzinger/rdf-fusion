use crate::parquet::reader::PreLoadedMetadataReaderFactory;
use crate::parquet::scan::ParquetQuadScanExec;
use datafusion::common::plan_datafusion_err;
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchema, DFSchemaRef, Statistics};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, PagePruningAccessPlanFilter, ParquetAccessPlan,
    ParquetFileReaderFactory, RowGroupAccess, RowGroupAccessPlanFilter,
};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetFileMetrics, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::SessionState;
use datafusion::logical_expr::{Expr, utils::conjunction};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExprs;
use datafusion::physical_plan::projection::{ProjectionExec, ProjectionExpr};
use object_store::ObjectMeta;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::sync::Arc;

/// Determines how the projection should be handled during the parquet scan.
pub enum PushdownProjection {
    /// No projection pushdown. The scan returns the base schema.
    No,
    /// Pushdown the projection with optional additional projection indexes.
    Yes(Option<Vec<usize>>),
}

pub type EagerPruningResult =
    (ParquetAccessPlan, Option<Arc<dyn PhysicalExpr>>, Statistics);

use crate::parquet::reader::{PreloadedBloomFilters, PreloadedParquetMetadata};

/// Defines which [`ParquetFileReaderFactory`] should be used during scanning.
pub enum ParquetQuadScanReaderFactoryType {
    /// Uses the default DataFusion parquet reader
    Default,
    /// Create a cached reader that already knows the parquet metadata
    Preloaded(PreloadedParquetMetadata, PreloadedBloomFilters),
}

/// A builder for constructing `ParquetQuadScanExec` with optional predicate pushdown and projection.
pub struct ParquetQuadScanBuilder<'a> {
    session_state: &'a SessionState,
    encoding: QuadStorageEncoding,
    file_groups: Vec<FileGroup>,
    pattern: Option<QuadPattern>,
    object_store_url: ObjectStoreUrl,
    reader_factory_type: ParquetQuadScanReaderFactoryType,
    pushdown_projection: PushdownProjection,
    eager_pruning: bool,
}

impl<'a> ParquetQuadScanBuilder<'a> {
    pub fn new(
        session_state: &'a SessionState,
        encoding: QuadStorageEncoding,
        object_store_url: ObjectStoreUrl,
        file_groups: Vec<FileGroup>,
    ) -> Self {
        Self {
            session_state,
            encoding,
            file_groups,
            pattern: None,
            object_store_url,
            reader_factory_type: ParquetQuadScanReaderFactoryType::Default,
            pushdown_projection: PushdownProjection::No,
            eager_pruning: false,
        }
    }

    /// Sets which quad pattern should be matched against the parquet files. If [`None`], the entire
    /// quads table will be scanned.
    pub fn with_quad_pattern(mut self, pattern: QuadPattern) -> Self {
        self.pattern = Some(pattern);
        self
    }

    /// Defines how the [`ParquetFileReaderFactory`] should be constructed during the scan.
    pub fn with_reader_factory_type(
        mut self,
        reader_factory_type: ParquetQuadScanReaderFactoryType,
    ) -> Self {
        self.reader_factory_type = reader_factory_type;
        self
    }

    /// Defines whether Parquet data skipping should be done during planning or during planning and
    /// execution. Doing some work during planning can be used to provide better statistics to the
    /// query planner.
    pub fn with_eager_pruning(mut self, eager_pruning: bool) -> Self {
        self.eager_pruning = eager_pruning;
        self
    }

    /// Defines whether to project the quad table. Optionally, some variables of the quad pattern
    /// can also be projected away.
    pub fn with_pushdown_projection(
        mut self,
        pushdown_projection: PushdownProjection,
    ) -> Self {
        self.pushdown_projection = pushdown_projection;
        self
    }

    /// Builds the Parquet scan.
    pub fn build(self) -> DFResult<Arc<dyn ExecutionPlan>> {
        let base_schema = self.encoding.quad_schema();

        let combined_logical_filter = if let Some(pattern) = &self.pattern {
            conjunction(pattern.compute_filters(&self.encoding)?)
        } else {
            None
        };

        let file_source =
            self.build_file_source(combined_logical_filter.clone(), &base_schema)?;

        let (file_groups, statistics) =
            self.apply_eager_pruning(combined_logical_filter.clone())?;

        let mut file_scan_config =
            FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
                .with_file_groups(file_groups);
        if let Some(stats) = statistics {
            file_scan_config = file_scan_config.with_statistics(stats);
        }
        let data_source =
            Arc::new(DataSourceExec::new(Arc::new(file_scan_config.build())));

        let pattern = self.pattern.clone().unwrap_or_else(QuadPattern::all_quads);
        let scan = Arc::new(ParquetQuadScanExec::try_new(pattern.clone(), data_source)?);

        return if matches!(self.encoding, QuadStorageEncoding::PlainTerm) {
            wrap_in_filter_and_projection(
                self.session_state,
                self.pattern.as_ref(),
                &self.pushdown_projection,
                combined_logical_filter,
                scan,
            )
        } else {
            Ok(scan)
        };

        /// Wraps the given plan in a filter and projection (if applicable). This is used to
        /// implement the pattern matching on scans that do not support pushing down the filters
        /// and projections.
        fn wrap_in_filter_and_projection(
            session_state: &SessionState,
            pattern: Option<&QuadPattern>,
            pushdown_projection: &PushdownProjection,
            combined_logical_filter: Option<Expr>,
            mut plan: Arc<dyn ExecutionPlan>,
        ) -> DFResult<Arc<dyn ExecutionPlan>> {
            if let Some(filter) = combined_logical_filter {
                let schema = plan.schema();
                let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
                let phys_filter = create_physical_expr(
                    &filter,
                    &df_schema,
                    session_state.execution_props(),
                )?;
                plan = Arc::new(FilterExec::try_new(phys_filter, plan)?);
            }

            if let PushdownProjection::Yes(indices) = pushdown_projection {
                if let Some(pattern) = pattern {
                    let schema = plan.schema();
                    let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
                    let exprs = ParquetQuadScanBuilder::compute_projection_exprs(
                        session_state,
                        pattern,
                        &df_schema,
                        indices.as_deref(),
                    )?;
                    plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
                }
            }

            Ok(plan)
        }
    }

    /// Builds the [`FileSource`] that is used to implement the scan.
    fn build_file_source(
        &self,
        combined_logical_filter: Option<Expr>,
        base_schema: &DFSchemaRef,
    ) -> DFResult<Arc<dyn FileSource>> {
        let pushdown_filters = !matches!(self.encoding, QuadStorageEncoding::PlainTerm);
        let table_schema = TableSchema::new(Arc::clone(base_schema.inner()), vec![]);

        let store = self
            .session_state
            .runtime_env()
            .object_store(&self.object_store_url)?;
        let default_reader = Arc::new(DefaultParquetFileReaderFactory::new(store));
        let reader_factory: Arc<dyn ParquetFileReaderFactory> = match &self
            .reader_factory_type
        {
            ParquetQuadScanReaderFactoryType::Default => Arc::clone(&default_reader) as _,
            ParquetQuadScanReaderFactoryType::Preloaded(cache, bloom_filter_cache) => {
                Arc::new(PreLoadedMetadataReaderFactory::new(
                    default_reader,
                    cache.clone(),
                    bloom_filter_cache.clone(),
                ))
            }
        };

        let mut parquet_source = ParquetSource::new(table_schema)
            .with_pushdown_filters(pushdown_filters)
            .with_parquet_file_reader_factory(reader_factory);

        if let Some(filter) = combined_logical_filter {
            let predicate = self
                .session_state
                .create_physical_expr(filter, base_schema.as_ref())?;
            parquet_source = parquet_source.with_predicate(predicate);
        }

        match &self.pushdown_projection {
            PushdownProjection::No => Ok(Arc::new(parquet_source)),
            PushdownProjection::Yes(indices) => {
                if matches!(self.encoding, QuadStorageEncoding::PlainTerm) {
                    Ok(Arc::new(parquet_source))
                } else if let Some(pattern) = &self.pattern {
                    ParquetQuadScanBuilder::pushdown_projection_into_index_scan(
                        self.session_state,
                        pattern,
                        &mut parquet_source,
                        indices.as_deref(),
                        base_schema,
                    )
                } else {
                    Ok(Arc::new(parquet_source))
                }
            }
        }
    }

    /// Applies pruning eagerly, if configured and returns the resulting estimated statistics which
    /// can be used for planning.
    fn apply_eager_pruning(
        &self,
        combined_logical_filter: Option<Expr>,
    ) -> DFResult<(Vec<FileGroup>, Option<Statistics>)> {
        if !self.eager_pruning {
            return Ok((self.file_groups.clone(), None));
        }

        let cache = match &self.reader_factory_type {
            ParquetQuadScanReaderFactoryType::Default => {
                return Ok((self.file_groups.clone(), None));
            }
            ParquetQuadScanReaderFactoryType::Preloaded(cache, _) => cache,
        };

        let mut total_rows = 0;
        let mut all_exact = true;
        let mut some_pruned = false;
        let mut all_file_groups = Vec::new();

        for fg in self.file_groups.clone() {
            let mut new_files = Vec::with_capacity(fg.files().len());
            for mut pf in fg.into_inner() {
                if let Some((parquet_meta, object_meta)) =
                    cache.get(&pf.object_meta.location)
                {
                    let (access_plan, _, stats) =
                        ParquetQuadScanBuilder::compute_eager_pruning(
                            self.session_state,
                            &self.encoding,
                            parquet_meta.as_ref(),
                            &object_meta,
                            combined_logical_filter.clone(),
                        )?;

                    pf.extensions = Some(Arc::new(access_plan));
                    some_pruned = true;

                    match stats.num_rows {
                        Precision::Exact(n) => total_rows += n,
                        Precision::Inexact(n) => {
                            total_rows += n;
                            all_exact = false;
                        }
                        Precision::Absent => all_exact = false,
                    }
                }
                new_files.push(pf);
            }
            all_file_groups.push(FileGroup::new(new_files));
        }

        let overall_stats = if some_pruned {
            let precision = if all_exact {
                Precision::Exact(total_rows)
            } else {
                Precision::Inexact(total_rows)
            };
            Some(Statistics {
                num_rows: precision,
                total_byte_size: Precision::Absent,
                column_statistics: Statistics::unknown_column(
                    self.encoding.quad_schema().inner(),
                ),
            })
        } else {
            None
        };

        Ok((all_file_groups, overall_stats))
    }

    /// Computes the physical expressions and names for renaming quad components to variables,
    /// optionally selecting a subset of columns according to projection_indices.
    pub fn compute_projection_exprs(
        session_state: &SessionState,
        pattern: &QuadPattern,
        schema: &DFSchema,
        projection_indices: Option<&[usize]>,
    ) -> DFResult<Vec<(Arc<dyn PhysicalExpr>, String)>> {
        let full_projections = pattern
            .compute_projection()
            .into_iter()
            .map(|(logical_expr, name)| {
                let phys_expr = create_physical_expr(
                    &logical_expr,
                    schema,
                    session_state.execution_props(),
                )?;
                Ok((phys_expr, name))
            })
            .collect::<DFResult<Vec<_>>>()?;

        if let Some(indices) = projection_indices {
            let mut exprs = Vec::with_capacity(indices.len());
            for &idx in indices {
                let expr = full_projections.get(idx).ok_or_else(|| {
                    plan_datafusion_err!(
                        "Projection index {} out of bounds for schema length {}",
                        idx,
                        full_projections.len()
                    )
                })?;
                exprs.push(expr.clone());
            }
            Ok(exprs)
        } else {
            Ok(full_projections)
        }
    }

    /// Ensures that the entire projection (skipping columns and renaming) is pushed down into the
    /// scan.
    pub fn pushdown_projection_into_index_scan(
        session_state: &SessionState,
        pattern: &QuadPattern,
        parquet_source: &mut ParquetSource,
        additional_projection_indices: Option<&[usize]>,
        schema: &DFSchemaRef,
    ) -> DFResult<Arc<dyn FileSource>> {
        let exprs = Self::compute_projection_exprs(
            session_state,
            pattern,
            schema.as_ref(),
            additional_projection_indices,
        )?;
        let projections = ProjectionExprs::new(
            exprs
                .into_iter()
                .map(|(expr, name)| ProjectionExpr::new(expr, name)),
        );

        match parquet_source.try_pushdown_projection(&projections)? {
            None => Err(plan_datafusion_err!(
                "Cannot pushdown projection into parquet source."
            )),
            Some(pushed_source) => Ok(pushed_source),
        }
    }

    /// Evaluates eager pruning logic to construct a `ParquetAccessPlan` and statistics.
    pub fn compute_eager_pruning(
        session_state: &SessionState,
        encoding: &QuadStorageEncoding,
        parquet_meta: &ParquetMetaData,
        object_meta: &ObjectMeta,
        combined_logical_filter: Option<Expr>,
    ) -> DFResult<EagerPruningResult> {
        let base_schema = encoding.quad_schema();
        let num_row_groups = parquet_meta.num_row_groups();
        let access_plan = ParquetAccessPlan::new_all(num_row_groups);

        let (access_plan, physical_filter_expr) = if let Some(logical_expr) =
            combined_logical_filter
        {
            let phys_expr = create_physical_expr(
                &logical_expr,
                base_schema.as_ref(),
                session_state.execution_props(),
            )?;

            let predicate = PruningPredicate::try_new(
                Arc::clone(&phys_expr),
                Arc::clone(base_schema.inner()),
            )?;

            let mut rg_filter = RowGroupAccessPlanFilter::new(access_plan);
            let metrics_set = ExecutionPlanMetricsSet::new();
            let metrics =
                ParquetFileMetrics::new(0, object_meta.location.as_ref(), &metrics_set);

            rg_filter.prune_by_statistics(
                base_schema.inner().as_ref(),
                parquet_meta.file_metadata().schema_descr(),
                parquet_meta.row_groups(),
                &predicate,
                &metrics,
            );

            let access_plan = rg_filter.build();

            // Page Index Pruning
            let page_filter = PagePruningAccessPlanFilter::new(
                &phys_expr,
                Arc::clone(base_schema.inner()),
            );
            let access_plan = page_filter.prune_plan_with_page_index(
                access_plan,
                base_schema.inner().as_ref(),
                parquet_meta.file_metadata().schema_descr(),
                parquet_meta,
                &metrics,
            );

            (access_plan, Some(phys_expr))
        } else {
            (access_plan, None)
        };

        // Determine if there are matching rows.
        let mut row_count = 0;
        let mut has_matching_row_group = false;

        for (i, rg) in parquet_meta.row_groups().iter().enumerate() {
            if access_plan.inner()[i] != RowGroupAccess::Skip {
                row_count += rg.num_rows();
                has_matching_row_group = true;
            }
        }

        let statistics = if has_matching_row_group {
            Statistics {
                num_rows: Precision::Inexact(row_count as usize),
                total_byte_size: Precision::Absent,
                column_statistics: Statistics::unknown_column(base_schema.inner()),
            }
        } else {
            Statistics {
                num_rows: Precision::Exact(0),
                total_byte_size: Precision::Absent,
                column_statistics: Statistics::unknown_column(base_schema.inner()),
            }
        };

        Ok((access_plan, physical_filter_expr, statistics))
    }
}
