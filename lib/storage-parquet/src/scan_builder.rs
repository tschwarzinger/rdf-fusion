use crate::bytes_estimator::estimate_scan_statistics;
use crate::counting_store::ByteCountingObjectStore;
use crate::reader::{
    BlockCachedReaderFactory, ParquetMetadataFetcher, max_concurrent_files,
};
use crate::scan::{BYTES_SCANNED_PLANNING, ParquetQuadScanExec};
use crate::switch::QuadTableSwitch;
use datafusion::arrow::array::{ArrayRef, BooleanArray, UInt64Array};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::ScalarValue;
use datafusion::common::plan_datafusion_err;
use datafusion::common::pruning::PruningStatistics;
use datafusion::common::stats::Precision;
use datafusion::common::{
    Column, ColumnStatistics, DFSchema, DFSchemaRef, ExprSchema, Statistics,
};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::{
    BloomFilterStatistics, PagePruningAccessPlanFilter, ParquetAccessPlan,
    RowGroupAccessPlanFilter, can_expr_be_pushed_down_with_schemas,
};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetFileMetrics,
    ParquetFileReaderFactory, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchemaBuilder;
use datafusion::error::DataFusionError;
use datafusion::execution::SessionState;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::expr::{BinaryExpr, InList};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::{conjunction, split_conjunction_owned};
use datafusion::logical_expr::{Expr, Operator, lit};
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{ParquetRecordBatchStreamBuilder, parquet_column};
use datafusion::parquet::basic::Type as PhysicalType;
use datafusion::parquet::file::metadata::{PageIndexPolicy, ParquetMetaData};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_optimizer::pruning::{
    PruningPredicate, PruningPredicateBuilder,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::metrics::{
    Count, MetricBuilder, MetricCategory, MetricType,
};
use datafusion::physical_plan::projection::{
    ProjectionExec, ProjectionExpr, ProjectionExprs,
};
use datafusion_datasource::compute_all_files_statistics;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use rdf_fusion_common::DFResult;
use rdf_fusion_common::config::{PlanningBloomFilters, RdfFusionSessionConfigExt};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::plain_term::{
    PlainTermEncoding, PlainTermScalar, PlainTermType,
};
use rdf_fusion_logical::quad_pattern::{QuadPattern, compute_quad_pattern_filters};
use std::collections::HashSet;
use std::sync::Arc;
use tracing::debug;

/// Determines how the projection should be handled during the parquet scan.
#[derive(Debug, Clone)]
pub enum PushdownProjection {
    /// No projection pushdown. The scan returns the base schema.
    No,
    /// Pushdown the projection with optional additional projection quad_tables.
    Yes(Option<Vec<usize>>),
}

use rdf_fusion_storage::block_cache::BlockCache;

/// A builder for constructing `ParquetQuadScanExec` with optional predicate pushdown and projection.
pub struct ParquetQuadScanBuilder<'a> {
    session_state: &'a SessionState,
    encoding: QuadStorageEncoding,
    file_groups: Vec<FileGroup>,
    pattern: Option<QuadPattern>,
    object_store_url: ObjectStoreUrl,
    block_cache: Option<Arc<BlockCache>>,
    pushdown_projection: PushdownProjection,
    eager_pruning: bool,
    split_file_groups: Option<usize>,
    additional_filter: Option<Expr>,
    quad_table_switch: Option<QuadTableSwitch>,
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
            block_cache: None,
            pushdown_projection: PushdownProjection::No,
            eager_pruning: false,
            split_file_groups: None,
            additional_filter: None,
            quad_table_switch: None,
        }
    }

    /// Sets which quad pattern should be matched against the parquet files. If [`None`], the entire
    /// quads table will be scanned.
    pub fn with_quad_pattern(mut self, pattern: QuadPattern) -> Self {
        self.pattern = Some(pattern);
        self
    }

    /// Caches the byte ranges read from the Parquet files (including Bloom filters) in the given
    /// block cache.
    pub fn with_block_cache(mut self, block_cache: Option<Arc<BlockCache>>) -> Self {
        self.block_cache = block_cache;
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

    /// After eager pruning, redistributes the remaining files into (at most) `n` file groups. This
    /// ensures that the files that survive pruning are spread across the partitions.
    pub fn with_split_file_groups(mut self, n: usize) -> Self {
        self.split_file_groups = Some(n);
        self
    }

    /// Applies the `filter` (over the quad table) in addition to the filters of the quad pattern.
    /// Like the filters of the pattern, it is used for eager pruning and pushed into the scan.
    pub fn with_additional_filter(mut self, filter: Expr) -> Self {
        self.additional_filter = Some(filter);
        self
    }

    /// Allows the scan to switch to another quad table at execution time. This is ignored if the
    /// scan cannot evaluate the entire filter of the pattern.
    pub fn with_quad_table_switch(mut self, switch: QuadTableSwitch) -> Self {
        self.quad_table_switch = Some(switch);
        self
    }

    /// Builds the Parquet scan.
    pub async fn build(self) -> DFResult<Arc<dyn ExecutionPlan>> {
        let base_schema = self.encoding.quad_schema();

        // The original, logical filter over the quad table.
        let mut filters = match &self.pattern {
            Some(pattern) => {
                compute_quad_pattern_filters(pattern, &self.encoding).await?
            }
            None => Vec::new(),
        };
        filters.extend(self.additional_filter.clone());
        let combined_logical_filter = conjunction(filters);

        // The predicate pushed into the parquet source. For PlainTerm, equalities and graph IN
        // lists are rewritten into leaf-field comparisons so the source can use them for pruning
        // and row filtering.
        let pushed_filter = match (&self.encoding, combined_logical_filter.clone()) {
            (QuadStorageEncoding::PlainTerm, Some(filter)) => {
                Some(rewrite_plain_term_predicates(filter, base_schema.as_ref())?)
            }
            (_, other) => other,
        };
        // Conjuncts that the statistics prove for all rows (e.g., `graph IS NULL` for files without
        // named graphs) are not pushed down. Otherwise, the Parquet source would load the page
        // index of every file for a predicate that cannot prune anything.
        let pushed_filter = pushed_filter.and_then(|filter| {
            remove_conjuncts_proven_by_statistics(
                filter,
                base_schema.inner(),
                &self.file_groups,
            )
        });

        // Determine whether every part of the pushed predicate can be evaluated by the parquet
        // decoder. If not, the source silently drops those conjuncts, so we re-apply the full
        // filter (and projection) in a FilterExec/ProjectionExec above the scan instead.
        let full_filter_is_pushable = match &pushed_filter {
            Some(filter) => {
                let physical = create_physical_expr(
                    filter,
                    base_schema.as_ref(),
                    self.session_state.execution_props(),
                    &PhysicalPlanningContext::default(),
                )?;
                can_expr_be_pushed_down_with_schemas(
                    &physical,
                    base_schema.inner().as_ref(),
                )
            }
            None => true,
        };

        let file_source = self.build_file_source(
            pushed_filter.clone(),
            &base_schema,
            full_filter_is_pushable,
        )?;

        // Bytes read while planning (e.g., for eager pruning) are not part of the inner scan's
        // `bytes_scanned`, so they are reported separately on the scan node.
        let planning_metrics = ExecutionPlanMetricsSet::new();
        let bytes_scanned_planning = MetricBuilder::new(&planning_metrics)
            .with_type(MetricType::Summary)
            .with_category(MetricCategory::Bytes)
            .global_counter(BYTES_SCANNED_PLANNING);

        let (file_groups, statistics) = self
            .apply_eager_pruning(pushed_filter.clone(), &bytes_scanned_planning)
            .await?;

        let mut file_scan_config =
            FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
                .with_file_groups(file_groups);
        if let Some(stats) = statistics {
            file_scan_config = file_scan_config.with_statistics(stats);
        }
        let data_source =
            Arc::new(DataSourceExec::new(Arc::new(file_scan_config.build())));

        let pattern = self.pattern.clone().unwrap_or_else(QuadPattern::all_quads);
        // The switch builds the alternative scan like this scan. If the filter is applied above the
        // scan, the alternative scan would have to be wrapped as well.
        let quad_table_switch = self
            .quad_table_switch
            .clone()
            .filter(|_| full_filter_is_pushable);
        let scan = Arc::new(ParquetQuadScanExec::try_new(
            pattern.clone(),
            data_source,
            planning_metrics,
            quad_table_switch,
        )?);

        if full_filter_is_pushable {
            Ok(scan)
        } else {
            // Re-apply the full filter (and projection) above the scan, since the parquet source
            // can only evaluate a subset of the predicates.
            Self::wrap_in_filter_and_projection(
                self.session_state,
                self.pattern.as_ref(),
                &self.pushdown_projection,
                combined_logical_filter,
                scan,
            )
        }
    }

    /// Wraps the given plan in a filter and projection (if applicable). This is used to implement
    /// the parts of pattern matching that are not pushed down into the parquet scan and for
    /// projections that are not pushed into the scan.
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
                &PhysicalPlanningContext::default(),
            )?;
            plan = Arc::new(FilterExec::try_new(phys_filter, plan)?);
        }

        if let PushdownProjection::Yes(quad_tables) = pushdown_projection
            && let Some(pattern) = pattern
        {
            let schema = plan.schema();
            let df_schema = DFSchema::try_from(schema.as_ref().clone())?;
            let exprs = ParquetQuadScanBuilder::compute_projection_exprs(
                session_state,
                pattern,
                &df_schema,
                quad_tables.as_deref(),
            )?;
            plan = Arc::new(ProjectionExec::try_new(exprs, plan)?);
        }

        Ok(plan)
    }

    /// Returns the object store that is used for the scan.
    fn object_store(&self) -> DFResult<Arc<dyn ObjectStore>> {
        self.session_state
            .runtime_env()
            .object_store(&self.object_store_url)
    }

    /// Returns the factory for the readers of the Parquet files that read from `store`. The
    /// metadata is served from the session's file metadata cache and the byte reads are cached in
    /// the block cache.
    fn reader_factory(&self, store: Arc<dyn ObjectStore>) -> BlockCachedReaderFactory {
        let metadata_cache = self
            .session_state
            .runtime_env()
            .cache_manager
            .get_file_metadata_cache();
        BlockCachedReaderFactory::new(store, metadata_cache, self.block_cache.clone())
    }

    /// Returns the metadata size hint from the session's Parquet options.
    fn metadata_size_hint(&self) -> Option<usize> {
        self.session_state
            .table_options()
            .parquet
            .global
            .metadata_size_hint
    }

    /// Builds the [`FileSource`] that is used to implement the scan.
    ///
    /// This mirrors what DataFusion's `ParquetFormat::create_physical_plan` does for a
    /// `ListingTable` (session Parquet options, metadata size hint, cached metadata reader) and
    /// additionally configures our predicate, projection, and block cache.
    fn build_file_source(
        &self,
        combined_logical_filter: Option<Expr>,
        base_schema: &DFSchemaRef,
        full_filter_is_pushable: bool,
    ) -> DFResult<Arc<dyn FileSource>> {
        let table_schema =
            TableSchemaBuilder::new(Arc::clone(base_schema.inner())).build();

        let reader_factory = Arc::new(self.reader_factory(self.object_store()?));

        let mut parquet_source = ParquetSource::new(table_schema)
            .with_table_parquet_options(
                self.session_state.table_options().parquet.clone(),
            )
            .with_pushdown_filters(true)
            .with_parquet_file_reader_factory(reader_factory);
        if let Some(metadata_size_hint) = self.metadata_size_hint() {
            parquet_source = parquet_source.with_metadata_size_hint(metadata_size_hint);
        }

        if let Some(filter) = combined_logical_filter {
            let predicate = self
                .session_state
                .create_physical_expr(filter, base_schema.as_ref())?;
            parquet_source = parquet_source.with_predicate(predicate);
        }

        match &self.pushdown_projection {
            PushdownProjection::No => Ok(Arc::new(parquet_source)),
            PushdownProjection::Yes(quad_tables) => {
                if !full_filter_is_pushable {
                    // The scan is wrapped in a filter/projection above, so the projection is not
                    // pushed into the parquet source.
                    Ok(Arc::new(parquet_source))
                } else if let Some(pattern) = &self.pattern {
                    ParquetQuadScanBuilder::pushdown_projection_into_index_scan(
                        self.session_state,
                        pattern,
                        &mut parquet_source,
                        quad_tables.as_deref(),
                        base_schema,
                    )
                } else {
                    Ok(Arc::new(parquet_source))
                }
            }
        }
    }

    /// Applies pruning eagerly, if configured, and returns the pruned file groups and the
    /// resulting estimated statistics which can be used for planning.
    ///
    /// The pruning happens in three phases:
    /// 1. The row groups of each file are pruned using the statistics in the footer. Files for
    ///    which no row group remains are removed from the scan.
    /// 2. The remaining row groups are pruned using Bloom filters, if the statistics are unlikely
    ///    to be effective for some bound columns (see [`bloom_filter_columns`]). Whether this is
    ///    done is controlled by [`PlanningBloomFilters`].
    /// 3. The page index of the remaining files is loaded and used for pruning pages.
    ///
    /// Hence, the Bloom filters and the (comparatively large) page index are only loaded for files
    /// that may contain matching rows. The metadata is obtained from DataFusion's file metadata
    /// cache, while the Bloom filters are cached in the block cache.
    ///
    /// The bytes read from the object store (i.e., not served from a cache) are recorded in
    /// `bytes_scanned`.
    async fn apply_eager_pruning(
        &self,
        combined_logical_filter: Option<Expr>,
        bytes_scanned: &Count,
    ) -> DFResult<(Vec<FileGroup>, Option<Statistics>)> {
        if !self.eager_pruning {
            return Ok((self.file_groups.clone(), None));
        }

        let store: Arc<dyn ObjectStore> = Arc::new(ByteCountingObjectStore::new(
            self.object_store()?,
            bytes_scanned.clone(),
        ));
        let fetcher = ParquetMetadataFetcher::new(self.session_state, Arc::clone(&store));
        let reader_factory = self.reader_factory(store);
        let filters = self.create_pruning_filters(combined_logical_filter)?;
        let (fetcher, reader_factory, filters) = (&fetcher, &reader_factory, &filters);

        // Up to `max_concurrent_files` files are pruned concurrently. Afterward, the remaining files
        // are put back into their original groups.
        let files = self
            .file_groups
            .iter()
            .enumerate()
            .flat_map(|(group, fg)| fg.iter().map(move |file| (group, file.clone())))
            .collect::<Vec<_>>();
        let pruned: Vec<(usize, Option<PartitionedFile>)> = futures::stream::iter(files)
            .map(|(group, file)| async move {
                let pruned = self
                    .prune_file(fetcher, reader_factory, filters, file)
                    .await?;
                Ok::<_, DataFusionError>((group, pruned))
            })
            .buffered(max_concurrent_files(self.session_state))
            .try_collect()
            .await?;

        let total_files = pruned.len();
        let mut groups = vec![Vec::new(); self.file_groups.len()];
        for (group, file) in pruned {
            groups[group].extend(file);
        }
        let mut all_file_groups = groups
            .into_iter()
            .filter(|files| !files.is_empty())
            .map(FileGroup::new)
            .collect::<Vec<_>>();
        let remaining_files = all_file_groups.iter().map(FileGroup::len).sum::<usize>();
        debug!(
            "Eager pruning removed {}/{total_files} files from the scan.",
            total_files - remaining_files
        );

        if let Some(n) = self.split_file_groups {
            let files = all_file_groups
                .into_iter()
                .flat_map(FileGroup::into_inner)
                .collect::<Vec<_>>();
            all_file_groups = split_files_in_order(files, n.max(1));
        }

        if all_file_groups.is_empty() {
            let stats = Statistics {
                num_rows: Precision::Exact(0),
                total_byte_size: Precision::Exact(0),
                column_statistics: Statistics::unknown_column(
                    self.encoding.quad_schema().inner(),
                ),
            };
            // Keep a single empty group, such that the scan still has one (empty) partition.
            return Ok((vec![FileGroup::new(vec![])], Some(stats)));
        }

        let (file_groups, stats) = compute_all_files_statistics(
            all_file_groups,
            Arc::clone(self.encoding.quad_schema().inner()),
            true,
            false,
        )?;
        Ok((file_groups, Some(stats)))
    }

    /// Prunes a single file in three phases (see [`Self::apply_eager_pruning`]). Returns [`None`]
    /// if no row group of the file remains.
    async fn prune_file(
        &self,
        fetcher: &ParquetMetadataFetcher,
        reader_factory: &BlockCachedReaderFactory,
        filters: &PruningFilters,
        file: PartitionedFile,
    ) -> DFResult<Option<PartitionedFile>> {
        let schema = self.encoding.quad_schema();
        let schema = schema.inner().as_ref();
        let metrics_set = ExecutionPlanMetricsSet::new();
        let metrics =
            ParquetFileMetrics::new(0, file.object_meta.location.as_ref(), &metrics_set);

        // Phase 0: Prune the file using its statistics (e.g., from the dataset description), such
        // that a file that cannot match is not opened at all.
        if let (Some(predicate), Some(statistics)) =
            (&filters.row_group, &file.statistics)
            && !file_may_match(predicate, statistics, schema)
        {
            return Ok(None);
        }

        // Phase 1: Prune row groups using the footer.
        let footer = fetcher
            .fetch(&file.object_meta, PageIndexPolicy::Skip)
            .await?;
        let mut access_plan =
            prune_row_groups(schema, &footer, filters.row_group.as_deref(), &metrics);
        if access_plan.row_group_indexes().is_empty() {
            return Ok(None);
        }

        // Phase 2: Prune row groups using the Bloom filters.
        let (bloom_filter_predicate, bloom_filter_columns) = match &filters.row_group {
            Some(predicate) if filters.bloom_filter => (
                Some(predicate.as_ref()),
                bloom_filter_columns(schema, &footer, predicate),
            ),
            _ => (None, Vec::new()),
        };
        if let Some(predicate) = bloom_filter_predicate
            && !bloom_filter_columns.is_empty()
        {
            let reader = reader_factory.create_reader(
                0,
                file.clone(),
                self.metadata_size_hint(),
                &metrics_set,
            )?;
            access_plan = prune_row_groups_by_bloom_filters(
                reader,
                &footer,
                predicate,
                &bloom_filter_columns,
                access_plan,
                &metrics,
            )
            .await?;
            if access_plan.row_group_indexes().is_empty() {
                return Ok(None);
            }
        }

        // Phase 3: Prune pages using the page index. The page index is not loaded if the footer
        // already guarantees that all rows of the remaining row groups match.
        let page_filter = filters.page.as_ref().filter(|_| {
            !filters.full_match.as_ref().is_some_and(|full_match| {
                all_row_groups_fully_match(schema, &footer, &access_plan, full_match)
            })
        });
        let (parquet_meta, access_plan) = match page_filter {
            Some(page_filter) => {
                let parquet_meta = fetcher
                    .fetch(&file.object_meta, PageIndexPolicy::Optional)
                    .await?;
                let access_plan = page_filter.prune_plan_with_page_index(
                    access_plan,
                    schema,
                    parquet_meta.file_metadata().schema_descr(),
                    &parquet_meta,
                    &metrics,
                );
                (parquet_meta, access_plan)
            }
            None => (footer, access_plan),
        };
        if access_plan.row_group_indexes().is_empty() {
            return Ok(None);
        }

        let mut stats = estimate_scan_statistics(&parquet_meta, &access_plan, schema);
        // The bounds of the file remain valid for the remaining rows. DataFusion requires them,
        // for example, to verify that the files of a group are ordered. They are inexact, as the
        // pruned rows may have contained the minimum or maximum.
        if let Some(file_stats) = &file.statistics {
            for (column, file_column) in stats
                .column_statistics
                .iter_mut()
                .zip(&file_stats.column_statistics)
            {
                column.min_value = file_column.min_value.clone().to_inexact();
                column.max_value = file_column.max_value.clone().to_inexact();
            }
        }
        Ok(Some(
            file.with_extension(access_plan)
                .with_statistics(Arc::new(stats)),
        ))
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
                    &PhysicalPlanningContext::default(),
                )?;
                Ok((phys_expr, name))
            })
            .collect::<DFResult<Vec<_>>>()?;

        if let Some(quad_tables) = projection_indices {
            let mut exprs = Vec::with_capacity(quad_tables.len());
            for &idx in quad_tables {
                let expr = full_projections.get(idx).ok_or_else(|| {
                    plan_datafusion_err!(
                        "Projection quad_table {} out of bounds for schema length {}",
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

    /// Creates the filters used for eager pruning from the logical filter.
    fn create_pruning_filters(
        &self,
        combined_logical_filter: Option<Expr>,
    ) -> DFResult<PruningFilters> {
        let Some(logical_expr) = combined_logical_filter else {
            return Ok(PruningFilters::default());
        };

        let base_schema = self.encoding.quad_schema();
        let phys_expr = create_physical_expr(
            &logical_expr,
            base_schema.as_ref(),
            self.session_state.execution_props(),
            &PhysicalPlanningContext::default(),
        )?;

        let row_group = PruningPredicateBuilder::new()
            .with_file_schema(Arc::clone(base_schema.inner()))
            .build(Arc::clone(&phys_expr));

        let enable_page_index = self
            .session_state
            .table_options()
            .parquet
            .global
            .enable_page_index;
        let page = enable_page_index
            .then(|| {
                PagePruningAccessPlanFilter::new(
                    &phys_expr,
                    Arc::clone(base_schema.inner()),
                )
            })
            .filter(|page_filter| page_filter.filter_number() > 0);

        // The full match predicate is only required for deciding whether to load the page index.
        let full_match = match (&page, negate_filter(&logical_expr)) {
            (Some(_), Some(negated)) => {
                let negated = create_physical_expr(
                    &negated,
                    base_schema.as_ref(),
                    self.session_state.execution_props(),
                    &PhysicalPlanningContext::default(),
                )?;
                PruningPredicateBuilder::new()
                    .with_file_schema(Arc::clone(base_schema.inner()))
                    .build(negated)
            }
            _ => None,
        };

        let planning_bloom_filters = self
            .session_state
            .config()
            .rdf_fusion_options_or_default()
            .execution
            .planning_bloom_filters;
        let bloom_filter = self
            .session_state
            .table_options()
            .parquet
            .global
            .bloom_filter_on_read
            && match planning_bloom_filters {
                PlanningBloomFilters::Auto => self.block_cache.is_some(),
                PlanningBloomFilters::Always => true,
                PlanningBloomFilters::Never => false,
            };

        Ok(PruningFilters {
            row_group,
            bloom_filter,
            page,
            full_match,
        })
    }
}

/// The filters used for eager pruning. If a filter is not present, no pruning is done on that
/// level.
#[derive(Default)]
struct PruningFilters {
    /// Prunes row groups based on the statistics in the footer. Also used for pruning row groups
    /// based on Bloom filters.
    row_group: Option<Arc<PruningPredicate>>,
    /// Whether row groups should be pruned based on Bloom filters (see
    /// [`PlanningBloomFilters`]).
    bloom_filter: bool,
    /// Prunes pages based on the page index. If not present (e.g., the page index is disabled or
    /// no page-level pruning is possible), the page index is never loaded.
    page: Option<PagePruningAccessPlanFilter>,
    /// The negation of the filter. A row group that is pruned by this predicate only contains
    /// rows that match the filter. Hence, the page index cannot prune any of its pages.
    ///
    /// Only present if `page` is present, as it is only used for deciding whether to load the
    /// page index. If `page` is present but this is not (i.e., the filter cannot be negated), the
    /// page index is loaded for every file with remaining row groups.
    full_match: Option<Arc<PruningPredicate>>,
}

/// Returns whether a file with the given `statistics` may contain rows that match the
/// `predicate`.
fn file_may_match(
    predicate: &PruningPredicate,
    statistics: &Statistics,
    schema: &Schema,
) -> bool {
    let statistics = FileBoundStatistics { statistics, schema };
    predicate
        .prune(&statistics)
        .map_or(true, |may_match| may_match.first().copied().unwrap_or(true))
}

/// The [`PruningStatistics`] of a single file.
///
/// Unlike DataFusion's `PrunableStatistics`, inexact minimum and maximum values are used as well.
/// The statistics of a file are computed from its footer, where inexact values are bounds of the
/// actual values (e.g., values truncated by the Parquet writer).
struct FileBoundStatistics<'a> {
    statistics: &'a Statistics,
    schema: &'a Schema,
}

impl FileBoundStatistics<'_> {
    fn column_statistics(&self, column: &Column) -> Option<&ColumnStatistics> {
        let index = self.schema.index_of(column.name()).ok()?;
        self.statistics.column_statistics.get(index)
    }

    fn bound(
        &self,
        column: &Column,
        get_bound: impl Fn(&ColumnStatistics) -> &Precision<ScalarValue>,
    ) -> Option<ArrayRef> {
        let value = get_bound(self.column_statistics(column)?).get_value()?;
        value.to_array_of_size(1).ok()
    }
}

impl PruningStatistics for FileBoundStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<ArrayRef> {
        self.bound(column, |stats| &stats.min_value)
    }

    fn max_values(&self, column: &Column) -> Option<ArrayRef> {
        self.bound(column, |stats| &stats.max_value)
    }

    fn num_containers(&self) -> usize {
        1
    }

    fn null_counts(&self, column: &Column) -> Option<ArrayRef> {
        let Precision::Exact(null_count) = self.column_statistics(column)?.null_count
        else {
            return None;
        };
        Some(Arc::new(UInt64Array::from(vec![null_count as u64])))
    }

    fn row_counts(&self) -> Option<ArrayRef> {
        let Precision::Exact(num_rows) = self.statistics.num_rows else {
            return None;
        };
        Some(Arc::new(UInt64Array::from(vec![num_rows as u64])))
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &HashSet<ScalarValue>,
    ) -> Option<BooleanArray> {
        None
    }
}

/// Prunes the row groups of a file using the statistics in its footer.
fn prune_row_groups(
    schema: &Schema,
    parquet_meta: &ParquetMetaData,
    predicate: Option<&PruningPredicate>,
    metrics: &ParquetFileMetrics,
) -> ParquetAccessPlan {
    let access_plan = ParquetAccessPlan::new_all(parquet_meta.num_row_groups());
    let Some(predicate) = predicate else {
        return access_plan;
    };

    let mut rg_filter = RowGroupAccessPlanFilter::new(access_plan);
    rg_filter.prune_by_statistics(
        schema,
        parquet_meta.file_metadata().schema_descr(),
        parquet_meta.row_groups(),
        predicate,
        metrics,
    );
    rg_filter.build()
}

/// A Parquet column whose Bloom filters are used for pruning row groups.
struct BloomFilterColumn {
    /// The name of the column in the pruning predicate.
    name: String,
    /// The index of the column in the Parquet schema.
    parquet_idx: usize,
    physical_type: PhysicalType,
    type_length: i32,
}

/// Returns the columns whose Bloom filters should be used for pruning the row groups of a file.
///
/// These are the columns that are bound to literals in the `predicate`, except the bound prefix
/// of the file's sort order. For example, for a file sorted by GPOS and a pattern that binds G, P,
/// and S, the row group statistics already prune G and P effectively. However, the values of S
/// are spread across many row groups, such that only the Bloom filters of S can help. If the file
/// has no sort order, all bound columns are considered.
fn bloom_filter_columns(
    schema: &Schema,
    parquet_meta: &ParquetMetaData,
    predicate: &PruningPredicate,
) -> Vec<BloomFilterColumn> {
    let parquet_schema = parquet_meta.file_metadata().schema_descr();
    let bound_columns = predicate
        .literal_columns()
        .into_iter()
        .filter_map(|name| {
            let (parquet_idx, _) = parquet_column(parquet_schema, schema, &name)?;
            Some((name, parquet_idx))
        })
        .collect::<Vec<_>>();

    let sorted_prefix = parquet_meta
        .row_groups()
        .first()
        .and_then(|row_group| row_group.sorting_columns())
        .into_iter()
        .flatten()
        .map_while(|sorting_column| usize::try_from(sorting_column.column_idx).ok())
        .take_while(|idx| bound_columns.iter().any(|(_, bound)| bound == idx))
        .collect::<HashSet<_>>();

    bound_columns
        .into_iter()
        .filter(|(_, parquet_idx)| !sorted_prefix.contains(parquet_idx))
        .map(|(name, parquet_idx)| {
            let column = parquet_schema.column(parquet_idx);
            BloomFilterColumn {
                name,
                parquet_idx,
                physical_type: column.physical_type(),
                type_length: column.type_length(),
            }
        })
        .collect()
}

/// Prunes the row groups in `access_plan` using the Bloom filters of the given `columns`. The
/// Bloom filters are only read for the row groups in `access_plan`.
async fn prune_row_groups_by_bloom_filters(
    reader: Box<dyn AsyncFileReader>,
    parquet_meta: &Arc<ParquetMetaData>,
    predicate: &PruningPredicate,
    columns: &[BloomFilterColumn],
    access_plan: ParquetAccessPlan,
    metrics: &ParquetFileMetrics,
) -> DFResult<ParquetAccessPlan> {
    let reader_metadata = ArrowReaderMetadata::try_new(
        Arc::clone(parquet_meta),
        ArrowReaderOptions::new(),
    )?;
    let mut builder =
        ParquetRecordBatchStreamBuilder::new_with_metadata(reader, reader_metadata);

    let mut bloom_filters = vec![BloomFilterStatistics::new(); access_plan.len()];
    for idx in access_plan.row_group_indexes() {
        for column in columns {
            let Some(sbbf) = builder
                .get_row_group_column_bloom_filter(idx, column.parquet_idx)
                .await?
            else {
                continue;
            };
            bloom_filters[idx].insert(
                column.name.clone(),
                sbbf,
                column.physical_type,
                column.type_length,
            );
        }
    }

    let mut rg_filter = RowGroupAccessPlanFilter::new(access_plan);
    rg_filter.prune_by_bloom_filters(predicate, metrics, &bloom_filters);
    Ok(rg_filter.build())
}

/// Removes the conjuncts of `filter` that all rows of the files in `file_groups` match according
/// to their statistics. Returns [`None`] if no conjunct remains.
fn remove_conjuncts_proven_by_statistics(
    filter: Expr,
    schema: &Schema,
    file_groups: &[FileGroup],
) -> Option<Expr> {
    conjunction(
        split_conjunction_owned(filter)
            .into_iter()
            .filter(|conjunct| !is_proven_by_statistics(conjunct, schema, file_groups)),
    )
}

/// Returns true if the exact statistics of all files in `file_groups` guarantee that all rows
/// match `expr`. Only `col IS NULL` and `col IS NOT NULL` are supported.
fn is_proven_by_statistics(
    expr: &Expr,
    schema: &Schema,
    file_groups: &[FileGroup],
) -> bool {
    let (inner, is_null) = match expr {
        Expr::IsNull(inner) => (inner, true),
        Expr::IsNotNull(inner) => (inner, false),
        _ => return false,
    };
    let Expr::Column(column) = inner.as_ref() else {
        return false;
    };
    let Ok(idx) = schema.index_of(&column.name) else {
        return false;
    };

    file_groups.iter().flat_map(FileGroup::iter).all(|file| {
        let Some(statistics) = &file.statistics else {
            return false;
        };
        let null_count = statistics
            .column_statistics
            .get(idx)
            .map(|column| column.null_count);
        match (statistics.num_rows, null_count) {
            (Precision::Exact(num_rows), Some(Precision::Exact(null_count))) => {
                if is_null {
                    null_count == num_rows
                } else {
                    null_count == 0
                }
            }
            _ => false,
        }
    })
}

/// Returns true if the footer statistics guarantee that all rows of the row groups in the
/// `access_plan` match the filter (see [`PruningFilters::full_match`]).
///
/// The access plan only contains the row groups that *may* contain matching rows, so this cannot
/// be derived from it. DataFusion also marks fully matched row groups in the access plan, but it
/// adds `OR col IS NULL` to its inverted predicate for each nullable column. As patterns on the
/// default graph filter for `graph IS NULL`, it never considers such row groups fully matched.
fn all_row_groups_fully_match(
    schema: &Schema,
    parquet_meta: &ParquetMetaData,
    access_plan: &ParquetAccessPlan,
    full_match: &PruningPredicate,
) -> bool {
    let metrics_set = ExecutionPlanMetricsSet::new();
    let metrics = ParquetFileMetrics::new(0, "full_match", &metrics_set);
    let mut rg_filter = RowGroupAccessPlanFilter::new(access_plan.clone());
    rg_filter.prune_by_statistics(
        schema,
        parquet_meta.file_metadata().schema_descr(),
        parquet_meta.row_groups(),
        full_match,
        &metrics,
    );
    rg_filter.remaining_row_group_count() == 0
}

/// Negates a conjunction of (in)equality and null checks, such that a row matches the result if
/// it does not match `expr`. Returns [`None`] for unsupported expressions.
///
/// SQL's three-valued logic is considered: `a = v` is negated to `a != v OR a IS NULL`, as a row
/// with a null `a` does not match `a = v`.
fn negate_filter(expr: &Expr) -> Option<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::And,
            right,
        }) => Some(negate_filter(left)?.or(negate_filter(right)?)),
        Expr::BinaryExpr(BinaryExpr {
            left,
            op: Operator::Eq,
            right,
        }) => {
            let value = match (left.as_ref(), right.as_ref()) {
                (value, Expr::Literal(..)) | (Expr::Literal(..), value) => value,
                _ => return None,
            };
            Some(
                left.as_ref()
                    .clone()
                    .not_eq(right.as_ref().clone())
                    .or(value.clone().is_null()),
            )
        }
        Expr::IsNull(inner) => Some(inner.as_ref().clone().is_not_null()),
        Expr::IsNotNull(inner) => Some(inner.as_ref().clone().is_null()),
        _ => None,
    }
}

/// Recursively rewrites PlainTerm equality predicates into comparisons on the struct's leaf fields.
///
/// DataFusion's parquet scan cannot evaluate an `Eq` between a PlainTerm struct column and a
/// PlainTerm struct literal as a row-level filter. Translating
/// `subject = {term_type:0, value:"..", data_type:null, language_tag:null}` into
/// `subject["term_type"] = 0 AND subject["value"] = ".." AND subject["data_type"] IS NULL AND
/// subject["language_tag"] IS NULL` produces predicates that DataFusion can evaluate and prune on.
fn rewrite_plain_term_predicates(expr: Expr, schema: &DFSchema) -> DFResult<Expr> {
    match expr {
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => {
            let left = rewrite_plain_term_predicates(*left, schema)?;
            let right = rewrite_plain_term_predicates(*right, schema)?;

            if op == Operator::Eq {
                if let Some(predicate) = rewrite_plain_term_eq(&left, &right)? {
                    return Ok(predicate);
                }
                if let Some(predicate) = rewrite_plain_term_eq(&right, &left)? {
                    return Ok(predicate);
                }
                if let Some(predicate) = rewrite_plain_term_col_eq(&left, &right, schema)
                {
                    return Ok(predicate);
                }
            }

            Ok(Expr::BinaryExpr(BinaryExpr {
                left: Box::new(left),
                op,
                right: Box::new(right),
            }))
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            let expr = rewrite_plain_term_predicates(*expr, schema)?;
            let list = list
                .into_iter()
                .map(|e| rewrite_plain_term_predicates(e, schema))
                .collect::<DFResult<Vec<_>>>()?;
            rewrite_plain_term_in_list(expr, list, negated)
        }
        other => Ok(other),
    }
}

/// Returns whether the given column is typed as a [PlainTermEncoding].
fn is_plain_term_column(column: &Column, schema: &DFSchema) -> bool {
    schema
        .field_from_column(column)
        .is_ok_and(|field| field.data_type() == &PlainTermEncoding::data_type())
}

/// Splits the `files` into up to `n` groups of consecutive files.
///
/// In contrast to [`FileGroup::split_files`], which sorts the files by their path, the order of
/// the files is preserved.
fn split_files_in_order(files: Vec<PartitionedFile>, n: usize) -> Vec<FileGroup> {
    if files.is_empty() {
        return Vec::new();
    }
    let chunk_size = files.len().div_ceil(n);
    files
        .chunks(chunk_size)
        .map(|chunk| FileGroup::new(chunk.to_vec()))
        .collect()
}

/// Rewrites an equality between two PlainTerm columns (e.g. a graph bound to an object/variable) into
/// leaf-field comparisons that DataFusion can evaluate. Comparable to a "null-safe" equality.
fn rewrite_plain_term_col_eq(
    left: &Expr,
    right: &Expr,
    schema: &DFSchema,
) -> Option<Expr> {
    let Expr::Column(left_col) = left else {
        return None;
    };
    let Expr::Column(right_col) = right else {
        return None;
    };
    if left_col == right_col {
        return None;
    }
    if !is_plain_term_column(left_col, schema) || !is_plain_term_column(right_col, schema)
    {
        return None;
    }

    let mut conjuncts = Vec::new();
    for field_name in ["term_type", "value", "data_type", "language_tag"] {
        let left = get_field(Expr::Column(left_col.clone()), field_name);
        let right = get_field(Expr::Column(right_col.clone()), field_name);
        // Null-safe: equal values, or both are NULL (for the nullable data_type/language_tag
        // fields). Using it for all fields is also correct for the non-nullable ones.
        conjuncts.push(
            left.clone()
                .eq(right.clone())
                .or(left.is_null().and(right.is_null())),
        );
    }
    conjunction(conjuncts)
}

/// Rewrites a PlainTerm `IN` predicate into a filter on the leaf `value` field.
///
/// `IN` predicates are only generated for graph names, which are always IRIs. So
/// `graph IN ({term_type:0, value:"g1", ..}, ..)` is rewritten to
/// `graph["term_type"] = 0 AND graph["value"] IN ("g1", ..)` which DataFusion can evaluate. Any
/// PlainTerm `IN` list that cannot be rewritten this way is rejected loudly instead of silently
/// producing false results.
fn rewrite_plain_term_in_list(
    expr: Expr,
    list: Vec<Expr>,
    negated: bool,
) -> DFResult<Expr> {
    // Only PlainTerm typed literals are affected.
    if !list.iter().any(is_plain_term_literal) {
        return Ok(Expr::InList(InList {
            expr: Box::new(expr),
            list,
            negated,
        }));
    }

    let column = match expr {
        Expr::Column(column) => column,
        other => {
            return Err(plan_datafusion_err!(
                "Cannot rewrite PlainTerm IN-list against a non-column expression: {other:?}"
            ));
        }
    };
    if negated {
        return Err(plan_datafusion_err!(
            "Cannot rewrite a negated PlainTerm IN-list predicate, which would silently produce false results."
        ));
    }

    let mut values = Vec::with_capacity(list.len());
    for item in list {
        let scalar = plain_term_scalar(&item).ok_or_else(|| {
            plan_datafusion_err!(
                "PlainTerm IN-list contains a non-PlainTerm element: {item:?}"
            )
        })?;
        let parts = scalar.as_parts().ok_or_else(|| {
            plan_datafusion_err!("PlainTerm IN-list contains a null term: {scalar:?}")
        })?;
        // Graph names are always IRIs (term_type 0 = NamedNode).
        if parts.term_type != i8::from(PlainTermType::NamedNode) {
            return Err(plan_datafusion_err!(
                "Cannot rewrite a non-IRI entry in a PlainTerm graph IN-list: {item:?}"
            ));
        }
        values.push(lit(ScalarValue::Utf8(Some(parts.value.to_string()))));
    }

    let field = |name: &str| get_field(Expr::Column(column.clone()), name);
    Ok(field("term_type")
        .eq(lit(ScalarValue::Int8(Some(
            PlainTermType::NamedNode.into(),
        ))))
        .and(field("value").in_list(values, false)))
}

fn is_plain_term_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(scalar, _) if PlainTermScalar::try_new(scalar.clone()).is_ok()
    )
}

fn plain_term_scalar(expr: &Expr) -> Option<PlainTermScalar> {
    if let Expr::Literal(scalar, _) = expr {
        PlainTermScalar::try_new(scalar.clone()).ok()
    } else {
        None
    }
}

/// Rewrites `other = <plain term literal>` into a conjunction of leaf-field comparisons.
///
/// Returns:
/// - `Ok(None)` if `literal` is not a PlainTerm-typed struct literal (nothing to rewrite),
/// - `Ok(Some(expr))` with the rewritten predicate,
/// - `Err` if `literal` is a PlainTerm literal that cannot be safely rewritten. Leaving such a
///   predicate untouched would silently produce false results (DataFusion cannot apply an equality
///   on a PlainTerm struct column), so we fail loudly instead.
fn rewrite_plain_term_eq(other: &Expr, literal: &Expr) -> DFResult<Option<Expr>> {
    let Expr::Literal(scalar, _) = literal else {
        return Ok(None);
    };

    let Ok(scalar) = PlainTermScalar::try_new(scalar.clone()) else {
        // Not a PlainTerm literal, nothing to do.
        return Ok(None);
    };

    // This is a PlainTerm literal comparison, so the other side must be a column we can rewrite.
    let Expr::Column(column) = other else {
        return Err(plan_datafusion_err!(
            "Cannot rewrite PlainTerm predicate comparison against a non-column expression: {other:?}"
        ));
    };

    let parts = scalar.as_parts().ok_or_else(|| {
        plan_datafusion_err!(
            "Cannot rewrite PlainTerm predicate comparison against a null term: {scalar:?}"
        )
    })?;

    let field = |name: &str| get_field(Expr::Column(column.clone()), name);

    let mut conjuncts = vec![
        field("term_type").eq(lit(ScalarValue::Int8(Some(parts.term_type)))),
        field("value").eq(lit(ScalarValue::Utf8(Some(parts.value.to_string())))),
    ];
    match parts.data_type {
        Some(datatype) => conjuncts.push(
            field("data_type").eq(lit(ScalarValue::Utf8(Some(datatype.to_string())))),
        ),
        None => conjuncts.push(field("data_type").is_null()),
    }
    match parts.language_tag {
        Some(language_tag) => conjuncts.push(
            field("language_tag")
                .eq(lit(ScalarValue::Utf8(Some(language_tag.to_string())))),
        ),
        None => conjuncts.push(field("language_tag").is_null()),
    }

    Ok(conjunction(conjuncts))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field};
    use datafusion::common::ColumnStatistics;
    use datafusion::common::ScalarValue;
    use datafusion::logical_expr::{col, lit};
    use rdf_fusion_common::NamedNodeRef;
    use rdf_fusion_encoding::EncodingScalar;
    use rdf_fusion_encoding::plain_term::{PLAIN_TERM_ENCODING, PlainTermScalar};

    fn named_node_term(iri: &str) -> Expr {
        Expr::Literal(
            PlainTermScalar::from(NamedNodeRef::new_unchecked(iri)).into_scalar_value(),
            None,
        )
    }

    fn test_schema() -> DFSchemaRef {
        QuadStorageEncoding::PlainTerm.quad_schema()
    }

    fn rewrite(expr: Expr) -> DFResult<Expr> {
        rewrite_plain_term_predicates(expr, test_schema().as_ref())
    }

    #[test]
    fn rewrites_plain_term_equality_to_field_comparisons() {
        let predicate: Expr = col("subject").eq(named_node_term("http://p1"));
        let rewritten = rewrite(predicate).unwrap();
        insta::assert_snapshot!(
            rewritten.to_string(),
            @r#"get_field(subject, Utf8("term_type")) = Int8(0) AND get_field(subject, Utf8("value")) = Utf8("http://p1") AND get_field(subject, Utf8("data_type")) IS NULL AND get_field(subject, Utf8("language_tag")) IS NULL"#
        );
    }

    #[test]
    fn rewrites_plain_term_column_equality() {
        let predicate: Expr = col("graph").eq(col("object"));
        let rewritten = rewrite(predicate).unwrap();
        insta::assert_snapshot!(
            rewritten.to_string(),
            @r#"(get_field(graph, Utf8("term_type")) = get_field(object, Utf8("term_type")) OR get_field(graph, Utf8("term_type")) IS NULL AND get_field(object, Utf8("term_type")) IS NULL) AND (get_field(graph, Utf8("value")) = get_field(object, Utf8("value")) OR get_field(graph, Utf8("value")) IS NULL AND get_field(object, Utf8("value")) IS NULL) AND (get_field(graph, Utf8("data_type")) = get_field(object, Utf8("data_type")) OR get_field(graph, Utf8("data_type")) IS NULL AND get_field(object, Utf8("data_type")) IS NULL) AND (get_field(graph, Utf8("language_tag")) = get_field(object, Utf8("language_tag")) OR get_field(graph, Utf8("language_tag")) IS NULL AND get_field(object, Utf8("language_tag")) IS NULL)"#
        );
    }
    #[test]
    fn leaves_non_plain_term_literals_unchanged() {
        let predicate: Expr = col("subject").eq(lit("http://p1"));
        let rewritten = rewrite(predicate.clone()).unwrap();
        assert_eq!(rewritten.to_string(), predicate.to_string());
    }

    #[test]
    fn rewrites_plain_term_in_list_to_value_field() {
        let predicate: Expr = col("graph").in_list(
            vec![named_node_term("http://g1"), named_node_term("http://g2")],
            false,
        );
        let rewritten = rewrite(predicate).unwrap();
        insta::assert_snapshot!(
            rewritten.to_string(),
            @r#"get_field(graph, Utf8("term_type")) = Int8(0) AND get_field(graph, Utf8("value")) IN ([Utf8("http://g1"), Utf8("http://g2")])"#
        );
    }

    #[test]
    fn errors_on_null_plain_term_literal() {
        let struct_array =
            PLAIN_TERM_ENCODING.create_named_nodes_array(StringArray::new_null(1));
        let null_scalar = ScalarValue::try_from_array(&struct_array, 0).unwrap();
        let predicate: Expr = col("subject").eq(Expr::Literal(null_scalar, None));
        assert!(
            rewrite(predicate).is_err(),
            "Expected an error for a null PlainTerm literal"
        );
    }

    #[test]
    fn errors_when_plain_term_literal_is_compared_to_non_column() {
        let predicate: Expr = named_node_term("http://p1").eq(lit("not-a-column"));
        assert!(
            rewrite(predicate).is_err(),
            "Expected an error when the other side is not a column"
        );
    }

    #[test]
    fn negates_filter() {
        let filter = col("a").eq(lit(1)).and(col("b").is_null());
        assert_eq!(
            negate_filter(&filter),
            Some(
                col("a")
                    .not_eq(lit(1))
                    .or(col("a").is_null())
                    .or(col("b").is_not_null())
            )
        );
    }

    #[test]
    fn negate_filter_rejects_unsupported_expressions() {
        assert_eq!(negate_filter(&col("a").eq(col("b"))), None);
        assert_eq!(negate_filter(&col("a").in_list(vec![lit(1)], false)), None);
        assert_eq!(
            negate_filter(&col("a").eq(lit(1)).and(col("b").gt(lit(1)))),
            None
        );
    }

    /// Creates a file with `num_rows` rows of which `null_counts[i]` are null in column `i`.
    fn file_with_null_counts(
        num_rows: usize,
        null_counts: &[Precision<usize>],
    ) -> PartitionedFile {
        let column_statistics = null_counts
            .iter()
            .map(|null_count| ColumnStatistics {
                null_count: *null_count,
                ..ColumnStatistics::new_unknown()
            })
            .collect();
        PartitionedFile::new("file.parquet", 0).with_statistics(Arc::new(Statistics {
            num_rows: Precision::Exact(num_rows),
            total_byte_size: Precision::Absent,
            column_statistics,
        }))
    }

    fn null_check_schema() -> Schema {
        Schema::new(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ])
    }

    #[test]
    fn removes_null_checks_proven_by_statistics() {
        let file_groups = vec![
            FileGroup::new(vec![file_with_null_counts(
                10,
                &[Precision::Exact(10), Precision::Exact(0)],
            )]),
            FileGroup::new(vec![file_with_null_counts(
                5,
                &[Precision::Exact(5), Precision::Exact(0)],
            )]),
        ];
        let filter = col("a")
            .is_null()
            .and(col("b").is_not_null())
            .and(col("b").eq(lit("x")));
        assert_eq!(
            remove_conjuncts_proven_by_statistics(
                filter,
                &null_check_schema(),
                &file_groups
            ),
            Some(col("b").eq(lit("x")))
        );
    }

    #[test]
    fn keeps_null_checks_not_proven_by_statistics() {
        let schema = null_check_schema();
        let filter = col("a").is_null().and(col("b").is_not_null());

        // One file has non-null values in `a` and null values in `b`.
        let file_groups = vec![FileGroup::new(vec![
            file_with_null_counts(10, &[Precision::Exact(10), Precision::Exact(0)]),
            file_with_null_counts(10, &[Precision::Exact(9), Precision::Exact(1)]),
        ])];
        assert_eq!(
            remove_conjuncts_proven_by_statistics(filter.clone(), &schema, &file_groups),
            Some(filter.clone())
        );

        // Inexact statistics and files without statistics prove nothing.
        let file_groups = vec![FileGroup::new(vec![file_with_null_counts(
            10,
            &[Precision::Inexact(10), Precision::Inexact(0)],
        )])];
        assert_eq!(
            remove_conjuncts_proven_by_statistics(filter.clone(), &schema, &file_groups),
            Some(filter.clone())
        );
        let file_groups = vec![FileGroup::new(vec![PartitionedFile::new(
            "file.parquet",
            0,
        )])];
        assert_eq!(
            remove_conjuncts_proven_by_statistics(filter.clone(), &schema, &file_groups),
            Some(filter)
        );
    }
}
