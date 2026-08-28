use crate::parquet::reader::PreLoadedMetadataReaderFactory;
use crate::parquet::reader::{PreloadedBloomFilters, PreloadedParquetMetadata};
use crate::parquet::scan::ParquetQuadScanExec;
use datafusion::common::ScalarValue;
use datafusion::common::plan_datafusion_err;
use datafusion::common::stats::Precision;
use datafusion::common::{Column, DFSchema, DFSchemaRef, ExprSchema, Statistics};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, PagePruningAccessPlanFilter, ParquetAccessPlan,
    ParquetFileReaderFactory, RowGroupAccess, RowGroupAccessPlanFilter,
    can_expr_be_pushed_down_with_schemas,
};
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, FileSource, ParquetFileMetrics, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchemaBuilder;
use datafusion::execution::SessionState;
use datafusion::functions::core::expr_fn::get_field;
use datafusion::logical_expr::expr::{BinaryExpr, InList};
use datafusion::logical_expr::physical_planning_context::PhysicalPlanningContext;
use datafusion::logical_expr::utils::conjunction;
use datafusion::logical_expr::{Expr, Operator, lit};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_optimizer::pruning::PruningPredicateBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::{
    ProjectionExec, ProjectionExpr, ProjectionExprs,
};
use object_store::ObjectMeta;
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_encoding::plain_term::{
    PlainTermEncoding, PlainTermScalar, PlainTermType,
};
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::sync::Arc;

/// Determines how the projection should be handled during the parquet scan.
pub enum PushdownProjection {
    /// No projection pushdown. The scan returns the base schema.
    No,
    /// Pushdown the projection with optional additional projection quad_tables.
    Yes(Option<Vec<usize>>),
}

use crate::block_cache::BlockCache;

type EagerPruningResult = (ParquetAccessPlan, Option<Arc<dyn PhysicalExpr>>, Statistics);

/// Defines which [`ParquetFileReaderFactory`] should be used during scanning.
pub enum ParquetQuadScanReaderFactoryType {
    /// Uses the default DataFusion parquet reader
    Default,
    /// Create a cached reader that already knows the parquet metadata
    Preloaded(
        PreloadedParquetMetadata,
        PreloadedBloomFilters,
        Option<Arc<BlockCache>>,
    ),
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
    output_ordering: Option<Vec<datafusion::physical_expr::LexOrdering>>,
    object_store: Option<Arc<dyn object_store::ObjectStore>>,
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
            output_ordering: None,
            object_store: None,
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

    pub fn with_output_ordering(
        mut self,
        output_ordering: Vec<datafusion::physical_expr::LexOrdering>,
    ) -> Self {
        self.output_ordering = Some(output_ordering);
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

    /// Caches the byte ranges of the parquet files being scanned in the given chunk cache.
    pub fn with_object_store(
        mut self,
        object_store: Arc<dyn object_store::ObjectStore>,
    ) -> Self {
        self.object_store = Some(object_store);
        self
    }

    /// Builds the Parquet scan.
    pub async fn build(self) -> DFResult<Arc<dyn ExecutionPlan>> {
        let base_schema = self.encoding.quad_schema();

        // The original, logical filter over the quad table.
        let combined_logical_filter = if let Some(pattern) = &self.pattern {
            conjunction(pattern.compute_filters(&self.encoding).await?)
        } else {
            None
        };

        // The predicate pushed into the parquet source. For PlainTerm, equalities and graph IN
        // lists are rewritten into leaf-field comparisons so the source can use them for pruning
        // and row filtering.
        let pushed_filter = match (&self.encoding, combined_logical_filter.clone()) {
            (QuadStorageEncoding::PlainTerm, Some(filter)) => {
                Some(rewrite_plain_term_predicates(filter, base_schema.as_ref())?)
            }
            (_, other) => other,
        };

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

        let (file_groups, statistics) =
            self.apply_eager_pruning(pushed_filter.clone())?;

        let mut file_scan_config =
            FileScanConfigBuilder::new(self.object_store_url.clone(), file_source)
                .with_file_groups(file_groups);
        if let Some(stats) = statistics {
            file_scan_config = file_scan_config.with_statistics(stats);
        }
        if let Some(ordering) = self.output_ordering {
            file_scan_config = file_scan_config.with_output_ordering(ordering);
        }
        let data_source =
            Arc::new(DataSourceExec::new(Arc::new(file_scan_config.build())));

        let pattern = self.pattern.clone().unwrap_or_else(QuadPattern::all_quads);
        let scan = Arc::new(ParquetQuadScanExec::try_new(pattern.clone(), data_source)?);

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

        if let PushdownProjection::Yes(quad_tables) = pushdown_projection {
            if let Some(pattern) = pattern {
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
        }

        Ok(plan)
    }

    /// Builds the [`FileSource`] that is used to implement the scan.
    fn build_file_source(
        &self,
        combined_logical_filter: Option<Expr>,
        base_schema: &DFSchemaRef,
        full_filter_is_pushable: bool,
    ) -> DFResult<Arc<dyn FileSource>> {
        let table_schema =
            TableSchemaBuilder::new(Arc::clone(base_schema.inner())).build();

        let store = if let Some(store) = self.object_store.clone() {
            store
        } else {
            self.session_state
                .runtime_env()
                .object_store(&self.object_store_url)?
        };
        let default_reader = Arc::new(DefaultParquetFileReaderFactory::new(store));
        let reader_factory: Arc<dyn ParquetFileReaderFactory> = match &self
            .reader_factory_type
        {
            ParquetQuadScanReaderFactoryType::Default => Arc::clone(&default_reader) as _,
            ParquetQuadScanReaderFactoryType::Preloaded(
                cache,
                bloom_filter_cache,
                block_cache,
            ) => Arc::new(PreLoadedMetadataReaderFactory::new(
                default_reader,
                cache.clone(),
                bloom_filter_cache.clone(),
                block_cache.clone(),
            )),
        };

        let mut parquet_source = ParquetSource::new(table_schema)
            .with_pushdown_filters(true)
            .with_parquet_file_reader_factory(reader_factory);

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
            ParquetQuadScanReaderFactoryType::Preloaded(cache, _, _) => cache,
        };

        let total_file_count: usize =
            self.file_groups.iter().map(|fg| fg.files().len()).sum();
        if total_file_count == 0 {
            let stats = Statistics {
                num_rows: Precision::Exact(0),
                total_byte_size: Precision::Exact(0),
                column_statistics: Statistics::unknown_column(
                    self.encoding.quad_schema().inner(),
                ),
            };
            return Ok((self.file_groups.clone(), Some(stats)));
        }

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

                    pf = pf.with_extension(access_plan);
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
                &PhysicalPlanningContext::default(),
            )?;

            let mut rg_filter = RowGroupAccessPlanFilter::new(access_plan);
            let metrics_set = ExecutionPlanMetricsSet::new();
            let metrics =
                ParquetFileMetrics::new(0, object_meta.location.as_ref(), &metrics_set);

            let predicate = PruningPredicateBuilder::new()
                .with_file_schema(Arc::clone(base_schema.inner()))
                .build(Arc::clone(&phys_expr));
            if let Some(predicate) = predicate {
                rg_filter.prune_by_statistics(
                    base_schema.inner().as_ref(),
                    parquet_meta.file_metadata().schema_descr(),
                    parquet_meta.row_groups(),
                    &predicate,
                    &metrics,
                );
            }

            let access_plan = rg_filter.build();

            // Page QuadTable Pruning
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
            match access_plan.inner()[i] {
                RowGroupAccess::Skip => {}
                RowGroupAccess::Scan => {
                    if rg.num_rows() > 0 {
                        row_count += rg.num_rows();
                        has_matching_row_group = true;
                    }
                }
                RowGroupAccess::Selection(ref selection) => {
                    let count = selection.row_count();
                    if count > 0 {
                        row_count += count as i64;
                        has_matching_row_group = true;
                    }
                }
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
}
