use crate::parquet::planner::ParquetQuadStoragePlanner;
use crate::parquet::reader::PreLoadedMetadataReaderFactory;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::stats::Precision;
use datafusion::common::{DFSchemaRef, Result as DFResult, Statistics};
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::parquet::{
    DefaultParquetFileReaderFactory, PagePruningAccessPlanFilter, ParquetAccessPlan,
    RowGroupAccess, RowGroupAccessPlanFilter,
};
use datafusion::datasource::physical_plan::{
    FileScanConfigBuilder, ParquetFileMetrics, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::create_physical_expr;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr_common::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_optimizer::pruning::PruningPredicate;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_planner::ExtensionPlanner;
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use futures::StreamExt;
use object_store::ObjectMeta;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use url::Url;

/// A snapshot of a [`ParquetQuadStorage`].
#[derive(Clone)]
pub struct ParquetQuadStorageSnapshot {
    encoding: QuadStorageEncoding,
    url: Url,
    object_meta: ObjectMeta,
    parquet_meta: Arc<ParquetMetaData>,
}

impl ParquetQuadStorageSnapshot {
    /// Creates a new [`ParquetQuadStorageSnapshot`].
    pub fn new(
        encoding: QuadStorageEncoding,
        url: Url,
        object_meta: ObjectMeta,
        parquet_meta: Arc<ParquetMetaData>,
    ) -> Self {
        Self {
            encoding,
            url,
            object_meta,
            parquet_meta,
        }
    }

    /// Plans a [`QuadPattern`].
    pub fn plan_quad_pattern(
        &self,
        pattern: &QuadPattern,
        output_schema: &DFSchemaRef,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let base_schema = Arc::clone(&self.encoding.quad_schema());

        // 1. Extract and combine logical filters
        let logical_filters = pattern.compute_filters(&self.encoding)?;
        let combined_logical_filter = logical_filters.into_iter().reduce(|a, b| a.and(b));

        // 2. Setup Eager Pruning Variables
        let num_row_groups = self.parquet_meta.num_row_groups();
        let access_plan = ParquetAccessPlan::new_all(num_row_groups);

        // 3. Eager Pruning Evaluation
        let (access_plan, physical_filter_expr) =
            if let Some(logical_expr) = combined_logical_filter {
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
                let metrics = ParquetFileMetrics::new(0, self.url.path(), &metrics_set);

                rg_filter.prune_by_statistics(
                    base_schema.inner().as_ref(),
                    self.parquet_meta.file_metadata().schema_descr(),
                    self.parquet_meta.row_groups(),
                    &predicate,
                    &metrics,
                );

                let mut access_plan = rg_filter.build();

                // Page Index Pruning
                let page_filter = PagePruningAccessPlanFilter::new(
                    predicate.orig_expr(),
                    Arc::clone(base_schema.inner()),
                );
                access_plan = page_filter.prune_plan_with_page_index(
                    access_plan,
                    base_schema.inner().as_ref(),
                    self.parquet_meta.file_metadata().schema_descr(),
                    &self.parquet_meta,
                    &metrics,
                );

                (access_plan, Some(phys_expr))
            } else {
                (access_plan, None)
            };

        // 4. Infer Statistics from Access Plan
        let mut total_rows = 0;
        for (idx, rg_meta) in self.parquet_meta.row_groups().iter().enumerate() {
            match access_plan.inner()[idx] {
                RowGroupAccess::Scan => {
                    total_rows += rg_meta.num_rows() as usize;
                }
                RowGroupAccess::Selection(ref selection) => {
                    let selected_rows: usize = selection
                        .iter()
                        .filter(|s| !s.skip)
                        .map(|s| s.row_count)
                        .sum();
                    total_rows += selected_rows;
                }
                RowGroupAccess::Skip => {}
            }
        }

        let num_rows = if total_rows == 0 {
            Precision::Exact(0)
        } else {
            Precision::Inexact(total_rows)
        };
        let statistics =
            Statistics::new_unknown(base_schema.inner().as_ref()).with_num_rows(num_rows);

        // 5. Build the Base Parquet Execution Node
        let mut partitioned_file =
            PartitionedFile::new_from_meta(self.object_meta.clone());
        partitioned_file.extensions = Some(Arc::new(access_plan));

        let table_schema = TableSchema::new(Arc::clone(base_schema.inner()), vec![]);

        // Extract the object store to create the default underlying reader
        let object_store_url = self.url.as_object_store_url();
        let store = session_state
            .runtime_env()
            .object_store(&object_store_url)?;

        // Create our custom factory
        let default_factory = Arc::new(DefaultParquetFileReaderFactory::new(store));
        let custom_factory = Arc::new(PreLoadedMetadataReaderFactory::new(
            default_factory,
            self.object_meta.location.to_string(),
            Arc::clone(&self.parquet_meta),
        ));

        // Inject the factory into ParquetSource
        let parquet_source = ParquetSource::new(table_schema)
            .with_parquet_file_reader_factory(custom_factory);

        let file_scan_config =
            FileScanConfigBuilder::new(object_store_url, Arc::new(parquet_source))
                .with_file(partitioned_file)
                .with_statistics(statistics)
                .build();

        let mut plan: Arc<dyn ExecutionPlan> =
            Arc::new(DataSourceExec::new(Arc::new(file_scan_config)));

        // 5. FilterExec operates on the base schema implicitly because `plan` has the base schema
        if let Some(phys_filter) = physical_filter_expr {
            plan = Arc::new(FilterExec::try_new(phys_filter, plan)?);
        }

        // 6. Wrap in ProjectionExec (Transitions data from base_schema to output_schema)
        let projections = pattern.compute_projection();
        let mut physical_projections = Vec::new();

        for (logical_expr, name) in projections {
            if let Ok(field) = output_schema.field_with_unqualified_name(&name) {
                let casted_expr = datafusion::logical_expr::cast(
                    logical_expr,
                    field.data_type().clone(),
                );

                let phys_proj_expr = create_physical_expr(
                    &casted_expr,
                    base_schema.as_ref(),
                    session_state.execution_props(),
                )?;
                physical_projections.push((phys_proj_expr, name.to_string()));
            }
        }

        plan = Arc::new(ProjectionExec::try_new(physical_projections, plan)?);

        Ok(plan)
    }
}

impl Debug for ParquetQuadStorageSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetQuadStorageSnapshot")
            .field("url", &self.url)
            .field("metadata", &self.parquet_meta)
            .finish()
    }
}

#[async_trait]
impl QuadStorageSnapshot for ParquetQuadStorageSnapshot {
    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        vec![Arc::new(ParquetQuadStoragePlanner::new(Arc::new(
            self.clone(),
        )))]
    }

    async fn named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        let pattern = QuadPattern::all_quads();
        let schema = pattern.compute_schema(&self.encoding);
        let plan = self
            .plan_quad_pattern(&pattern, &schema, state)
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let graph_col_idx = plan
            .schema()
            .index_of(COL_GRAPH)
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        let graph_col = Arc::new(Column::new(COL_GRAPH, graph_col_idx));

        let group_by =
            PhysicalGroupBy::new_single(vec![(graph_col, COL_GRAPH.to_string())]);

        let fields = vec![Field::new(
            COL_GRAPH,
            self.encoding.term_type().clone(),
            true,
        )];
        let output_schema = Arc::new(Schema::new(fields));

        let distinct_plan = Arc::new(
            AggregateExec::try_new(
                AggregateMode::Single,
                group_by,
                vec![],
                vec![],
                plan,
                output_schema,
            )
            .map_err(|e| StorageError::Other(Box::new(e)))?,
        );

        Ok(distinct_plan)
    }

    async fn len(&self, state: &SessionState) -> Result<usize, StorageError> {
        let pattern = QuadPattern::all_quads();
        let schema = pattern.compute_schema(&self.encoding);
        let plan = self
            .plan_quad_pattern(&pattern, &schema, state)
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let count = count_rows(plan, state.task_ctx())
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(count)
    }
}

async fn count_rows(
    plan: Arc<dyn ExecutionPlan>,
    task_ctx: Arc<TaskContext>,
) -> DFResult<usize> {
    let stats = plan.partition_statistics(None)?;
    if let Precision::Exact(exact_count) = stats.num_rows {
        return Ok(exact_count);
    }

    let mut total_count = 0;
    let partition_count = plan.properties().output_partitioning().partition_count();

    for partition in 0..partition_count {
        let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;

        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            total_count += batch.num_rows();
        }
    }

    Ok(total_count)
}
