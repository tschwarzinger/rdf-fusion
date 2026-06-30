use crate::parquet::planner::ParquetQuadStoragePlanner;
use crate::parquet::reader::{PreloadedBloomFilters, PreloadedParquetMetadata};
use crate::parquet::scan_builder::{
    ParquetQuadScanBuilder, ParquetQuadScanReaderFactoryType, PushdownProjection,
};
#[allow(unused_imports)]
use crate::parquet::storage::ParquetQuadStorage;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::Result as DFResult;
use datafusion::common::stats::Precision;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
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
    bloom_filter_cache: PreloadedBloomFilters,
}

impl ParquetQuadStorageSnapshot {
    /// Creates a new [`ParquetQuadStorageSnapshot`].
    pub fn new(
        encoding: QuadStorageEncoding,
        url: Url,
        object_meta: ObjectMeta,
        parquet_meta: Arc<ParquetMetaData>,
        bloom_filter_cache: PreloadedBloomFilters,
    ) -> Self {
        Self {
            encoding,
            url,
            object_meta,
            parquet_meta,
            bloom_filter_cache,
        }
    }

    /// Plans a [`QuadPattern`].
    pub fn plan_quad_pattern(
        &self,
        pattern: &QuadPattern,
        projection: Option<Vec<usize>>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let cache = PreloadedParquetMetadata::new();
        cache.insert(
            self.object_meta.location.clone(),
            (Arc::clone(&self.parquet_meta), self.object_meta.clone()),
        );

        let custom_factory = ParquetQuadScanReaderFactoryType::Preloaded(
            cache,
            self.bloom_filter_cache.clone(),
        );

        let partitioned_file = PartitionedFile::new_from_meta(self.object_meta.clone());

        let plan = ParquetQuadScanBuilder::new(
            session_state,
            self.encoding.clone(),
            self.url.as_object_store_url(),
            vec![FileGroup::new(vec![partitioned_file])],
        )
        .with_quad_pattern(pattern.clone())
        .with_reader_factory_type(custom_factory)
        .with_eager_pruning(true)
        .with_pushdown_projection(PushdownProjection::Yes(projection))
        .build()?;

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
        let plan = self
            .plan_quad_pattern(&pattern, None, state)
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
        let plan = self
            .plan_quad_pattern(&pattern, None, state)
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
