use crate::block_cache::BlockCache;
use crate::parquet::scan_builder::{ParquetQuadScanBuilder, PushdownProjection};
#[allow(unused_imports)]
use crate::parquet::storage::ParquetQuadStorage;
use crate::parquet::storage::ParquetQuadTable;
use crate::quad_tables::compute_scan_score;
use async_trait::async_trait;
use datafusion::arrow::datatypes::{Field, Schema};
use datafusion::common::Result as DFResult;
use datafusion::common::stats::Precision;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext};
use futures::StreamExt;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::cmp::Reverse;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use url::Url;

/// A snapshot of a [`ParquetQuadStorage`].
#[derive(Clone)]
pub struct ParquetQuadStorageSnapshot {
    encoding: QuadStorageEncoding,
    url: Url,
    object_store_url: ObjectStoreUrl,
    quad_tables: Arc<Vec<ParquetQuadTable>>,
    block_cache: Option<Arc<BlockCache>>,
}

impl ParquetQuadStorageSnapshot {
    /// Creates a new [`ParquetQuadStorageSnapshot`].
    pub fn new(
        encoding: QuadStorageEncoding,
        url: Url,
        object_store_url: ObjectStoreUrl,
        quad_tables: Arc<Vec<ParquetQuadTable>>,
        block_cache: Option<Arc<BlockCache>>,
    ) -> Self {
        Self {
            encoding,
            url,
            object_store_url,
            quad_tables,
            block_cache,
        }
    }

    /// Returns the quad table that is best suited for scanning the `pattern` (see
    /// [`compute_scan_score`]). On ties, the first quad table is chosen.
    fn choose_quad_table(&self, pattern: &QuadPattern) -> &ParquetQuadTable {
        self.quad_tables
            .iter()
            .enumerate()
            .max_by_key(|(idx, table)| {
                let score = table.sort_order().map_or(0, |sort_order| {
                    compute_scan_score(
                        sort_order.components(),
                        &pattern.active_graph,
                        &pattern.triple_pattern,
                        pattern.blank_node_mode,
                    )
                });
                (score, Reverse(*idx))
            })
            .map(|(_, table)| table)
            .expect("A Parquet storage has at least one quad table")
    }

    /// Plans a [`QuadPattern`].
    pub async fn plan_quad_pattern(
        &self,
        pattern: &QuadPattern,
        projection: Option<Vec<usize>>,
        session_state: &SessionState,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let quad_table = self.choose_quad_table(pattern);
        let mut builder = ParquetQuadScanBuilder::new(
            session_state,
            self.encoding.clone(),
            self.object_store_url.clone(),
            vec![FileGroup::new(quad_table.files().as_ref().clone())],
        )
        .with_quad_pattern(pattern.clone())
        .with_block_cache(self.block_cache.clone())
        .with_eager_pruning(true)
        .with_split_file_groups(session_state.config().target_partitions())
        .with_pushdown_projection(PushdownProjection::Yes(projection));
        if let Some(output_ordering) = quad_table.output_ordering() {
            builder = builder.with_output_ordering(vec![output_ordering.clone()]);
        }
        let plan = builder.build().await?;

        Ok(plan)
    }
}

impl Debug for ParquetQuadStorageSnapshot {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetQuadStorageSnapshot")
            .field("url", &self.url)
            .field("num_quad_tables", &self.quad_tables.len())
            .finish()
    }
}

#[async_trait]
impl QuadStorageSnapshot for ParquetQuadStorageSnapshot {
    async fn scan_quad_pattern(
        &self,
        pattern: &QuadPattern,
        projection: Option<Vec<usize>>,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        self.plan_quad_pattern(pattern, projection, session_state)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))
    }

    async fn named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        let pattern = QuadPattern::all_quads();
        let plan = self
            .plan_quad_pattern(&pattern, None, state)
            .await
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
            .await
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
    let stats =
        StatisticsContext::new().compute(plan.as_ref(), &StatisticsArgs::new())?;
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
