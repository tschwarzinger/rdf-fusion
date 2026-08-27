use crate::block_cache::BlockCache;
use crate::delta::log::{
    DeltaQuadsStorageLog, DeltaQuadsStorageLogChangesetRef, DeltaStorageLogVersionRange,
};
use crate::delta::objectids::DeltaObjectIdDictionary;
use crate::delta::planner::DeltaQuadsStoragePlanner;
use crate::delta::quad_table::DeltaQuadsQuadTableSnapshot;
use crate::delta::scan_plan_builder::DeltaQuadsStorageScanPlanBuilder;
use async_trait::async_trait;
use datafusion::common::Result as DFResult;
use datafusion::common::stats::Precision;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::{ExecutionPlan, StatisticsArgs, StatisticsContext};
use datafusion::physical_planner::ExtensionPlanner;
use deltalake::arrow::datatypes::{Field, Schema};
use futures::StreamExt;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::config::DeltaStorageOptions;
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::sync::Arc;

/// A snapshot of a [`DeltaQuadsStorage`](crate::delta::DeltaQuadsStorage).
#[derive(Clone)]
pub struct DeltaQuadsStorageSnapshot {
    log: Arc<DeltaQuadsStorageLog>,
    quad_tables: Vec<DeltaQuadsQuadTableSnapshot>,
    encoding: QuadStorageEncoding,
    object_id_mapping: Option<Arc<DeltaObjectIdDictionary>>,
    version: u64,
    options: DeltaStorageOptions,
    transactional_changeset: Option<DeltaQuadsStorageLogChangesetRef>,
    cache: Arc<BlockCache>,
}

impl DeltaQuadsStorageSnapshot {
    /// Creates a new [`DeltaQuadsStorageSnapshot`].
    pub fn new(
        log: Arc<DeltaQuadsStorageLog>,
        quad_tables: Vec<DeltaQuadsQuadTableSnapshot>,
        encoding: QuadStorageEncoding,
        object_id_mapping: Option<Arc<DeltaObjectIdDictionary>>,
        options: DeltaStorageOptions,
        version: u64,
        cache: Arc<BlockCache>,
    ) -> Self {
        Self {
            log,
            quad_tables,
            encoding,
            object_id_mapping,
            version,
            options,
            transactional_changeset: None,
            cache,
        }
    }

    pub fn log(&self) -> &Arc<DeltaQuadsStorageLog> {
        &self.log
    }

    pub fn cache(&self) -> &Arc<BlockCache> {
        &self.cache
    }

    pub fn quad_tables(&self) -> &[DeltaQuadsQuadTableSnapshot] {
        &self.quad_tables
    }

    pub fn encoding(&self) -> &QuadStorageEncoding {
        &self.encoding
    }

    pub fn options(&self) -> &DeltaStorageOptions {
        &self.options
    }

    pub fn object_id_mapping(&self) -> Option<&Arc<DeltaObjectIdDictionary>> {
        self.object_id_mapping.as_ref()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn transactional_changeset(&self) -> Option<&DeltaQuadsStorageLogChangesetRef> {
        self.transactional_changeset.as_ref()
    }

    pub fn with_transactional_changeset(
        mut self,
        changeset: DeltaQuadsStorageLogChangesetRef,
    ) -> Self {
        self.transactional_changeset = Some(changeset);
        self
    }
}

#[async_trait]
impl QuadStorageSnapshot for DeltaQuadsStorageSnapshot {
    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        let snapshot = self.clone();
        let planner = DeltaQuadsStoragePlanner::new(snapshot);
        vec![Arc::new(planner)]
    }

    async fn named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        let range = DeltaStorageLogVersionRange::new_unchecked(0, self.version);
        let changeset = self
            .log
            .compute_changeset(state, range)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let Some(named_graphs) = changeset
            .added_named_graphs(&crate::delta::log::ChangesetContext::default(), state)
            .await?
        else {
            let fields = vec![Field::new(
                COL_GRAPH,
                self.encoding.term_type().clone(),
                true,
            )];
            return Ok(Arc::new(EmptyExec::new(Arc::new(Schema::new(fields)))));
        };

        Ok(named_graphs)
    }

    async fn len(&self, state: &SessionState) -> Result<usize, StorageError> {
        let mut builder = DeltaQuadsStorageScanPlanBuilder::new(
            state.clone(),
            QuadPattern::all_quads(),
            self.encoding.clone(),
        )
        .with_cache(Arc::clone(&self.cache))
        .with_best_quad_table(&self.quad_tables)
        .map_err(|e| StorageError::Other(Box::new(e)))?
        .with_changeset_for_log(&self.log, Some(self.version))
        .await
        .map_err(|e| StorageError::Other(Box::new(e)))?;

        if let Some(transactional) = &self.transactional_changeset {
            builder = builder.with_changeset(Arc::clone(transactional));
        }

        let scan_planning_result = builder
            .build()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let physical_plan = scan_planning_result.scan;
        let count = count_rows(physical_plan, state.task_ctx())
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        return Ok(count);

        async fn count_rows(
            plan: Arc<dyn ExecutionPlan>,
            task_ctx: Arc<TaskContext>,
        ) -> DFResult<usize> {
            let stats = StatisticsContext::new()
                .compute(plan.as_ref(), &StatisticsArgs::new())?;
            if let Precision::Exact(exact_count) = stats.num_rows {
                return Ok(exact_count);
            }

            let mut total_count = 0;
            let partition_count =
                plan.properties().output_partitioning().partition_count();

            for partition in 0..partition_count {
                let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;

                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result?;
                    total_count += batch.num_rows();
                }
            }

            Ok(total_count)
        }
    }
}
