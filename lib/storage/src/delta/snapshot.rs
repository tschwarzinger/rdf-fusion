use crate::delta::index::DeltaQuadStorageIndexSnapshot;
use crate::delta::log::{
    DeltaQuadStorageLog, DeltaQuadStorageLogChangesetRef, DeltaStorageLogVersionRange,
};
use crate::delta::objectids::DeltaObjectIdMapping;
use crate::delta::planner::DeltaQuadStoragePlanner;
use crate::delta::scan_plan_builder::DeltaQuadStorageScanPlanBuilder;
use async_trait::async_trait;
use datafusion::common::Result as DFResult;
use datafusion::common::stats::Precision;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_planner::ExtensionPlanner;
use deltalake::arrow::datatypes::{Field, Schema};
use futures::StreamExt;
use rdf_fusion_common::StorageError;
use rdf_fusion_common::quads::COL_GRAPH;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::sync::Arc;

/// A snapshot of a [`DeltaQuadStorage`](crate::delta::DeltaQuadStorage).
#[derive(Clone)]
pub struct DeltaQuadStorageSnapshot {
    log: Arc<DeltaQuadStorageLog>,
    indexes: Vec<DeltaQuadStorageIndexSnapshot>,
    encoding: QuadStorageEncoding,
    object_id_mapping: Option<Arc<DeltaObjectIdMapping>>,
    version: u64,
    transactional_changeset: Option<DeltaQuadStorageLogChangesetRef>,
}

impl DeltaQuadStorageSnapshot {
    /// Creates a new [`DeltaQuadStorageSnapshot`].
    pub fn new(
        log: Arc<DeltaQuadStorageLog>,
        indexes: Vec<DeltaQuadStorageIndexSnapshot>,
        encoding: QuadStorageEncoding,
        object_id_mapping: Option<Arc<DeltaObjectIdMapping>>,
        version: u64,
    ) -> Self {
        Self {
            log,
            indexes,
            encoding,
            object_id_mapping,
            version,
            transactional_changeset: None,
        }
    }

    pub fn log(&self) -> &Arc<DeltaQuadStorageLog> {
        &self.log
    }

    pub fn indexes(&self) -> &[DeltaQuadStorageIndexSnapshot] {
        &self.indexes
    }

    pub fn encoding(&self) -> &QuadStorageEncoding {
        &self.encoding
    }

    pub fn object_id_mapping(&self) -> Option<&Arc<DeltaObjectIdMapping>> {
        self.object_id_mapping.as_ref()
    }

    pub fn version(&self) -> u64 {
        self.version
    }

    pub fn transactional_changeset(&self) -> Option<&DeltaQuadStorageLogChangesetRef> {
        self.transactional_changeset.as_ref()
    }

    pub fn with_transactional_changeset(
        mut self,
        changeset: DeltaQuadStorageLogChangesetRef,
    ) -> Self {
        self.transactional_changeset = Some(changeset);
        self
    }
}

#[async_trait]
impl QuadStorageSnapshot for DeltaQuadStorageSnapshot {
    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        let snapshot = self.clone();
        let planner = DeltaQuadStoragePlanner::new(snapshot);
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

        let Some(named_graphs) = changeset.added_named_graphs(state).await? else {
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
        let mut builder = DeltaQuadStorageScanPlanBuilder::new(
            state.clone(),
            QuadPattern::for_all_quads(),
            self.encoding.clone(),
        )
        .with_best_index(&self.indexes)
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
            let stats = plan.partition_statistics(None)?;
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
