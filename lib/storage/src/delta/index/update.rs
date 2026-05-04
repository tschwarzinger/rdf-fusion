use crate::delta::error::DeltaQuadStorageError;
use crate::delta::index::{
    DeltaQuadStorageIndex, DeltaQuadStorageIndexSnapshot, FILE_ROW_COUNT,
};
use crate::delta::log::DeltaQuadStorageLogChangesetRef;
use crate::delta::scan_plan_builder::DeltaQuadStorageScanPlanBuilder;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::SessionState;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalExpr, PhysicalSortExpr};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties};
use deltalake::DeltaTable;
use deltalake::kernel::transaction::CommitBuilder;
use deltalake::kernel::{Action, Remove, Transaction};
use deltalake::parquet::file::properties::WriterProperties;
use deltalake::protocol::{DeltaOperation, SaveMode};
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use futures::StreamExt;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

/// Implements the updating process for a [`DeltaQuadStorageIndex`].
pub struct DeltaStorageQuadIndexUpdater {
    /// The index to update.
    index: DeltaQuadStorageIndexSnapshot,
    /// The delta table to update.
    index_table: DeltaTable,
    /// The target version to update to.
    changeset: DeltaQuadStorageLogChangesetRef,
    /// The session state to use.
    state: SessionState,
    /// The writer properties to use.
    writer_properties: WriterProperties,
}

impl DeltaStorageQuadIndexUpdater {
    /// Creates a new [`DeltaStorageQuadIndexUpdater`].
    pub fn new(
        index: DeltaQuadStorageIndexSnapshot,
        index_table: DeltaTable,
        changeset: DeltaQuadStorageLogChangesetRef,
        state: SessionState,
        writer_properties: WriterProperties,
    ) -> Self {
        Self {
            index,
            index_table,
            changeset,
            state,
            writer_properties,
        }
    }

    /// Applies the update to the index.
    pub async fn apply_update(
        mut self,
    ) -> Result<(DeltaTable, u64), DeltaQuadStorageError> {
        // Currently, we only support full rewrites.
        self.full_rewrite().await
    }

    /// Starts a full rewrite of the index, removing all existing files while adding the newly
    /// written files.
    async fn full_rewrite(&mut self) -> Result<(DeltaTable, u64), DeltaQuadStorageError> {
        let plan_result = DeltaQuadStorageScanPlanBuilder::new(
            self.state.clone(),
            QuadPattern::for_all_quads(),
            self.index.encoding(),
        )
        .with_index(self.index.clone())
        .with_changeset(Arc::clone(&self.changeset))
        .build()
        .await?;

        let sorted_physical_plan = self.sort_for_index(plan_result.scan)?;
        let files_to_add = self.write_new_files(sorted_physical_plan).await?;
        let files_to_remove = self.remove_all_files()?;
        let all_actions = [files_to_add, files_to_remove].concat();

        let result_table = self.commit_transaction(all_actions).await?;
        Ok((
            result_table,
            self.changeset.version_range().ending_version(),
        ))
    }

    /// Applies the correct sort order for this index using physical expressions
    fn sort_for_index(
        &self,
        plan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let schema = plan.schema();
        let mut sort_exprs = Vec::new();

        for component in self.index.components().inner() {
            let col_name = component.column_name();

            // Look up the physical index of the column in the schema
            let col_idx = schema.index_of(col_name).map_err(|e| {
                DeltaQuadStorageError::Other(format!(
                    "Column {col_name} not found in schema for sorting: {e}"
                ))
            })?;

            // Create the physical column reference
            let phys_col =
                Arc::new(Column::new(col_name, col_idx)) as Arc<dyn PhysicalExpr>;

            // Logical .sort(true, true) means asc=true, nulls_first=true.
            // This translates to descending=false, nulls_first=true in Arrow.
            sort_exprs.push(PhysicalSortExpr {
                expr: phys_col,
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            });
        }

        // Wrap the plan in a SortExec node
        let ordering = LexOrdering::new(sort_exprs).expect("Contains four columns");
        let coalesced_plan = Arc::new(CoalescePartitionsExec::new(plan));
        let sorted_plan = Arc::new(SortExec::new(ordering, coalesced_plan));

        Ok(sorted_plan)
    }

    async fn write_new_files(
        &self,
        quads: Arc<dyn ExecutionPlan>,
    ) -> Result<Vec<Action>, DeltaQuadStorageError> {
        if quads.output_partitioning().partition_count() != 1 {
            return Err(DeltaQuadStorageError::Other(
                "Index update requires a single partition".to_string(),
            ));
        }

        let mut stream = quads.execute(0, self.state.task_ctx())?;
        let mut writer = RecordBatchWriter::for_table(&self.index_table)?
            .with_writer_properties(self.writer_properties.clone());

        let mut current_rows = 0;
        let mut all_actions = Vec::new();
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result?;
            let num_rows = batch.num_rows();

            if num_rows == 0 {
                continue;
            }

            let aligned_batch = RecordBatch::try_new(
                Arc::clone(&writer.arrow_schema()),
                batch.columns().to_vec(),
            )?;

            writer.write(aligned_batch).await?;

            current_rows += num_rows;

            // If the update exceeds the target row count, flush and recreate the writer (new file)
            if current_rows >= FILE_ROW_COUNT {
                let file_actions = writer.flush().await?;
                all_actions.extend(file_actions.into_iter().map(Action::Add));
                current_rows = 0;
            }
        }

        if current_rows > 0 {
            let file_actions = writer.flush().await?;
            all_actions.extend(file_actions.into_iter().map(Action::Add));
        }
        Ok(all_actions)
    }

    /// Removes all files from the current snapshot of the table.
    fn remove_all_files(&self) -> Result<Vec<Action>, DeltaQuadStorageError> {
        let deletion_timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| DeltaQuadStorageError::Other(e.to_string()))?
            .as_millis() as i64;

        let remove_actions: Vec<Action> = self
            .index_table
            .snapshot()?
            .log_data()
            .into_iter()
            .map(|file| {
                #[allow(deprecated)]
                let add = file.add_action();
                Action::Remove(Remove {
                    path: add.path.clone(),
                    deletion_timestamp: Some(deletion_timestamp),
                    data_change: true,
                    extended_file_metadata: Some(true),
                    partition_values: Some(add.partition_values.clone()),
                    size: Some(add.size),
                    tags: add.tags.clone(),
                    deletion_vector: add.deletion_vector.clone(),
                    base_row_id: add.base_row_id,
                    default_row_commit_version: add.default_row_commit_version,
                })
            })
            .collect();

        Ok(remove_actions)
    }

    async fn commit_transaction(
        &mut self,
        actions: Vec<Action>,
    ) -> Result<DeltaTable, DeltaQuadStorageError> {
        let log_store = self.index_table.log_store();
        let snapshot = self.index_table.snapshot()?;

        let operation = DeltaOperation::Write {
            mode: SaveMode::Append,
            partition_by: None,
            predicate: None,
        };

        let sync_txn = Transaction {
            app_id: DeltaQuadStorageIndex::APP_ID.to_string(),
            version: self.changeset.version_range().ending_version() as i64,
            last_updated: None,
        };

        let commit_props = deltalake::kernel::transaction::CommitProperties::default()
            .with_application_transaction(sync_txn);

        let commit_builder = CommitBuilder::from(commit_props).with_actions(actions);

        let _finalized = commit_builder
            .build(Some(snapshot), log_store, operation)
            .await?;

        let mut table = self.index_table.clone();
        table.load().await?;

        Ok(table)
    }
}
