use crate::delta::error::DeltaQuadStorageError;
use crate::delta::log::operation_log_file::OperationLogFile;
use crate::delta::log::operations_changeset_stream::OperationsChangesetStream;
use crate::delta::log::{
    COL_OPERATION, DeltaQuadStorageLogChangeset, DeltaStorageLogOperation,
    DeltaStorageLogVersionRange, EagerChangeset,
};
use crate::exec::VerifyNotNullExec;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::DefaultParquetFileReaderFactory;
use datafusion::datasource::physical_plan::{
    FileGroup, FileScanConfigBuilder, ParquetSource,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::datasource::table_schema::TableSchema;
use datafusion::execution::SessionState;
use datafusion::physical_expr::expressions::{Column, col, is_not_null, lit};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_plan::aggregates::{
    AggregateExec, AggregateMode, PhysicalGroupBy,
};
use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use deltalake::logstore::ObjectStoreRef;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::mem::size_of;
use std::sync::Arc;

/// This is an optimized [`DeltaQuadStorageLogChangeset`] for changesets that only add quads into
/// the database.
pub struct LazyInsertionOnlyChangeset {
    table_schema: SchemaRef,
    table_uri: ObjectStoreUrl,
    object_store: ObjectStoreRef,
    version_range: DeltaStorageLogVersionRange,
    files: Vec<OperationLogFile>,
}

impl LazyInsertionOnlyChangeset {
    pub fn new(
        table_schema: SchemaRef,
        table_uri: ObjectStoreUrl,
        object_store: ObjectStoreRef,
        version_range: DeltaStorageLogVersionRange,
        files: Vec<OperationLogFile>,
    ) -> Self {
        Self {
            table_schema,
            table_uri,
            object_store,
            version_range,
            files,
        }
    }

    /// Returns an execution plan that scans all Parquet files.
    fn scan_all_files(
        &self,
        projection_indices: Vec<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let mut file_group = Vec::new();
        for file in &self.files {
            let partitioned_file =
                PartitionedFile::new(file.inner().path.clone(), file.inner().size as u64);
            file_group.push(partitioned_file);
        }

        let table_schema = TableSchema::new(Arc::clone(&self.table_schema), vec![]);

        let file_factory =
            DefaultParquetFileReaderFactory::new(Arc::clone(&self.object_store));
        let source = Arc::new(
            ParquetSource::new(table_schema)
                .with_parquet_file_reader_factory(Arc::new(file_factory)),
        );
        let file_scan_config = FileScanConfigBuilder::new(self.table_uri.clone(), source)
            .with_file_group(FileGroup::new(file_group))
            .with_projection_indices(Some(projection_indices))?
            .build();

        let scan_plan = Arc::new(DataSourceExec::new(Arc::new(file_scan_config)));
        Ok(scan_plan)
    }
}

#[async_trait]
impl DeltaQuadStorageLogChangeset for LazyInsertionOnlyChangeset {
    fn version_range(&self) -> DeltaStorageLogVersionRange {
        self.version_range
    }

    async fn cleared_graphs(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        Ok(None)
    }

    async fn removed_quads(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        Ok(None)
    }

    async fn added_quads(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        let scan_plan = self.scan_all_files(vec![2, 3, 4, 5])?;

        let group_by = PhysicalGroupBy::new_single(vec![
            (Arc::new(Column::new(COL_GRAPH, 0)), COL_GRAPH.to_string()),
            (
                Arc::new(Column::new(COL_SUBJECT, 1)),
                COL_SUBJECT.to_string(),
            ),
            (
                Arc::new(Column::new(COL_PREDICATE, 2)),
                COL_PREDICATE.to_string(),
            ),
            (Arc::new(Column::new(COL_OBJECT, 3)), COL_OBJECT.to_string()),
        ]);

        let scan_plan_schema = scan_plan.schema();
        let aggregate_plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            scan_plan,
            scan_plan_schema,
        )?);

        let verified_plan = Arc::new(VerifyNotNullExec::try_new(
            aggregate_plan,
            vec![1, 2, 3], // subject, predicate, object
        )?);

        Ok(Some(verified_plan))
    }

    /// The added quads may implicitly create new graphs.
    async fn added_named_graphs(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        let scan_plan = self.scan_all_files(vec![2])?;
        let filtered = FilterExec::try_new(
            is_not_null(col(COL_GRAPH, scan_plan.schema().as_ref())?)?,
            scan_plan,
        )?;

        let group_by = PhysicalGroupBy::new_single(vec![(
            Arc::new(Column::new(COL_GRAPH, 0)),
            COL_GRAPH.to_string(),
        )]);

        let scan_plan_schema = filtered.schema();
        let aggregate_plan = Arc::new(AggregateExec::try_new(
            AggregateMode::Single,
            group_by,
            vec![],
            vec![],
            Arc::new(filtered),
            scan_plan_schema,
        )?);

        Ok(Some(aggregate_plan))
    }

    async fn dropped_named_graphs(
        &self,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadStorageError> {
        Ok(None)
    }

    async fn as_eager_changeset(
        &self,
        state: &SessionState,
    ) -> Result<EagerChangeset, DeltaQuadStorageError> {
        let operations = self
            .added_quads(state)
            .await?
            .expect("Quads are never empty");
        let operations_schema = operations.schema();
        let with_operation_type = ProjectionExec::try_new(
            [
                ProjectionExpr::new(
                    lit(DeltaStorageLogOperation::InsertQuad.as_stored()),
                    COL_OPERATION,
                ),
                ProjectionExpr::new(
                    col(COL_GRAPH, operations_schema.as_ref())?,
                    COL_GRAPH,
                ),
                ProjectionExpr::new(
                    col(COL_SUBJECT, operations_schema.as_ref())?,
                    COL_SUBJECT,
                ),
                ProjectionExpr::new(
                    col(COL_PREDICATE, operations_schema.as_ref())?,
                    COL_PREDICATE,
                ),
                ProjectionExpr::new(
                    col(COL_OBJECT, operations_schema.as_ref())?,
                    COL_OBJECT,
                ),
            ],
            operations,
        )?;
        let stream = execute_stream(Arc::new(with_operation_type), state.task_ctx())?;
        let stream = OperationsChangesetStream::try_new(stream);
        EagerChangeset::partition_operations(state, self.version_range, stream).await
    }

    fn size(&self) -> usize {
        size_of::<Self>() + self.files.len() * size_of::<OperationLogFile>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::DeltaQuadStorageBuilder;
    use datafusion::physical_plan::collect;
    use rdf_fusion_common::{GraphName, NamedNode, Quad};
    use rdf_fusion_encoding::quads_to_plain_term_dataframe;
    use rdf_fusion_execution::RdfFusionContextBuilder;
    use rdf_fusion_extensions::storage::QuadStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_as_eager_changeset_conversion() -> Result<(), Box<dyn std::error::Error>>
    {
        let storage = Arc::new(DeltaQuadStorageBuilder::new().build().await?);
        let context = RdfFusionContextBuilder::new(storage.clone()).build()?;

        let state = context.session_context().state();
        let txn = storage.begin_transaction(&state).await?;
        txn.insert(quads_to_plain_term_dataframe(
            &context.session_context(),
            &[Quad::new(
                NamedNode::new_unchecked("https://my.com/s"),
                NamedNode::new_unchecked("https://my.com/p"),
                NamedNode::new_unchecked("https://my.com/o"),
                GraphName::DefaultGraph,
            )],
        ))
        .await?;
        txn.commit().await?;

        // Should return a lazy changeset as we've only added.
        let changeset = storage
            .log()
            .compute_changeset(&state, DeltaStorageLogVersionRange::new_unchecked(0, 1))
            .await?;
        let eager = changeset.as_eager_changeset(&state).await?;

        let added_quads = eager.added_quads(&state).await.unwrap().unwrap();
        let result = collect(added_quads, state.task_ctx()).await?;
        assert_eq!(result.iter().map(|rb| rb.num_rows()).sum::<usize>(), 1);
        Ok(())
    }
}
