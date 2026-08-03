use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::log::operation_log_file::OperationLogFile;
use crate::delta::log::operations_changeset_stream::OperationsChangesetStream;
use crate::delta::log::{
    COL_OPERATION, ChangesetContext, DeltaQuadsStorageLogChangeset,
    DeltaStorageLogOperation, DeltaStorageLogVersionRange, EagerChangeset,
};
use crate::exec::VerifyNotNullExec;
use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
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
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_expr::expressions::{Column, col, is_not_null, lit};
use datafusion::physical_expr::projection::ProjectionExpr;
use datafusion::physical_expr::{LexOrdering, LexRequirement, PhysicalSortExpr};
use datafusion::physical_plan::sorts::sort::SortExec;
use rdf_fusion_common::QuadComponent;
use rdf_fusion_physical::distinct::SortedDistinctExec;

use datafusion::physical_plan::filter::FilterExec;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use deltalake::logstore::ObjectStoreRef;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::mem::size_of;
use std::sync::Arc;

/// This is an optimized [`DeltaQuadsStorageLogChangeset`] for changesets that only add quads into
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
        state: &SessionState,
        projection_indices: Vec<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadsStorageError> {
        let target_partitions = state.config().target_partitions();

        let mut file_groups = vec![Vec::new(); target_partitions];
        for (i, file) in self.files.iter().enumerate() {
            let partitioned_file =
                PartitionedFile::new(file.inner().path.clone(), file.inner().size as u64);
            file_groups[i % target_partitions].push(partitioned_file);
        }
        file_groups.retain(|g| !g.is_empty());
        let file_groups: Vec<FileGroup> =
            file_groups.into_iter().map(FileGroup::new).collect();

        let table_schema = TableSchema::new(Arc::clone(&self.table_schema), vec![]);

        let file_factory =
            DefaultParquetFileReaderFactory::new(Arc::clone(&self.object_store));
        let source = Arc::new(
            ParquetSource::new(table_schema)
                .with_parquet_file_reader_factory(Arc::new(file_factory)),
        );
        let file_scan_config = FileScanConfigBuilder::new(self.table_uri.clone(), source)
            .with_file_groups(file_groups)
            .with_projection_indices(Some(projection_indices))?
            .build();

        let datasource = DataSourceExec::new(Arc::new(file_scan_config));

        Ok(Arc::new(datasource))
    }
}

#[async_trait]
impl DeltaQuadsStorageLogChangeset for LazyInsertionOnlyChangeset {
    fn version_range(&self) -> DeltaStorageLogVersionRange {
        self.version_range
    }

    async fn cleared_graphs(
        &self,
        _context: &ChangesetContext,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError> {
        Ok(None)
    }

    async fn removed_quads(
        &self,
        _context: &ChangesetContext,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError> {
        Ok(None)
    }

    async fn added_quads(
        &self,
        context: &ChangesetContext,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError> {
        let scan_plan = self.scan_all_files(state, vec![2, 3, 4, 5])?;

        let scan_plan_schema = scan_plan.schema();
        let mut sort_exprs = Vec::new();

        let components = if let Some(order) = &context.intended_sort_order {
            order.inner().to_vec()
        } else {
            vec![
                QuadComponent::GraphName,
                QuadComponent::Subject,
                QuadComponent::Predicate,
                QuadComponent::Object,
            ]
        };

        for component in &components {
            let name = component.column_name();
            let idx = scan_plan_schema.index_of(name)?;
            sort_exprs.push(PhysicalSortExpr {
                expr: Arc::new(Column::new(name, idx)) as Arc<dyn PhysicalExpr>,
                options: SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            });
        }

        let ordering = LexOrdering::new(sort_exprs).unwrap();
        let sort_plan = Arc::new(
            SortExec::new(ordering.clone(), scan_plan).with_preserve_partitioning(true),
        );
        let merged_sort_plan =
            Arc::new(SortPreservingMergeExec::new(ordering.clone(), sort_plan));

        let distinct_plan = Arc::new(SortedDistinctExec::new(
            merged_sort_plan,
            LexRequirement::from(ordering),
        )) as Arc<dyn ExecutionPlan>;

        let mut verify_indices = Vec::new();
        let agg_schema = distinct_plan.schema();
        for name in [COL_SUBJECT, COL_PREDICATE, COL_OBJECT] {
            verify_indices.push(agg_schema.index_of(name)?);
        }

        let verified_plan =
            Arc::new(VerifyNotNullExec::try_new(distinct_plan, verify_indices)?);
        Ok(Some(verified_plan))
    }

    /// The added quads may implicitly create new graphs.
    async fn added_named_graphs(
        &self,
        _context: &ChangesetContext,
        state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError> {
        let scan_plan = self.scan_all_files(state, vec![2])?;
        let filtered = FilterExec::try_new(
            is_not_null(col(COL_GRAPH, scan_plan.schema().as_ref())?)?,
            scan_plan,
        )?;

        let scan_plan_schema = filtered.schema();
        let sort_exprs = vec![PhysicalSortExpr {
            expr: Arc::new(Column::new(
                COL_GRAPH,
                scan_plan_schema.index_of(COL_GRAPH)?,
            )) as Arc<dyn PhysicalExpr>,
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        }];
        let ordering = LexOrdering::new(sort_exprs).unwrap();
        let sort_plan = Arc::new(
            SortExec::new(ordering.clone(), Arc::new(filtered))
                .with_preserve_partitioning(true),
        ) as Arc<dyn ExecutionPlan>;
        let merged_sort_plan =
            Arc::new(SortPreservingMergeExec::new(ordering.clone(), sort_plan));

        let distinct_plan = Arc::new(SortedDistinctExec::new(
            merged_sort_plan,
            LexRequirement::from(ordering),
        )) as Arc<dyn ExecutionPlan>;

        Ok(Some(distinct_plan))
    }

    async fn dropped_named_graphs(
        &self,
        _context: &ChangesetContext,
        _state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>, DeltaQuadsStorageError> {
        Ok(None)
    }

    async fn as_eager_changeset(
        &self,
        state: &SessionState,
    ) -> Result<EagerChangeset, DeltaQuadsStorageError> {
        let operations = self
            .added_quads(&ChangesetContext::default(), state)
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
    use crate::delta::DeltaQuadsStorageBuilder;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::SessionContext;
    use rdf_fusion_common::{GraphName, NamedNode, Quad};
    use rdf_fusion_encoding::quads_to_plain_term_dataframe;
    use rdf_fusion_extensions::storage::QuadStorage;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_as_eager_changeset_conversion() -> Result<(), Box<dyn std::error::Error>>
    {
        let storage = Arc::new(DeltaQuadsStorageBuilder::new().build().await?);
        let context = SessionContext::new();
        let state = context.state();

        let txn = storage.begin_transaction(&state).await?;
        txn.insert(quads_to_plain_term_dataframe(
            &context,
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

        let added_quads = eager
            .added_quads(&ChangesetContext::default(), &state)
            .await
            .unwrap()
            .unwrap();
        let result = collect(added_quads, state.task_ctx()).await?;
        assert_eq!(result.iter().map(|rb| rb.num_rows()).sum::<usize>(), 1);
        Ok(())
    }
}
