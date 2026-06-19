use crate::delta::error::DeltaQuadStorageError;
use crate::delta::index::snapshot::DeltaQuadStorageIndexSnapshot;
use crate::index::IndexComponents;
use datafusion::dataframe::DataFrame;
use datafusion::execution::SessionState;
use datafusion::logical_expr::col;
use datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::DeltaTableProvider;
use std::sync::Arc;

/// Implements validation of a single [`DeltaQuadStorageIndexSnapshot`].
pub async fn validate_index(
    state: &SessionState,
    snapshot: &DeltaQuadStorageIndexSnapshot,
) -> Result<(), DeltaQuadStorageError> {
    let provider = DeltaTableProvider::try_new(
        snapshot.eager_snapshot().clone(),
        Arc::clone(snapshot.log_store()),
        Default::default(),
    )?;

    let context = SessionContext::new_with_state(state.clone());
    let df = context.read_table(Arc::new(provider))?;
    check_duplicates(df.clone(), &snapshot.components()).await?;

    return Ok(());

    // Validate No Duplicates by comparing the total count of rows with the distinct count.
    async fn check_duplicates(
        df: DataFrame,
        components: &IndexComponents,
    ) -> Result<(), DeltaQuadStorageError> {
        let total_count = df.clone().count().await?;

        let component_cols: Vec<_> = components
            .inner()
            .iter()
            .map(|c| col(c.column_name()))
            .collect();

        let distinct_count = df.select(component_cols)?.distinct()?.count().await?;

        if total_count != distinct_count {
            return Err(DeltaQuadStorageError::Other(format!(
                "Validation failed: Index contains duplicates. Total: {total_count}, Distinct: {distinct_count}"
            )));
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::prelude::SessionContext;
    use deltalake::StructField;
    use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
    use deltalake::kernel::transaction::TableReference;
    use deltalake::operations::create::CreateBuilder;
    use rdf_fusion_encoding::QuadStorageEncoding;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_validate_index_success_no_duplicates() {
        let ctx = setup_context();
        let schema = test_schema();

        let batch = create_record_batch_with_default_graph(
            schema,
            vec!["s1", "s2", "s3"],
            vec!["p1", "p1", "p2"],
            vec!["o1", "o2", "o3"],
        );

        let snapshot = create_snapshot(batch, IndexComponents::GSPO).await;
        let result = validate_index(&ctx.state(), &snapshot).await;

        assert!(
            result.is_ok(),
            "Validation should pass when there are no duplicates"
        );
    }

    #[tokio::test]
    async fn test_validate_index_fails_with_duplicates() {
        let ctx = setup_context();
        let schema = test_schema();

        let batch = create_record_batch_with_default_graph(
            schema,
            vec!["s1", "s1", "s3"],
            vec!["p1", "p1", "p2"],
            vec!["o1", "o1", "o3"],
        );

        let snapshot = create_snapshot(batch, IndexComponents::GSPO).await;
        let result = validate_index(&ctx.state(), &snapshot).await;

        assert_eq!(
            result.unwrap_err().to_string(),
            "Validation failed: Index contains duplicates. Total: 3, Distinct: 2"
        );
    }

    /// Helper to create a test SessionContext
    fn setup_context() -> SessionContext {
        SessionContext::new()
    }

    /// Creates an in-memory Delta table from the provided RecordBatch and wraps it in a
    /// [`DeltaQuadStorageIndexSnapshot`].
    async fn create_snapshot(
        batch: RecordBatch,
        components: IndexComponents,
    ) -> DeltaQuadStorageIndexSnapshot {
        let fields = batch
            .schema()
            .fields()
            .iter()
            .map(|f| StructField::try_from_arrow(f.as_ref()).unwrap())
            .collect::<Vec<_>>();

        let table = CreateBuilder::new()
            .with_location("memory://")
            .with_columns(fields)
            .await
            .expect("Failed to create table")
            .write(vec![batch])
            .await
            .expect("Failed to write to in-memory Delta table");

        let log_store = table.log_store();
        let snapshot = table
            .snapshot()
            .expect("Cannot take snapshot")
            .eager_snapshot()
            .clone();

        // The encoding doesn't match the given input. This may break in the future.
        DeltaQuadStorageIndexSnapshot::new(
            QuadStorageEncoding::PlainTerm,
            snapshot,
            log_store,
            Arc::new(vec![]),
            components,
            0,
        )
    }

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("graph", DataType::Utf8, true),
            Field::new("subject", DataType::Utf8, false),
            Field::new("predicate", DataType::Utf8, false),
            Field::new("object", DataType::Utf8, false),
        ]))
    }

    fn create_record_batch_with_default_graph(
        schema: Arc<Schema>,
        subjects: Vec<&str>,
        predicates: Vec<&str>,
        objects: Vec<&str>,
    ) -> RecordBatch {
        let graph = StringArray::new_null(3);
        let subject = StringArray::from(subjects);
        let predicate = StringArray::from(predicates);
        let object = StringArray::from(objects);

        RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(graph),
                Arc::new(subject),
                Arc::new(predicate),
                Arc::new(object),
            ],
        )
        .unwrap()
    }
}
