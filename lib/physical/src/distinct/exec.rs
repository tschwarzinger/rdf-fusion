use datafusion::arrow::compute::interleave;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::error::Result;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{
    Distribution, LexRequirement, OrderingRequirements, Partitioning,
};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, RecordBatchStream,
    SendableRecordBatchStream,
};
use futures::{Stream, StreamExt};
use itertools::Itertools;
use std::any::Any;
use std::fmt::Debug;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct SortedDistinctExec {
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    sort_exprs: LexRequirement,
}

impl SortedDistinctExec {
    pub fn new(input: Arc<dyn ExecutionPlan>, sort_exprs: LexRequirement) -> Self {
        let properties = PlanProperties::clone(input.properties())
            .with_partitioning(Partitioning::UnknownPartitioning(1));
        Self {
            input,
            properties: Arc::new(properties),
            metrics: ExecutionPlanMetricsSet::new(),
            sort_exprs,
        }
    }
}

impl DisplayAs for SortedDistinctExec {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(
            f,
            "SortedDistinctExec: [{}]",
            self.properties
                .eq_properties
                .output_ordering()
                .map(|ord| ord.iter().format(", ").to_string())
                .unwrap_or_else(|| "Inner plan not yet sorted".to_string())
        )
    }
}

impl ExecutionPlan for SortedDistinctExec {
    fn name(&self) -> &str {
        "SortedDistinctExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn required_input_distribution(&self) -> Vec<Distribution> {
        vec![Distribution::SinglePartition]
    }

    fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>> {
        vec![Some(OrderingRequirements::Hard(vec![
            self.sort_exprs.clone(),
        ]))]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let child = Arc::clone(&children[0]);
        let new_sort_exprs = if let Some(ordering) = child.properties().output_ordering()
        {
            let all_match = ordering.len() == self.sort_exprs.len()
                && self
                    .sort_exprs
                    .iter()
                    .all(|orig| ordering.iter().any(|new| orig.expr.eq(&new.expr)));
            if all_match {
                LexRequirement::from(ordering.clone())
            } else {
                self.sort_exprs.clone()
            }
        } else {
            self.sort_exprs.clone()
        };

        Ok(Arc::new(Self::new(child, new_sort_exprs)))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = input_stream.schema();

        let sort_fields = schema
            .fields()
            .iter()
            .map(|f| SortField::new(f.data_type().clone()))
            .collect::<Vec<_>>();

        let converter = RowConverter::new(sort_fields)?;

        Ok(Box::pin(SortedDistinctStream {
            input: input_stream,
            converter,
            last_row: None,
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

struct SortedDistinctStream {
    input: SendableRecordBatchStream,
    converter: RowConverter,
    last_row: Option<Vec<u8>>,
}

impl Stream for SortedDistinctStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        match futures::ready!(self.input.poll_next_unpin(cx)) {
            Some(Ok(batch)) => {
                if batch.num_rows() == 0 {
                    return Poll::Ready(Some(Ok(batch)));
                }

                let rows = match self.converter.convert_columns(batch.columns()) {
                    Ok(r) => r,
                    Err(e) => return Poll::Ready(Some(Err(e.into()))),
                };

                let mut has_duplicates = false;

                // Check against last row from previous batch
                let mut start_idx = 0;
                if let Some(ref last) = self.last_row {
                    if last.as_slice() == rows.row(0).as_ref() {
                        has_duplicates = true;
                        start_idx = 1;
                    }
                }

                // Check for duplicates within the current batch
                if !has_duplicates {
                    for i in 1..batch.num_rows() {
                        if rows.row(i) == rows.row(i - 1) {
                            has_duplicates = true;
                            break;
                        }
                    }
                }

                self.last_row = Some(rows.row(batch.num_rows() - 1).as_ref().to_vec());

                if !has_duplicates {
                    // Fast path
                    return Poll::Ready(Some(Ok(batch)));
                }

                // Slow path: collect quad_tables of unique rows
                let mut unique_indices = Vec::with_capacity(batch.num_rows());
                if start_idx == 0 {
                    unique_indices.push((0, 0));
                }
                for i in 1..batch.num_rows() {
                    if rows.row(i) != rows.row(i - 1) {
                        unique_indices.push((0, i));
                    }
                }

                if unique_indices.is_empty() {
                    // All rows were duplicates of the last row from the previous batch
                    return Poll::Ready(Some(Ok(RecordBatch::new_empty(batch.schema()))));
                }

                if start_idx == 1 && unique_indices.len() == batch.num_rows() - 1 {
                    // Fast path 2: only the first row was a duplicate, rest are unique
                    return Poll::Ready(Some(Ok(batch.slice(1, batch.num_rows() - 1))));
                }

                let new_columns = batch
                    .columns()
                    .iter()
                    .map(|col| {
                        interleave(&[col.as_ref()], &unique_indices).map_err(Into::into)
                    })
                    .collect::<Result<Vec<_>>>();

                match new_columns {
                    Ok(cols) => {
                        let new_batch =
                            RecordBatch::try_new(Arc::clone(&batch.schema()), cols)?;
                        Poll::Ready(Some(Ok(new_batch)))
                    }
                    Err(e) => Poll::Ready(Some(Err(e))),
                }
            }
            other => Poll::Ready(other),
        }
    }
}

impl RecordBatchStream for SortedDistinctStream {
    fn schema(&self) -> datafusion::arrow::datatypes::SchemaRef {
        self.input.schema()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;

    fn create_batch(values: Vec<i32>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    async fn run_distinct(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
        let schema = if let Some(batch) = batches.first() {
            batch.schema()
        } else {
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]))
        };

        let sort_fields = vec![SortField::new(DataType::Int32)];
        let converter = RowConverter::new(sort_fields).unwrap();

        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        let input = Box::pin(RecordBatchStreamAdapter::new(schema, stream));

        let mut distinct_stream = SortedDistinctStream {
            input,
            converter,
            last_row: None,
        };

        let mut results = Vec::new();
        while let Some(batch) = distinct_stream.next().await {
            results.push(batch?);
        }
        Ok(results)
    }

    #[tokio::test]
    async fn test_only_unique_values() -> Result<()> {
        let batch = create_batch(vec![1, 2, 3]);
        let results = run_distinct(vec![batch]).await?;
        assert_eq!(results.len(), 1);
        let arr = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(arr.values(), &[1, 2, 3]);
        Ok(())
    }

    #[tokio::test]
    async fn test_duplicates_within_batch() -> Result<()> {
        let batch = create_batch(vec![1, 1, 2, 3, 3]);
        let results = run_distinct(vec![batch]).await?;
        assert_eq!(results.len(), 1);
        let arr = results[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(arr.values(), &[1, 2, 3]);
        Ok(())
    }

    #[tokio::test]
    async fn test_duplicates_across_batch() -> Result<()> {
        let batch1 = create_batch(vec![1, 1, 2]);
        let batch2 = create_batch(vec![2, 3, 3]);
        let results = run_distinct(vec![batch1, batch2]).await?;

        let mut all_values = Vec::new();
        for batch in results {
            let arr = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap();
            all_values.extend_from_slice(arr.values());
        }
        assert_eq!(all_values, vec![1, 2, 3]);
        Ok(())
    }
}
