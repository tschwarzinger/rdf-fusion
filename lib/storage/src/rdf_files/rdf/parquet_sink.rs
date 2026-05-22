use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// A [`DataSink`] that wraps a [`ParquetSink`].
///
/// TODO: validate schema
#[derive(Debug)]
pub struct RdfParquetDataSink {
    inner: ParquetSink,
    schema: SchemaRef,
}

impl RdfParquetDataSink {
    /// Creates a new [`RdfParquetDataSink`].
    pub fn new(inner: ParquetSink, schema: SchemaRef) -> Self {
        Self { inner, schema }
    }
}

impl DisplayAs for RdfParquetDataSink {
    fn fmt_as(
        &self,
        t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        self.inner.fmt_as(t, f)
    }
}

#[async_trait]
impl DataSink for RdfParquetDataSink {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    async fn write_all(
        &self,
        data: SendableRecordBatchStream,
        context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        self.inner.write_all(data, context).await
    }
}
