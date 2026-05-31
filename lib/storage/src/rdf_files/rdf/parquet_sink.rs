use crate::parquet::RdfFusionParquetWriterProperties;
use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::datasource::sink::DataSink;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::buffered::BufWriter;
use object_store::path::Path;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

/// A [`DataSink`] that writes RDF Parquet data.
#[derive(Debug)]
pub struct RdfParquetDataSink {
    object_store: Arc<dyn ObjectStore>,
    path: Path,
    properties: RdfFusionParquetWriterProperties,
    schema: SchemaRef,
}

impl RdfParquetDataSink {
    /// Creates a new [`RdfParquetDataSink`].
    pub fn new(
        object_store: Arc<dyn ObjectStore>,
        path: Path,
        properties: RdfFusionParquetWriterProperties,
        schema: SchemaRef,
    ) -> Self {
        Self {
            object_store,
            path,
            properties,
            schema,
        }
    }
}

impl DisplayAs for RdfParquetDataSink {
    fn fmt_as(
        &self,
        _t: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "RdfParquetDataSink(path={})", self.path)
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
        mut data: SendableRecordBatchStream,
        _context: &Arc<TaskContext>,
    ) -> datafusion::common::Result<u64> {
        let arrow_properties = self.properties.to_arrow();
        let mut writer = AsyncArrowWriter::try_new(
            BufWriter::new(Arc::clone(&self.object_store), self.path.clone()),
            Arc::clone(&self.schema),
            Some(arrow_properties),
        )?;

        let mut row_count = 0;
        while let Some(batch) = data.next().await {
            let batch = batch?;
            row_count += batch.num_rows() as u64;
            writer.write(&batch).await?;
        }

        writer.close().await?;
        Ok(row_count)
    }
}
