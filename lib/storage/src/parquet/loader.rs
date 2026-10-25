use crate::parquet::RdfParquetWriterOptions;
use crate::rdf_files::{ParseRdfFileNode, RdfFileScanOptions};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::exec_datafusion_err;
use datafusion::datasource::file_format::parquet::ParquetSink;
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::datasource::physical_plan::{FileGroup, FileOutputMode, FileSinkConfig};
use datafusion::datasource::sink::DataSinkExec;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::parquet::errors::ParquetError;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::{ExecutionPlanProperties, collect};
use datafusion::prelude::*;
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::path::Path;
use rdf_fusion_common::{
    RdfFormat, RdfInput, RdfInputSource, RdfSortOrder, url_to_object_store_url,
};
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
use std::sync::Arc;
use url::Url;

/// A loader that converts RDF files into Parquet format.
pub struct RdfParquetLoader {
    session_context: SessionContext,
    context_view: RdfFusionContextView,
    encoding: QuadStorageEncoding,
    sort_order: Option<RdfSortOrder>,
}

impl RdfParquetLoader {
    /// Creates a new [`RdfParquetLoader`].
    pub fn try_new(
        session_context: SessionContext,
        context_view: RdfFusionContextView,
        encoding: QuadStorageEncodingName,
        sort_order: Option<RdfSortOrder>,
    ) -> Result<Self, RdfParquetLoaderCreationError> {
        let encoding = match encoding {
            QuadStorageEncodingName::PlainTerm => QuadStorageEncoding::PlainTerm,
            QuadStorageEncodingName::ObjectId => {
                return Err(RdfParquetLoaderCreationError::UnsupportedEncoding(encoding));
            }
            QuadStorageEncodingName::String => QuadStorageEncoding::String,
        };
        Ok(Self {
            session_context,
            context_view,
            encoding,
            sort_order,
        })
    }

    /// Loads the given RDF file into a Parquet database at the specified output URL.
    pub async fn load(
        &self,
        input: RdfInput,
        output_url: Url,
    ) -> Result<(), RdfParquetLoadingError> {
        self.load_many(vec![input], output_url).await
    }

    /// Loads the given RDF input into a Parquet database at the specified output URL.
    ///
    /// If the URL ends with a `/`, it is treated as a directory and the quads are split into
    /// multiple files of approximately `datafusion.execution.soft_max_rows_per_output_file` rows.
    /// Otherwise, a single file is written to the URL (which does not need a `.parquet`
    /// extension, e.g., for objects in IndexedDB).
    pub async fn load_many(
        &self,
        inputs: Vec<RdfInput>,
        output_url: Url,
    ) -> Result<(), RdfParquetLoadingError> {
        let object_store_url = url_to_object_store_url(&output_url)?;
        let object_store = self
            .session_context
            .runtime_env()
            .object_store(&object_store_url)?;
        let path = Path::from(output_url.path());

        // Check if destination exists and is not empty
        let mut list_stream = object_store.list(Some(&path));
        if let Some(item) = list_stream.next().await {
            item.map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Err(RdfParquetLoadingError::AlreadyExists(output_url.clone()));
        }

        let ctx = &self.session_context;

        let mut df: Option<DataFrame> = None;

        for input in inputs {
            let extension = input.url.path().split('.').next_back().unwrap_or_default();
            let format = RdfFormat::from_extension(extension).ok_or_else(|| {
                exec_datafusion_err!("Unknown RDF format for URL {}", input.url)
            })?;

            let options = RdfFileScanOptions::with_format(format)
                .with_default_graph(input.default_graph)
                .with_rename_blank_nodes(true)
                .with_base_iri(input.url.as_str())
                .expect("IRI is valid");
            let node = ParseRdfFileNode::new(
                RdfInputSource::from_url(input.url),
                options,
                self.encoding.quad_schema(),
            );

            let current_df = DataFrame::new(
                ctx.state(),
                LogicalPlan::Extension(datafusion::logical_expr::Extension {
                    node: Arc::new(node),
                }),
            );

            df = match df {
                Some(existing_df) => Some(existing_df.union(current_df)?),
                None => Some(current_df),
            };
        }

        let mut df = if let Some(df) = df {
            df
        } else {
            ctx.read_batch(RecordBatch::new_empty(Arc::clone(
                self.encoding.quad_schema().inner(),
            )))?
        };

        // Ensure quads are unique in the store
        df = df.distinct()?;

        let df = match &self.sort_order {
            None => df,
            Some(sort_order) => {
                let (state, plan) = df.into_parts();
                let builder =
                    RdfFusionLogicalPlanBuilderContext::new(self.context_view.clone())
                        .create(Arc::new(plan))
                        .apply_rdf_sort_order(sort_order)?
                        .build()?;
                DataFrame::new(state, builder)
            }
        };

        // A URL that ends with a `/` is treated as a directory and the sink splits the quads into
        // multiple files of approximately `datafusion.execution.soft_max_rows_per_output_file`
        // rows (the same convention as for reading, see `ListingTableUrl`). Otherwise, a single
        // file is written.
        let single_file = !output_url.path().ends_with('/');

        let (mut state, plan) = df.into_parts();
        if self.sort_order.is_some() {
            // Avoid redistributes the batches round-robin across multiple files.
            state
                .config_mut()
                .options_mut()
                .set("datafusion.execution.minimum_parallel_output_files", "1")?;
        }

        let mut physical_plan = state.create_physical_plan(&plan).await?;
        if physical_plan.output_partitioning().partition_count() > 1 {
            physical_plan = Arc::new(CoalescePartitionsExec::new(physical_plan));
        }
        let schema = physical_plan.schema();

        let writer_options = RdfParquetWriterOptions::try_new_for_sort_order(
            &state.default_table_options().parquet,
            &schema,
            self.sort_order.as_ref(),
        )?;
        let config = FileSinkConfig {
            original_url: output_url.to_string(),
            object_store_url,
            file_group: FileGroup::default(),
            table_paths: vec![ListingTableUrl::parse(output_url.as_str())?],
            output_schema: Arc::clone(&schema),
            table_partition_cols: vec![],
            insert_op: InsertOp::Append,
            keep_partition_by_columns: false,
            file_extension: "parquet".to_owned(),
            file_output_mode: if single_file {
                FileOutputMode::SingleFile
            } else {
                FileOutputMode::Directory
            },
        };
        let sink = Arc::new(
            ParquetSink::new(config, writer_options.table_parquet_options().clone())
                .with_sorting_columns(writer_options.sorting_columns()),
        );

        // The sink writes the files in separate tasks, such that producing the batches (e.g.,
        // merging the sorted runs) runs in parallel to encoding the Parquet files.
        let sink_exec = Arc::new(DataSinkExec::new(physical_plan, sink, None));
        collect(sink_exec, state.task_ctx()).await?;

        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Error while loading RDF data into Parquet file: {0}")]
pub enum RdfParquetLoadingError {
    #[error("Object already exists at {0}.")]
    AlreadyExists(Url),
    #[error(transparent)]
    DataFusion(#[from] DataFusionError),
    #[error(transparent)]
    Parquet(#[from] ParquetError),
}

#[derive(Debug, thiserror::Error)]
#[error("Could not create RDF Parquet loader: {0}")]
pub enum RdfParquetLoaderCreationError {
    #[error("Unsupported encoding: {0}")]
    UnsupportedEncoding(QuadStorageEncodingName),
}
