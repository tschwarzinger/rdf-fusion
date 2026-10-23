use crate::parquet::RdfFusionParquetWriterProperties;
use crate::rdf_files::{ParseRdfFileNode, RdfFileScanOptions};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::exec_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::parquet::errors::ParquetError;
use datafusion::prelude::*;
use futures::StreamExt;
use object_store::buffered::BufWriter;
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

        let max_rows_per_file = ctx
            .state()
            .config_options()
            .execution
            .soft_max_rows_per_output_file
            .get();
        let mut stream = df.execute_stream().await?;
        let schema = stream.schema();

        let properties = RdfFusionParquetWriterProperties::new(self.encoding.clone())
            .with_sort_order(self.sort_order.clone());
        let arrow_properties = properties.to_arrow();

        // A URL that ends with a `/` is treated as a directory and the quads are split into
        // multiple files (the same convention as for reading, see `ListingTableUrl`). If a sort
        // order is configured, consecutive files only overlap at their boundaries, which allows
        // pruning entire files. Otherwise, a single file is written.
        let single_file = !output_url.path().ends_with('/');
        let mut file_index = 0;
        let mut create_writer = || {
            let file_path = if single_file {
                path.clone()
            } else {
                path.clone().join(format!("part-{file_index:05}.parquet"))
            };
            file_index += 1;
            AsyncArrowWriter::try_new(
                BufWriter::new(Arc::clone(&object_store), file_path),
                Arc::clone(&schema),
                Some(arrow_properties.clone()),
            )
        };

        let mut parquet_writer = create_writer()?;
        let mut rows_in_file = 0;
        while let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() == 0 {
                continue;
            }

            // The limit is approximate, as batches are never split (similar to DataFusion's
            // `soft_max_rows_per_output_file`).
            if !single_file && rows_in_file >= max_rows_per_file {
                parquet_writer.close().await?;
                parquet_writer = create_writer()?;
                rows_in_file = 0;
            }

            parquet_writer.write(&batch).await?;
            rows_in_file += batch.num_rows();
        }

        parquet_writer.close().await?;

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
