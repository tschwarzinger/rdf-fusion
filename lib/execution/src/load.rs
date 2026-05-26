use crate::RdfFusionContext;
use datafusion::arrow::array::RecordBatch;
use datafusion::common::exec_datafusion_err;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::*;
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use object_store::path::Path;
use rdf_fusion_common::{RdfFormat, RdfInput, RdfInputSource, RdfSortOrder};
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName};
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
use rdf_fusion_storage::rdf_files::{ParseRdfFileNode, RdfFileScanOptions};
use std::sync::Arc;
use url::Url;

/// A loader that converts RDF files into Parquet format.
pub struct RdfParquetLoader {
    context: RdfFusionContext,
    encoding: QuadStorageEncoding,
}

impl RdfParquetLoader {
    /// Creates a new [`RdfParquetLoader`].
    pub fn try_new(
        context: RdfFusionContext,
        encoding: QuadStorageEncodingName,
    ) -> Result<Self, RdfParquetLoaderCreationError> {
        let encoding = match encoding {
            QuadStorageEncodingName::PlainTerm => QuadStorageEncoding::PlainTerm,
            QuadStorageEncodingName::ObjectId => {
                return Err(RdfParquetLoaderCreationError::UnsupportedEncoding(encoding));
            }
            QuadStorageEncodingName::String => QuadStorageEncoding::String,
        };
        Ok(Self { context, encoding })
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
    pub async fn load_many(
        &self,
        inputs: Vec<RdfInput>,
        output_url: Url,
    ) -> Result<(), RdfParquetLoadingError> {
        let object_store_url = output_url.as_object_store_url();
        let object_store = self
            .context
            .session_context()
            .runtime_env()
            .object_store(&object_store_url)?;
        let path = Path::from(output_url.path());

        // Check if destination exists and is not empty
        let mut list_stream = object_store.list(Some(&path));
        if let Some(item) = futures::StreamExt::next(&mut list_stream).await {
            item.map_err(|e| DataFusionError::External(Box::new(e)))?;
            return Err(RdfParquetLoadingError::AlreadyExists(output_url.clone()));
        }

        let ctx = self.context.session_context();

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

        let write_options =
            DataFrameWriteOptions::default().with_single_file_output(true);

        let rdf_fusion_options = self.context.options().clone();
        let (df, write_options) = match &rdf_fusion_options.storage.parquet.sort_order {
            None => (df, write_options),
            Some(RdfSortOrder::NativeOrder(sort_order)) => {
                let sort_expr = sort_order
                    .iter()
                    .map(|c| col(c.column_name()).sort(true, true))
                    .collect::<Vec<_>>();
                (df, write_options.with_sort_by(sort_expr))
            }
            Some(sort_order) => {
                let (state, plan) = df.into_parts();
                let builder =
                    RdfFusionLogicalPlanBuilderContext::new(self.context.create_view())
                        .create(Arc::new(plan))
                        .apply_rdf_sort_order(sort_order)?
                        .build()?;
                df = DataFrame::new(state, builder);
                (df, write_options)
            }
        };

        df.write_parquet(output_url.as_str(), write_options, None)
            .await?;

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
}

#[derive(Debug, thiserror::Error)]
#[error("Could not create RDF Parquet loader: {0}")]
pub enum RdfParquetLoaderCreationError {
    #[error("Unsupported encoding: {0}")]
    UnsupportedEncoding(QuadStorageEncodingName),
}
