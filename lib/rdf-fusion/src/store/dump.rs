use crate::error::SerializerError;
use crate::store::Store;
use datafusion::dataframe::DataFrame;
use datafusion::datasource::sink::DataSink;
use datafusion::logical_expr::col;
use datafusion::physical_plan::execute_stream;
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use object_store::path::Path;
use rdf_fusion_common::quads::{COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{GraphName, RdfDumpFormat, RdfSortOrder};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_storage::parquet::RdfFusionParquetWriterProperties;
use rdf_fusion_storage::rdf_files::{RdfFileDataSink, RdfParquetDataSink};
use std::sync::Arc;
use url::Url;

/// The encoding to use for dumping.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum DumpEncoding {
    /// Use the plain term encoding.
    #[default]
    PlainTerm,
    /// Use the string encoding.
    String,
}

/// Options for dumping a store.
#[derive(Debug, Default, Clone)]
pub struct RdfDumpOptions {
    graph: Option<GraphName>,
    sort_by: Option<RdfSortOrder>,
    triple_fallback: TripleFallbackStrategy,
    encoding: DumpEncoding,
}

impl RdfDumpOptions {
    /// The graph to dump. If `None`, the whole store is dumped.
    pub fn graph(&self) -> Option<&GraphName> {
        self.graph.as_ref()
    }

    /// The sort order.
    pub fn sort_by(&self) -> Option<&RdfSortOrder> {
        self.sort_by.as_ref()
    }

    /// The strategy to use when exporting quads to a triple-only format.
    pub fn triple_fallback(&self) -> TripleFallbackStrategy {
        self.triple_fallback
    }

    /// The encoding to use.
    pub fn encoding(&self) -> DumpEncoding {
        self.encoding
    }

    /// See [`Self::graph`]
    pub fn with_graph(mut self, graph: Option<GraphName>) -> Self {
        self.graph = graph;
        self
    }

    /// See [`Self::sort_by`]
    pub fn with_sort_by(mut self, sort_by: Option<RdfSortOrder>) -> Self {
        self.sort_by = sort_by;
        self
    }

    /// See [`Self::with_triple_fallback_strategy`]
    pub fn with_triple_fallback_strategy(
        mut self,
        triple_fallback: TripleFallbackStrategy,
    ) -> Self {
        self.triple_fallback = triple_fallback;
        self
    }

    /// See [`Self::encoding`]
    pub fn with_encoding(mut self, encoding: DumpEncoding) -> Self {
        self.encoding = encoding;
        self
    }
}

/// The strategy to use when exporting quads to a triple-only format.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TripleFallbackStrategy {
    /// Errors if a non-default graph is encountered.
    #[default]
    ErrorOnNonDefaultGraph,
    /// Ignores the graph column and deduplicates the triples.
    IgnoreGraph,
}

pub(crate) async fn dump_store(
    store: &Store,
    output_url: String,
    format: RdfDumpFormat,
    options: RdfDumpOptions,
) -> Result<(), SerializerError> {
    if let Some(sort_by) = &options.sort_by {
        sort_by
            .validate()
            .map_err(|err| SerializerError::Other(Box::new(err)))?;
    }

    let url = Url::parse(&output_url).map_err(|e| SerializerError::Other(Box::new(e)))?;

    let mut builder = store.context().quads_for_pattern_as_builder(
        options.graph.as_ref().map(|g| g.as_ref()),
        None,
        None,
        None,
    );

    if let Some(sort_order) = &options.sort_by {
        builder = builder.apply_rdf_sort_order(sort_order)?
    }

    builder = match options.encoding {
        DumpEncoding::PlainTerm => builder.with_plain_terms()?,
        DumpEncoding::String => {
            builder.with_encoding(rdf_fusion_encoding::EncodingName::String)?
        }
    };

    let mut df = DataFrame::new(
        store.context().session_context().state(),
        builder
            .build()
            .map_err(|e| SerializerError::Other(e.into()))?,
    );

    if !format.supports_datasets()
        && options.triple_fallback == TripleFallbackStrategy::IgnoreGraph
    {
        df = df
            .select(vec![col(COL_SUBJECT), col(COL_PREDICATE), col(COL_OBJECT)])?
            .distinct()?;
    }

    let (session, plan) = df.into_parts();
    let optimized_plan = session.optimize(&plan)?;
    let physical_plan = session.create_physical_plan(&optimized_plan).await?;

    let runtime_env = session.runtime_env();
    let object_store_url = url.as_object_store_url();
    let object_store = runtime_env.object_store(&object_store_url)?;

    let sink_schema = physical_plan.schema();
    let sink: Arc<dyn DataSink> = match format {
        RdfDumpFormat::Parquet => {
            let storage_encoding = match options.encoding {
                DumpEncoding::PlainTerm => QuadStorageEncoding::PlainTerm,
                DumpEncoding::String => QuadStorageEncoding::String,
            };
            let properties = RdfFusionParquetWriterProperties::new(storage_encoding)
                .with_sort_order(options.sort_by.clone());
            Arc::new(RdfParquetDataSink::new(
                Arc::clone(&object_store),
                Path::from(url.path()),
                properties,
                Arc::clone(&sink_schema),
            ))
        }
        RdfDumpFormat::Rdf(rdf_format) => Arc::new(RdfFileDataSink::new(
            object_store,
            Path::from(url.path()),
            rdf_format,
            Arc::clone(&sink_schema),
        )),
    };

    let stream = execute_stream(physical_plan, session.task_ctx())?;
    sink.write_all(stream, &session.task_ctx()).await?;

    Ok(())
}
