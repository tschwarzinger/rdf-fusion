mod exec;
mod logical_node;
mod parquet_sink;
mod planner;
mod sink;

pub use exec::*;
pub use logical_node::*;
pub use parquet_sink::*;
pub use planner::*;
pub use sink::*;

use oxrdfio::{RdfFormat, RdfParser, TokioAsyncReaderQuadParser};
use rdf_fusion_common::{GraphName, Iri, IriParseError};
use std::cmp::Ordering;
use tokio::io::AsyncRead;
use url::Url;

/// A source for RDF data.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RdfFileSourceConfig {
    /// The URL of the RDF data.
    pub url: Url,
    /// The format of the RDF data.
    pub format: RdfFormat,
}

impl PartialOrd for RdfFileSourceConfig {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RdfFileSourceConfig {
    fn cmp(&self, other: &Self) -> Ordering {
        self.url.cmp(&other.url).then_with(|| {
            format!("{:?}", self.format).cmp(&format!("{:?}", other.format))
        })
    }
}

/// Options for scanning RDF files.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RdfFileScanOptions {
    /// The rdf format.
    pub format: RdfFormat,
    /// The base IRI for the parser.
    pub base_iri: Option<Iri<String>>,
    /// Whether to rename blank nodes.
    pub rename_blank_nodes: bool,
    /// The default graph for the parser.
    pub default_graph: Option<GraphName>,
    /// Whether to allow named graphs.
    pub without_named_graphs: bool,
}

impl PartialOrd for RdfFileScanOptions {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for RdfFileScanOptions {
    fn cmp(&self, other: &Self) -> Ordering {
        format!("{:?}", self.format)
            .cmp(&format!("{:?}", other.format))
            .then_with(|| {
                self.base_iri
                    .as_ref()
                    .map(|i| i.as_str())
                    .cmp(&other.base_iri.as_ref().map(|i| i.as_str()))
            })
            .then_with(|| self.rename_blank_nodes.cmp(&other.rename_blank_nodes))
            .then_with(|| {
                self.default_graph
                    .as_ref()
                    .map(|g| g.to_string())
                    .cmp(&other.default_graph.as_ref().map(|g| g.to_string()))
            })
            .then_with(|| self.without_named_graphs.cmp(&other.without_named_graphs))
    }
}

impl RdfFileScanOptions {
    /// Creates a new [`TokioAsyncReaderQuadParser`] from the given `reader` and the current options.
    pub fn create_parser<R: AsyncRead + Unpin + Send + 'static>(
        &self,
        reader: R,
    ) -> TokioAsyncReaderQuadParser<R> {
        let mut parser = RdfParser::from_format(self.format);
        if let Some(base_iri) = &self.base_iri {
            parser = parser
                .with_base_iri(base_iri.as_str())
                .expect("Invalid base IRI");
        }
        if self.rename_blank_nodes {
            parser = parser.rename_blank_nodes();
        }
        if self.without_named_graphs {
            parser = parser.without_named_graphs();
        }
        if let Some(default_graph) = &self.default_graph {
            parser = parser.with_default_graph(default_graph.clone());
        }
        parser.for_tokio_async_reader(reader)
    }

    /// Creates a new [`RdfFileScanOptions`] for the given format.
    pub fn with_format(format: RdfFormat) -> Self {
        Self {
            format,
            base_iri: None,
            rename_blank_nodes: false,
            default_graph: None,
            without_named_graphs: false,
        }
    }

    /// Sets whether blank nodes should be renamed.
    pub fn with_rename_blank_nodes(mut self, rename_blank_nodes: bool) -> Self {
        self.rename_blank_nodes = rename_blank_nodes;
        self
    }

    /// Sets the base IRI for the parser.
    pub fn with_base_iri(
        mut self,
        base_iri: impl Into<String>,
    ) -> Result<Self, IriParseError> {
        let base_iri = Iri::parse(base_iri.into())?;
        self.base_iri = Some(base_iri);
        Ok(self)
    }

    /// Sets the default graph for the parser.
    pub fn with_default_graph(mut self, default_graph: impl Into<GraphName>) -> Self {
        self.default_graph = Some(default_graph.into());
        self
    }

    /// Sets whether named graphs are allowed.
    pub fn without_named_graphs(mut self, without_named_graphs: bool) -> Self {
        self.without_named_graphs = without_named_graphs;
        self
    }
}
