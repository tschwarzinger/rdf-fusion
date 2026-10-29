//! Contains implementations related to querying RDF data (e.g., Turtle).

mod blank_nodes;
mod exec;
mod line_range_reader;
mod logical_node;
mod planner;
mod sink;

pub use blank_nodes::*;
pub use exec::*;
pub use line_range_reader::*;
pub use logical_node::*;
pub use planner::*;
pub use sink::*;
use std::cmp::Ordering;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::exec_err;
use oxrdfio::{RdfFormat, RdfParser, TokioAsyncReaderQuadParser};
use rdf_fusion_common::quads::COL_SUBJECT;
use rdf_fusion_common::{DFResult, GraphName, Iri, IriParseError};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{QuadStorageEncoding, TermEncoding};
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
    /// Renames the blank nodes, if present.
    ///
    /// The renaming is not done by the parser itself but by the [`RdfParserExec`], such that a
    /// file that is parsed in multiple chunks is renamed consistently. The renamer (and hence its
    /// seed) is part of the options, such that every physical plan of the same scan renames the
    /// blank nodes consistently (e.g., when a file is loaded into multiple quad tables).
    pub blank_node_renamer: Option<BlankNodeRenamer>,
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
            .then_with(|| self.blank_node_renamer.cmp(&other.blank_node_renamer))
            .then_with(|| {
                format!("{:?}", self.default_graph)
                    .cmp(&format!("{:?}", other.default_graph))
            })
            .then_with(|| self.without_named_graphs.cmp(&other.without_named_graphs))
    }
}

impl RdfFileScanOptions {
    /// Creates a new [`RdfFileScanOptions`] for the given format.
    pub fn with_format(format: RdfFormat) -> Self {
        Self {
            format,
            base_iri: None,
            blank_node_renamer: None,
            default_graph: None,
            without_named_graphs: false,
        }
    }

    /// Sets whether blank nodes should be renamed. If so, a [`BlankNodeRenamer`] with a random
    /// seed is created.
    pub fn with_rename_blank_nodes(mut self, rename_blank_nodes: bool) -> Self {
        self.blank_node_renamer = rename_blank_nodes.then(BlankNodeRenamer::new_random);
        self
    }

    /// Sets the base IRI for the parser.
    pub fn with_base_iri(
        mut self,
        base_iri: impl Into<String>,
    ) -> Result<Self, IriParseError> {
        let base_iri_str = base_iri.into();
        let _ = rdf_fusion_common::NamedNode::new(&base_iri_str)?;
        let base_iri = Iri::parse(base_iri_str).unwrap();
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

    /// Creates a new [`TokioAsyncReaderQuadParser`] from the given `reader` and the current options.
    ///
    /// Blank nodes are not renamed by the parser (see [`Self::blank_node_renamer`]).
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
        if self.without_named_graphs {
            parser = parser.without_named_graphs();
        }
        if let Some(default_graph) = &self.default_graph {
            parser = parser.with_default_graph(default_graph.clone());
        }
        parser.for_tokio_async_reader(reader)
    }
}

/// Detects the encoding from the given schema.
pub fn detect_encoding_from_schema(schema: &SchemaRef) -> DFResult<QuadStorageEncoding> {
    let subject_field = schema.field_with_name(COL_SUBJECT)?;
    let dt = subject_field.data_type();
    if dt == PLAIN_TERM_ENCODING.data_type() {
        Ok(QuadStorageEncoding::PlainTerm)
    } else if dt == STRING_ENCODING.data_type() {
        Ok(QuadStorageEncoding::String)
    } else {
        exec_err!("Unsupported encoding data type: {:?}", dt)
    }
}
