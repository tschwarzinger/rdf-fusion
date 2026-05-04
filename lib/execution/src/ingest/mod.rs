mod rdf_parser_table_provider;

use oxrdfio::RdfFormat;
use rdf_fusion_model::{GraphName, Iri, IriParseError};
pub use rdf_parser_table_provider::RdfParserTableProvider;

/// Options for the RDF parser.
#[derive(Debug, Clone)]
pub struct RdfParserOptions {
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

impl RdfParserOptions {
    /// Creates a new [`RdfParserOptions`] for the given format.
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
