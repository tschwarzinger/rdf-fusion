use oxrdf::{GraphName, NamedOrBlankNode};

/// A SPARQL [query dataset specification](https://www.w3.org/TR/sparql11-query/#specifyingDataset)
#[derive(Eq, PartialEq, Debug, Clone, Hash, Default)]
pub struct QueryDataset {
    default: Option<Vec<GraphName>>,
    named: Option<Vec<NamedOrBlankNode>>,
}

impl QueryDataset {
    /// Creates a new [`QueryDataset`].
    pub fn new(
        default: Option<Vec<GraphName>>,
        named: Option<Vec<NamedOrBlankNode>>,
    ) -> Self {
        Self { default, named }
    }

    /// Builds a [`QueryDataset`] from a SPARQL algebra dataset specification.
    ///
    /// If the given specification is `None` (i.e. the query does not specify `FROM`/`FROM NAMED`),
    /// the default dataset is used (the store default graph plus all named graphs).
    pub fn from_algebra(inner: &Option<crate::sparql::algebra::QueryDataset>) -> Self {
        if let Some(inner) = inner {
            Self {
                default: Some(inner.default.iter().map(|g| g.clone().into()).collect()),
                named: inner
                    .named
                    .as_ref()
                    .map(|named| named.iter().map(|g| g.clone().into()).collect()),
            }
        } else {
            Self {
                default: Some(vec![GraphName::DefaultGraph]),
                named: None,
            }
        }
    }

    /// Checks if this dataset specification is the default one
    /// (i.e. the default graph is the store default graph and all the store named graphs are available)
    ///
    /// ```
    /// use rdf_fusion_common::sparql::QueryDataset;
    /// use rdf_fusion_common::GraphName;
    ///
    /// let mut dataset = QueryDataset::new(Some(vec![GraphName::DefaultGraph]), None);
    /// assert!(dataset.is_default_dataset());
    ///
    /// let named_node = rdf_fusion_common::NamedNode::new("http://example.com")?.into();
    /// dataset.set_default_graph(vec![named_node]);
    /// assert!(!dataset.is_default_dataset());
    ///
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn is_default_dataset(&self) -> bool {
        self.default
            .as_ref()
            .is_some_and(|t| t == &[GraphName::DefaultGraph])
            && self.named.is_none()
    }

    /// Returns the list of the store graphs that are available to the query as the default graph or `None` if the union of all graphs is used as the default graph
    /// This list is by default only the store default graph
    pub fn default_graph_graphs(&self) -> Option<&[GraphName]> {
        self.default.as_deref()
    }

    /// Sets if the default graph for the query should be the union of all the graphs in the queried store
    pub fn set_default_graph_as_union(&mut self) {
        self.default = None;
    }

    /// Sets the list of graphs the query should consider as being part of the default graph.
    ///
    /// By default only the store default graph is considered.
    /// ```
    /// use rdf_fusion_common::sparql::QueryDataset;
    /// use rdf_fusion_common::NamedNode;
    ///
    /// let mut dataset = QueryDataset::default();
    /// let default = vec![NamedNode::new("http://example.com")?.into()];
    /// dataset.set_default_graph(default.clone());
    /// assert_eq!(
    ///     dataset.default_graph_graphs(),
    ///     Some(default.as_slice())
    /// );
    ///
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn set_default_graph(&mut self, graphs: Vec<GraphName>) {
        self.default = Some(graphs)
    }

    /// Returns the list of the available named graphs for the query or `None` if all graphs are available
    pub fn available_named_graphs(&self) -> Option<&[NamedOrBlankNode]> {
        self.named.as_deref()
    }

    /// Sets the list of allowed named graphs in the query.
    ///
    /// ```
    /// use rdf_fusion_common::sparql::QueryDataset;
    /// use rdf_fusion_common::NamedNode;
    ///
    /// let mut dataset = QueryDataset::default();
    /// let named = vec![NamedNode::new("http://example.com")?.into()];
    /// dataset.set_available_named_graphs(named.clone());
    /// assert_eq!(
    ///     dataset.available_named_graphs(),
    ///     Some(named.as_slice())
    /// );
    ///
    /// # Ok::<_, Box<dyn std::error::Error>>(())
    /// ```
    pub fn set_available_named_graphs(&mut self, named_graphs: Vec<NamedOrBlankNode>) {
        self.named = Some(named_graphs);
    }
}
