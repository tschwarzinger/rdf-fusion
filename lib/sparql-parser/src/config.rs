use rdf_fusion_common::sparql::QueryDataset;
use rdf_fusion_common::{DateTime, Iri};

/// Configuration passed to [`crate::SparqlParser`] when parsing a query or update.
#[derive(Debug, Clone)]
pub struct ParserConfig {
    now: DateTime,
    default_base_iri: Option<Iri<String>>,
    default_dataset: Option<QueryDataset>,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ParserConfig {
    /// Creates a new [`ParserConfig`] with default values.
    pub fn builder() -> ParserConfigBuilder {
        ParserConfigBuilder::default()
    }

    /// Returns now.
    pub fn now(&self) -> DateTime {
        self.now
    }

    /// Provides a reference to the default base IRI.
    pub fn default_base_iri(&self) -> Option<&Iri<String>> {
        self.default_base_iri.as_ref()
    }

    /// Returns a reference to the default dataset used when parsing queries.
    pub fn default_dataset(&self) -> Option<&QueryDataset> {
        self.default_dataset.as_ref()
    }
}

/// A Builder for [`ParserConfig`] to construct it ergonomically.
///
/// The builder avoids that, for example, [`DateTime::now`] must be called if it's overridden
/// anyway by the user.
#[derive(Default)]
pub struct ParserConfigBuilder {
    now: Option<DateTime>,
    default_dataset: Option<QueryDataset>,
    default_base_iri: Option<Iri<String>>,
}

impl ParserConfigBuilder {
    /// Creates a new, empty [`ParserConfigBuilder`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the `now` time for the query.
    pub fn with_now(mut self, now: DateTime) -> Self {
        self.now = Some(now);
        self
    }

    /// Sets the queried dataset.
    pub fn with_default_dataset(mut self, dataset: Option<QueryDataset>) -> Self {
        self.default_dataset = dataset;
        self
    }

    /// Sets the base IRI of a query.
    pub fn with_base_iri(mut self, base_iri: Option<Iri<String>>) -> Self {
        self.default_base_iri = base_iri;
        self
    }

    /// Builds the [`ParserConfig`] falling back to default values where none were provided.
    pub fn build(self) -> ParserConfig {
        ParserConfig {
            now: self.now.unwrap_or_else(DateTime::now),
            default_dataset: self.default_dataset,
            default_base_iri: self.default_base_iri,
        }
    }
}
