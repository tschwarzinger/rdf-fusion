use rdf_fusion_common::sparql::QueryDataset;
use rdf_fusion_common::{DateTime, Iri};
use rdf_fusion_encoding::EncodingName;

/// Configuration passed to [`crate::SparqlParser`] when parsing a query or update.
#[derive(Debug, Clone)]
pub struct ParserOptions {
    now: DateTime,
    default_base_iri: Option<Iri<String>>,
    default_dataset: Option<QueryDataset>,
    output_encoding_name: Option<EncodingName>,
}

impl Default for ParserOptions {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl ParserOptions {
    /// Creates a new [`ParserOptions`] with default values.
    pub fn builder() -> ParserOptionsBuilder {
        ParserOptionsBuilder::default()
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

    /// Returns the output encoding name.
    pub fn output_encoding_name(&self) -> Option<EncodingName> {
        self.output_encoding_name
    }
}

/// A Builder for [`ParserOptions`] to construct it ergonomically.
///
/// The builder avoids that, for example, [`DateTime::now`] must be called if it's overridden
/// anyway by the user.
#[derive(Default)]
pub struct ParserOptionsBuilder {
    now: Option<DateTime>,
    default_dataset: Option<QueryDataset>,
    default_base_iri: Option<Iri<String>>,
    output_encoding_name: Option<EncodingName>,
}

impl ParserOptionsBuilder {
    /// Creates a new, empty [`ParserOptionsBuilder`].
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

    /// Sets the output encoding for the query.
    pub fn with_output_encoding_name(
        mut self,
        output_encoding_name: Option<EncodingName>,
    ) -> Self {
        self.output_encoding_name = output_encoding_name;
        self
    }

    /// Builds the [`ParserOptions`] falling back to default values where none were provided.
    pub fn build(self) -> ParserOptions {
        ParserOptions {
            now: self.now.unwrap_or_else(DateTime::now),
            default_dataset: self.default_dataset,
            default_base_iri: self.default_base_iri,
            output_encoding_name: self.output_encoding_name,
        }
    }
}
