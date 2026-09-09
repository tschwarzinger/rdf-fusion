use crate::SparqlParseError;
use crate::ast;
use rdf_fusion_common::sparql::{GraphTarget, QueryDataset};
use rdf_fusion_common::vocab::xsd;
use rdf_fusion_common::{DateTime, Iri, Literal, NamedNode};
use std::collections::HashMap;

/// Holds the context for planning a SPARQL query.
#[derive(Debug, Clone)]
pub struct RewriterContext {
    now: DateTime,
    dataset: QueryDataset,
    base_iri: Option<Iri<String>>,
    prefixes: HashMap<String, Iri<String>>,
}

impl Default for RewriterContext {
    fn default() -> Self {
        Self::builder().build()
    }
}

impl RewriterContext {
    /// Returns a builder to construct a [`RewriterContext`] with sensible defaults.
    pub fn builder() -> RewriterContextBuilder {
        RewriterContextBuilder::new()
    }

    /// Transforms this context into a [`RewriterContextBuilder`] which allows modifying it.
    pub fn into_builder(self) -> RewriterContextBuilder {
        RewriterContextBuilder {
            now: Some(self.now),
            prefixes: self.prefixes,
            dataset: Some(self.dataset),
            base_iri: self.base_iri,
        }
    }

    /// Returns the now time for the query.
    pub fn now(&self) -> DateTime {
        self.now
    }

    /// Returns the queried dataset.
    pub fn dataset(&self) -> &QueryDataset {
        &self.dataset
    }

    /// Returns the base IRI of a query.
    pub fn base_iri(&self) -> Option<&Iri<String>> {
        self.base_iri.as_ref()
    }

    /// Resolves an IRI using the base iri.
    pub fn resolve_iri(&self, iri: &ast::Iri) -> Result<NamedNode, SparqlParseError> {
        match iri {
            ast::Iri::IriRef(r) => {
                let iriref = r.value.as_ref();
                if let Some(base) = &self.base_iri {
                    let resolved = base.resolve(iriref).map_err(|e| {
                        SparqlParseError::new_without_span(format!(
                            "Invalid IRI '{iriref}': {e}"
                        ))
                    })?;
                    Ok(NamedNode::new_unchecked(resolved.as_str().to_string()))
                } else {
                    Ok(NamedNode::new_unchecked(r.value.clone().into_owned()))
                }
            }
            ast::Iri::PrefixedName(p) => {
                let prefix = p.namespace;
                let local_name = p.local.as_ref();

                if let Some(prefix_iri) = self.prefixes.get(prefix) {
                    let mut resolved_iri = prefix_iri.as_str().to_string();
                    resolved_iri.push_str(local_name);
                    Ok(NamedNode::new_unchecked(resolved_iri))
                } else {
                    Err(SparqlParseError::new_without_span(format!(
                        "Prefix '{prefix}' not found"
                    )))
                }
            }
        }
    }

    /// Resolves a graph ref, resolving any IRIs if necessary.
    pub fn rewrite_graph_ref_all(
        &self,
        graph: &ast::GraphRefAll,
    ) -> Result<GraphTarget, SparqlParseError> {
        match graph {
            ast::GraphRefAll::Graph(iri) => {
                let nn = self.resolve_iri(iri).map_err(|e| {
                    SparqlParseError::new_without_span(format!(
                        "Failed to resolve IRI: {e:?}"
                    ))
                })?;
                Ok(GraphTarget::NamedNode(nn))
            }
            ast::GraphRefAll::Default => Ok(GraphTarget::DefaultGraph),
            ast::GraphRefAll::Named => Ok(GraphTarget::NamedGraphs),
            ast::GraphRefAll::All => Ok(GraphTarget::AllGraphs),
        }
    }

    /// Maps a literal to [`rdf_fusion_common::Literal`], resolving IRIs if necessary using the
    /// base IRI.
    pub fn map_literal(
        &self,
        literal: &ast::Literal,
    ) -> Result<Literal, SparqlParseError> {
        match literal {
            ast::Literal::Boolean(b) => Ok(Literal::new_typed_literal(
                b.value.to_string(),
                xsd::BOOLEAN,
            )),
            ast::Literal::Integer(_i, lexeme) => Ok(Literal::new_typed_literal(
                lexeme.clone().into_owned(),
                xsd::INTEGER,
            )),
            ast::Literal::Decimal(_d, lexeme) => Ok(Literal::new_typed_literal(
                lexeme.clone().into_owned(),
                xsd::DECIMAL,
            )),
            ast::Literal::Double(_d, lexeme) => Ok(Literal::new_typed_literal(
                lexeme.clone().into_owned(),
                xsd::DOUBLE,
            )),
            ast::Literal::String(s) => {
                Ok(Literal::new_simple_literal(s.value.clone().into_owned()))
            }
            ast::Literal::LangString(s, lang) => {
                Ok(Literal::new_language_tagged_literal_unchecked(
                    s.value.clone().into_owned(),
                    lang.value.to_ascii_lowercase(),
                ))
            }
            ast::Literal::Typed(s, iri) => {
                let named_node = self.resolve_iri(iri)?;
                Ok(Literal::new_typed_literal(
                    s.value.clone().into_owned(),
                    named_node,
                ))
            }
        }
    }
}

/// A Builder for [`RewriterContext`] to construct it ergonomically.
///
/// The builder avoids that, for example, [`DateTime::now`] must be called if its overridden
/// anyways by the user.
#[derive(Default, Clone)]
pub struct RewriterContextBuilder {
    now: Option<DateTime>,
    dataset: Option<QueryDataset>,
    base_iri: Option<Iri<String>>,
    prefixes: HashMap<String, Iri<String>>,
}

impl RewriterContextBuilder {
    /// Creates a new, empty [`RewriterContextBuilder`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the `now` time for the query.
    pub fn with_now(mut self, now: DateTime) -> Self {
        self.now = Some(now);
        self
    }

    /// Sets the queried dataset.
    pub fn with_dataset(mut self, dataset: QueryDataset) -> Self {
        self.dataset = Some(dataset);
        self
    }

    /// Sets the base IRI of a query.
    pub fn with_base_iri(mut self, base_iri: Iri<String>) -> Self {
        self.base_iri = Some(base_iri);
        self
    }

    /// Process prologue declarations to set the base IRI and prefixes.
    pub fn with_prologue(mut self, prologue: &[ast::PrologueDecl]) -> Self {
        for decl in prologue {
            match decl {
                ast::PrologueDecl::Base(iri_ref) => {
                    let iriref = iri_ref.value.as_ref();
                    if let Some(base) = &self.base_iri {
                        if let Ok(resolved) = base.resolve(iriref) {
                            self.base_iri = Some(resolved);
                        }
                    } else if let Ok(parsed) = Iri::parse(iriref.to_owned()) {
                        self.base_iri = Some(parsed);
                    }
                }
                ast::PrologueDecl::Prefix(prefix, iri_ref) => {
                    let iriref = iri_ref.value.as_ref();
                    let resolved = if let Some(base) = &self.base_iri {
                        base.resolve(iriref).ok()
                    } else {
                        Iri::parse(iriref.to_owned()).ok()
                    };
                    if let Some(iri) = resolved {
                        self.prefixes.insert(prefix.to_string(), iri);
                    }
                }
            }
        }
        self
    }

    /// Adds a prefix mapping.
    pub fn with_prefix(mut self, prefix: String, iri: Iri<String>) -> Self {
        self.prefixes.insert(prefix, iri);
        self
    }

    /// Builds the [`RewriterContext`] falling back to default values where none were provided.
    pub fn build(self) -> RewriterContext {
        RewriterContext {
            now: self.now.unwrap_or_else(DateTime::now),
            dataset: self.dataset.unwrap_or_default(),
            base_iri: self.base_iri,
            prefixes: self.prefixes,
        }
    }
}
