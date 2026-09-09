mod context;
mod expression;
mod graph_pattern;
mod query;
mod update;

pub use context::*;
pub use expression::*;
pub use graph_pattern::*;
pub use query::*;
use rdf_fusion_common::sparql::QueryDataset;
pub use update::*;

use crate::{ParserOptions, SparqlParseError, ast};

fn create_rewriter_context(
    ast: &ast::Query,
    config: &ParserOptions,
) -> Result<RewriterContext, SparqlParseError> {
    let mut planner_context_builder = RewriterContextBuilder::new();

    if let Some(base) = &config.default_base_iri() {
        planner_context_builder = planner_context_builder.with_base_iri((*base).clone());
    }

    if let Some(dataset) = &config.default_dataset() {
        planner_context_builder =
            planner_context_builder.with_dataset((*dataset).clone());
    }

    let planner_context = planner_context_builder
        .with_now(config.now())
        .with_prologue(&ast.prologue) // may overwrite defaults
        .build();

    let dataset = create_query_dataset(ast, &planner_context)?;
    let planner_context = planner_context.into_builder().with_dataset(dataset).build();
    Ok(planner_context)
}

/// Computes the [`QueryDataset`] specified by a query's `FROM` / `FROM NAMED` clauses.
///
/// This is the dataset specification of a parsed query before any evaluation, useful
/// to know which graphs need to be loaded for a query to run.
pub fn query_dataset(
    ast: &ast::Query,
    config: &ParserOptions,
) -> Result<QueryDataset, SparqlParseError> {
    Ok(create_rewriter_context(ast, config)?.dataset().clone())
}

/// Creates a new [`QueryDataset`] based on the given query (e.g., `FROM NAMED`).
fn create_query_dataset(
    ast: &ast::Query,
    rewriter_context: &RewriterContext,
) -> Result<QueryDataset, SparqlParseError> {
    let dataset_clause = match &ast.variant {
        ast::QueryQuery::Select(s) => &s.dataset_clause,
        ast::QueryQuery::Construct(s) => &s.dataset_clause,
        ast::QueryQuery::Ask(s) => &s.dataset_clause,
        ast::QueryQuery::Describe(s) => &s.dataset_clause,
    };
    let mut default = Vec::new();
    let mut named = Vec::new();
    for clause in dataset_clause {
        match clause {
            ast::GraphClause::Default(iri) => {
                let resolved = rewriter_context.resolve_iri(iri).map_err(|e| {
                    SparqlParseError::new_without_span(format!(
                        "Failed to resolve IRI: {e:?}"
                    ))
                })?;
                default.push(rdf_fusion_common::GraphName::NamedNode(resolved));
            }
            ast::GraphClause::Named(iri) => {
                let resolved = rewriter_context.resolve_iri(iri).map_err(|e| {
                    SparqlParseError::new_without_span(format!(
                        "Failed to resolve IRI: {e:?}"
                    ))
                })?;
                named.push(rdf_fusion_common::NamedOrBlankNode::NamedNode(resolved));
            }
        }
    }
    let dataset = if default.is_empty() && named.is_empty() {
        rewriter_context.dataset().clone()
    } else {
        QueryDataset::new(Some(default), Some(named))
    };
    Ok(dataset)
}
