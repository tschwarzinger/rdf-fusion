use crate::RdfFusionContext;
use crate::results::QueryResults;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{QueryOptions, UpdateOptions, evaluate_query_with_snapshot};

use datafusion::dataframe::DataFrame;
use futures::{StreamExt, TryStreamExt};
use oxrdfio::RdfParser;
use sparesults::QuerySolution as OxQuerySolution;

use rdf_fusion_common::sparql::term::{GraphNamePattern, GroundQuadPattern, QuadPattern};
use rdf_fusion_common::sparql::{
    GraphTarget, QueryVariant, RdfFusionQuery, RdfFusionUpdate, UpdateOperation,
};
use rdf_fusion_common::{
    BlankNode, GraphName, NamedNodePattern, Quad, Term, TermPattern,
};

use rdf_fusion_encoding::quads_to_plain_term_dataframe;
use rdf_fusion_extensions::storage::graph_target_to_plain_term_dataframe;
use std::collections::HashMap;
use std::io;
use tokio_util::io::StreamReader;

/// Implements the SPARQL `UPDATE` query.
pub async fn evaluate_update(
    ctx: &RdfFusionContext,
    update: &RdfFusionUpdate,
    _options: UpdateOptions,
) -> Result<(), QueryEvaluationError> {
    let state = ctx.session_context().state();
    let transaction = ctx.storage().begin_transaction(&state).await?;

    for operation in update.operations() {
        match operation {
            UpdateOperation::InsertData { quads } => {
                let df = ctx.session_context().read_batch(quads.clone())?;
                transaction.insert(df).await?;
            }
            UpdateOperation::DeleteData { quads } => {
                let df = ctx.session_context().read_batch(quads.clone())?;
                transaction.remove(df).await?;
            }
            UpdateOperation::DeleteInsert {
                delete,
                insert,
                pattern,
            } => {
                let query = RdfFusionQuery::new(pattern.clone(), QueryVariant::Select);
                let snapshot = transaction.snapshot().await?;
                let (results, _) = evaluate_query_with_snapshot(
                    ctx,
                    &query,
                    QueryOptions::default(),
                    snapshot,
                )
                .await?;

                if let QueryResults::Solutions(mut solutions) = results {
                    let mut delete_substituter = QuadPatternSubstituter::new(
                        delete
                            .iter()
                            .map(ground_quad_pattern_to_quad_pattern)
                            .collect(),
                    );
                    let mut insert_substituter =
                        QuadPatternSubstituter::new(insert.clone());

                    let mut deleted_quads = Vec::new();
                    let mut inserted_quads = Vec::new();
                    while let Some(solution) = solutions.next().await {
                        let solution = solution?;

                        deleted_quads.extend(delete_substituter.substitute(&solution));
                        inserted_quads.extend(insert_substituter.substitute(&solution));
                    }

                    if !deleted_quads.is_empty() {
                        let df = quads_to_plain_term_dataframe(
                            ctx.session_context(),
                            &deleted_quads,
                        );
                        transaction.remove(df).await?;
                    }

                    if !inserted_quads.is_empty() {
                        let df = quads_to_plain_term_dataframe(
                            ctx.session_context(),
                            &inserted_quads,
                        );
                        transaction.insert(df).await?;
                    }
                }
            }
            UpdateOperation::Load {
                source,
                destination,
                silent,
            } => {
                let res = async {
                    let response = reqwest::get(source.as_str()).await.map_err(|e| {
                        QueryEvaluationError::InternalError(e.to_string())
                    })?;
                    let format = response
                        .headers()
                        .get(reqwest::header::CONTENT_TYPE)
                        .and_then(|ct| ct.to_str().ok())
                        .and_then(|ct: &str| {
                            rdf_fusion_common::RdfFormat::from_media_type(ct)
                        })
                        .or_else(|| {
                            rdf_fusion_common::RdfFormat::from_extension(
                                source.as_str().rsplit_once('.')?.1,
                            )
                        })
                        .unwrap_or(rdf_fusion_common::RdfFormat::Turtle);

                    let stream = response.bytes_stream().map_err(io::Error::other);
                    let reader = StreamReader::new(stream);
                    let mut parser = RdfParser::from_format(format)
                        .with_base_iri(source.as_str())
                        .map_err(|err| {
                            QueryEvaluationError::InternalError(format!(
                                "Invalid source IRI: {err} {source}"
                            ))
                        })?
                        .for_tokio_async_reader(reader);

                    let mut quads = Vec::new();
                    let destination = destination.clone();

                    while let Some(quad) = parser.next().await {
                        let mut quad =
                            quad.map_err(QueryEvaluationError::GraphParsing)?;
                        if !format.supports_datasets()
                            || matches!(quad.graph_name, GraphName::DefaultGraph)
                        {
                            quad.graph_name = destination.clone();
                        }
                        quads.push(quad);

                        if quads.len() >= 1000 {
                            let df = quads_to_plain_term_dataframe(
                                ctx.session_context(),
                                &quads,
                            );
                            transaction.insert(df).await?;
                            quads.clear();
                        }
                    }

                    if !quads.is_empty() {
                        let df =
                            quads_to_plain_term_dataframe(ctx.session_context(), &quads);
                        transaction.insert(df).await?;
                    }
                    Ok::<(), QueryEvaluationError>(())
                }
                .await;

                if let Err(e) = res {
                    if !silent {
                        return Err(e);
                    }
                }
            }
            UpdateOperation::Clear { silent, graph } => {
                let df = create_graph_target_dataframe(ctx, transaction.as_ref(), graph)
                    .await?;
                let res = transaction.clear_graph(df).await;
                if let Err(e) = res {
                    if !silent {
                        return Err(QueryEvaluationError::Storage(e));
                    }
                }
            }
            UpdateOperation::Drop { silent, graph } => {
                let df = create_graph_target_dataframe(ctx, transaction.as_ref(), graph)
                    .await?;
                let res = transaction.drop_graph(df).await;
                if let Err(e) = res {
                    if !silent {
                        return Err(QueryEvaluationError::Storage(e));
                    }
                }
            }
            UpdateOperation::Create { silent, graph } => {
                let df = create_graph_target_dataframe(
                    ctx,
                    transaction.as_ref(),
                    &GraphTarget::NamedNode(graph.clone()),
                )
                .await?;
                let res = transaction
                    .create_named_graph(df)
                    .await
                    .map_err(QueryEvaluationError::Storage)?;
                if let Some(false) = res {
                    if !silent {
                        return Err(QueryEvaluationError::GraphAlreadyExists(
                            graph.clone(),
                        ));
                    }
                }
            }
        }
    }

    transaction.commit().await?;

    Ok(())
}

async fn create_graph_target_dataframe(
    ctx: &RdfFusionContext,
    transaction: &dyn rdf_fusion_extensions::storage::QuadStorageTransaction,
    graph: &GraphTarget,
) -> Result<DataFrame, QueryEvaluationError> {
    let snapshot = transaction
        .snapshot()
        .await
        .map_err(QueryEvaluationError::Storage)?;

    graph_target_to_plain_term_dataframe(
        ctx.session_context(),
        &ctx.storage().encoding(),
        snapshot.as_ref(),
        &graph.clone().into(),
    )
    .await
    .map_err(QueryEvaluationError::Storage)
}

struct QuadPatternSubstituter {
    templates: Vec<QuadPattern>,
    bnodes: HashMap<BlankNode, BlankNode>,
}

impl QuadPatternSubstituter {
    fn new(templates: Vec<QuadPattern>) -> Self {
        Self {
            templates,
            bnodes: HashMap::new(),
        }
    }

    fn substitute(&mut self, solution: &OxQuerySolution) -> Vec<Quad> {
        let mut result = Vec::with_capacity(self.templates.len());
        for template in &self.templates {
            if let Some(quad) =
                instantiate_quad_pattern(template, solution, &mut self.bnodes)
            {
                result.push(quad);
            }
        }
        self.bnodes.clear();
        result
    }
}

fn ground_quad_pattern_to_quad_pattern(pattern: &GroundQuadPattern) -> QuadPattern {
    QuadPattern {
        subject: ground_term_pattern_to_term_pattern(&pattern.subject),
        predicate: pattern.predicate.clone(),
        object: ground_term_pattern_to_term_pattern(&pattern.object),
        graph_name: pattern.graph_name.clone(),
    }
}

fn ground_term_pattern_to_term_pattern(
    pattern: &rdf_fusion_common::sparql::term::GroundTermPattern,
) -> TermPattern {
    match pattern {
        rdf_fusion_common::sparql::term::GroundTermPattern::NamedNode(n) => {
            TermPattern::NamedNode(n.clone())
        }
        rdf_fusion_common::sparql::term::GroundTermPattern::Literal(l) => {
            TermPattern::Literal(l.clone())
        }
        rdf_fusion_common::sparql::term::GroundTermPattern::Variable(v) => {
            TermPattern::Variable(v.clone())
        }
    }
}

fn instantiate_quad_pattern(
    pattern: &QuadPattern,
    solution: &OxQuerySolution,
    bnodes: &mut HashMap<BlankNode, BlankNode>,
) -> Option<Quad> {
    let subject = match &pattern.subject {
        TermPattern::NamedNode(n) => {
            rdf_fusion_common::NamedOrBlankNode::NamedNode(n.clone())
        }
        TermPattern::BlankNode(b) => {
            let bnode = bnodes.entry(b.clone()).or_default();
            rdf_fusion_common::NamedOrBlankNode::BlankNode(bnode.clone())
        }
        TermPattern::Variable(v) => match solution.get(v)? {
            Term::NamedNode(n) => {
                rdf_fusion_common::NamedOrBlankNode::NamedNode(n.clone())
            }
            Term::BlankNode(b) => {
                rdf_fusion_common::NamedOrBlankNode::BlankNode(b.clone())
            }
            Term::Literal(_) => return None,
        },
        TermPattern::Literal(_) => return None,
    };
    let predicate = match &pattern.predicate {
        NamedNodePattern::NamedNode(n) => n.clone(),
        NamedNodePattern::Variable(v) => match solution.get(v)? {
            Term::NamedNode(n) => n.clone(),
            _ => return None,
        },
    };
    let object = match &pattern.object {
        TermPattern::NamedNode(n) => Term::NamedNode(n.clone()),
        TermPattern::BlankNode(b) => {
            let bnode = bnodes.entry(b.clone()).or_default();
            Term::BlankNode(bnode.clone())
        }
        TermPattern::Literal(l) => Term::Literal(l.clone()),
        TermPattern::Variable(v) => solution.get(v)?.clone(),
    };
    let graph_name = match &pattern.graph_name {
        GraphNamePattern::NamedNode(n) => GraphName::NamedNode(n.clone()),
        GraphNamePattern::DefaultGraph => GraphName::DefaultGraph,
        GraphNamePattern::Variable(v) => match solution.get(v)? {
            Term::NamedNode(n) => GraphName::NamedNode(n.clone()),
            Term::BlankNode(b) => GraphName::BlankNode(b.clone()),
            Term::Literal(_) => return None,
        },
    };
    Some(Quad {
        subject,
        predicate,
        object,
        graph_name,
    })
}
