use crate::RdfFusionContext;
use crate::sparql::QueryOptions;
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::{
    RdfFusionQuery, RdfFusionUpdate, UpdateOptions, evaluate_query_with_snapshot,
};
use futures::{StreamExt, TryStreamExt};
use itertools::izip;
use oxrdfio::{RdfFormat, RdfParser};
use rdf_fusion_common::sparql::algebra::GraphTarget;
use rdf_fusion_common::sparql::term::{
    GraphNamePattern, GroundQuadPattern, GroundTermPattern, QuadPattern,
};
use rdf_fusion_common::sparql::{GraphUpdateOperation, Query};
use rdf_fusion_common::{
    BlankNode, GraphName, NamedNodePattern, NamedOrBlankNode, NamedOrBlankNodeRef, Quad,
    Term, TermPattern,
};
use rdf_fusion_encoding::quads_to_plain_term_dataframe;
use rdf_fusion_extensions::storage::QuadStorageGraphTarget;
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
use sparesults::QuerySolution;
use std::collections::HashMap;
use std::io;
use tokio_util::io::StreamReader;

/// Implements the SPARQL `UPDATE` query.
pub async fn evaluate_update(
    ctx: &RdfFusionContext,
    builder_context: RdfFusionLogicalPlanBuilderContext,
    update: &RdfFusionUpdate,
    _options: UpdateOptions,
) -> Result<(), QueryEvaluationError> {
    let state = ctx.session_context().state();
    let transaction = ctx.storage().begin_transaction(&state).await?;

    for (operation, dataset) in izip!(&update.inner.operations, &update.using_datasets) {
        match operation {
            GraphUpdateOperation::InsertData { data } => {
                let data: Vec<Quad> = data
                    .iter()
                    .map(|q| Quad {
                        subject: q.subject.clone(),
                        predicate: q.predicate.clone(),
                        object: q.object.clone(),
                        graph_name: convert_graph_name(q.graph_name.clone()),
                    })
                    .collect();
                let df = quads_to_plain_term_dataframe(ctx.session_context(), &data);
                transaction.insert(df).await?;
            }
            GraphUpdateOperation::DeleteData { data } => {
                let data: Vec<Quad> = data
                    .iter()
                    .map(|q| Quad {
                        subject: NamedOrBlankNode::NamedNode(q.subject.clone()),
                        predicate: q.predicate.clone(),
                        object: convert_ground_term(q.object.clone()),
                        graph_name: convert_graph_name(q.graph_name.clone()),
                    })
                    .collect();
                let df = quads_to_plain_term_dataframe(ctx.session_context(), &data);
                transaction.remove(df).await?;
            }
            GraphUpdateOperation::Clear { silent, graph } => {
                let target = convert_graph_target(graph);
                let res = transaction.clear_graph(&target).await;
                if let Err(e) = res {
                    if !silent {
                        return Err(QueryEvaluationError::Storage(e));
                    }
                }
            }
            GraphUpdateOperation::Drop { silent, graph } => {
                let target = convert_graph_target(graph);
                let res = transaction.drop_graph(&target).await;
                if let Err(e) = res {
                    if !silent {
                        return Err(QueryEvaluationError::Storage(e));
                    }
                }
            }
            GraphUpdateOperation::Create { silent, graph } => {
                let res = transaction
                    .create_named_graph(NamedOrBlankNodeRef::NamedNode(graph.into()))
                    .await?;
                if let Some(false) = res {
                    if !silent {
                        return Err(QueryEvaluationError::GraphAlreadyExists(
                            graph.clone(),
                        ));
                    }
                }
            }
            GraphUpdateOperation::DeleteInsert {
                delete,
                insert,
                pattern,
                using,
            } => {
                let dataset = dataset.clone().unwrap_or_default();

                let query = RdfFusionQuery {
                    inner: Query::Select {
                        dataset: using.clone(),
                        pattern: *pattern.clone(),
                        base_iri: None,
                    },
                    dataset,
                };
                let snapshot = transaction.snapshot().await?;
                let (results, _) = evaluate_query_with_snapshot(
                    ctx,
                    builder_context.clone(),
                    &query,
                    QueryOptions::default(),
                    snapshot,
                )
                .await?;

                if let crate::results::QueryResults::Solutions(mut solutions) = results {
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
            GraphUpdateOperation::Load {
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
                        .and_then(|ct: &str| RdfFormat::from_media_type(ct))
                        .or_else(|| {
                            RdfFormat::from_extension(source.as_str().rsplit_once('.')?.1)
                        })
                        .unwrap_or(RdfFormat::Turtle);

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
                    let destination = convert_rdf_fusion_graph_name(destination.clone());

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
        }
    }

    transaction.commit().await?;

    Ok(())
}

fn convert_rdf_fusion_graph_name(
    gn: rdf_fusion_common::sparql::term::GraphName,
) -> GraphName {
    match gn {
        rdf_fusion_common::sparql::term::GraphName::NamedNode(n) => {
            GraphName::NamedNode(n)
        }
        rdf_fusion_common::sparql::term::GraphName::DefaultGraph => {
            GraphName::DefaultGraph
        }
    }
}

fn convert_graph_name(gn: rdf_fusion_common::sparql::term::GraphName) -> GraphName {
    match gn {
        rdf_fusion_common::sparql::term::GraphName::NamedNode(n) => {
            GraphName::NamedNode(n)
        }
        rdf_fusion_common::sparql::term::GraphName::DefaultGraph => {
            GraphName::DefaultGraph
        }
    }
}

fn convert_ground_term(term: rdf_fusion_common::sparql::term::GroundTerm) -> Term {
    match term {
        rdf_fusion_common::sparql::term::GroundTerm::NamedNode(n) => Term::NamedNode(n),
        rdf_fusion_common::sparql::term::GroundTerm::Literal(l) => Term::Literal(l),
    }
}

fn convert_graph_target(graph: &GraphTarget) -> QuadStorageGraphTarget {
    match graph {
        GraphTarget::NamedNode(n) => QuadStorageGraphTarget::NamedNode(n.clone()),
        GraphTarget::DefaultGraph => QuadStorageGraphTarget::DefaultGraph,
        GraphTarget::NamedGraphs => QuadStorageGraphTarget::NamedGraphs,
        GraphTarget::AllGraphs => QuadStorageGraphTarget::AllGraphs,
    }
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

    fn substitute(&mut self, solution: &QuerySolution) -> Vec<Quad> {
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

fn ground_term_pattern_to_term_pattern(pattern: &GroundTermPattern) -> TermPattern {
    match pattern {
        GroundTermPattern::NamedNode(n) => TermPattern::NamedNode(n.clone()),
        GroundTermPattern::Literal(l) => TermPattern::Literal(l.clone()),
        GroundTermPattern::Variable(v) => TermPattern::Variable(v.clone()),
    }
}

fn instantiate_quad_pattern(
    pattern: &QuadPattern,
    solution: &QuerySolution,
    bnodes: &mut HashMap<BlankNode, BlankNode>,
) -> Option<Quad> {
    let subject = match &pattern.subject {
        TermPattern::NamedNode(n) => NamedOrBlankNode::NamedNode(n.clone()),
        TermPattern::BlankNode(b) => {
            let bnode = bnodes.entry(b.clone()).or_default();
            NamedOrBlankNode::BlankNode(bnode.clone())
        }
        TermPattern::Variable(v) => match solution.get(v)? {
            Term::NamedNode(n) => NamedOrBlankNode::NamedNode(n.clone()),
            Term::BlankNode(b) => NamedOrBlankNode::BlankNode(b.clone()),
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
