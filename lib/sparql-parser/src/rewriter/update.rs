use crate::ast;
use crate::error::SparqlParseError;
use crate::options::ParserOptions;
use crate::rewriter::{GraphPatternRewriter, RewriterContext, RewriterContextBuilder};
use crate::span::Span;
use datafusion_common::arrow::record_batch::RecordBatch;
use rdf_fusion_common::GraphName;
use rdf_fusion_common::sparql::{RdfFusionUpdate, UpdateOperation};
use rdf_fusion_encoding::plain_term::PlainTermQuadsBuilder;
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;

/// A dedicated rewriter to transform SPARQL Update ASTs into Update Algebra operations.
pub struct UpdateRewriter {
    context_view: RdfFusionContextView,
}

impl UpdateRewriter {
    pub fn new(context_view: RdfFusionContextView) -> Self {
        Self { context_view }
    }

    /// Rewrites a full SPARQL Update AST into an `RdfFusionUpdate`.
    pub fn rewrite(
        &self,
        ast: &ast::Update,
        config: &ParserOptions,
    ) -> Result<RdfFusionUpdate, SparqlParseError> {
        let mut operations = Vec::new();
        let mut seen_blank_nodes = std::collections::HashMap::new();

        let mut current_context_builder = RewriterContextBuilder::new();
        if let Some(base) = &config.default_base_iri() {
            current_context_builder =
                current_context_builder.with_base_iri((*base).clone());
        }
        if let Some(dataset) = &config.default_dataset() {
            current_context_builder =
                current_context_builder.with_dataset((*dataset).clone());
        }

        for (prologue, op) in &ast.operations {
            current_context_builder = current_context_builder.with_prologue(prologue);
            let rewriter_context = current_context_builder.clone().build();
            let builder_context =
                RdfFusionLogicalPlanBuilderContext::new(self.context_view.clone());

            let rewriter =
                GraphPatternRewriter::new(builder_context, rewriter_context.clone());
            let operation_rewriter = UpdateOperationRewriter {
                rewriter,
                rewriter_context,
            };
            let operation =
                operation_rewriter.rewrite_operation(op, &mut seen_blank_nodes)?;
            operations.extend(operation);
        }

        Ok(RdfFusionUpdate::new(operations))
    }
}

/// Responsible for rewriting a single update operation.
///
/// The context was already created for that specific operation.
struct UpdateOperationRewriter {
    rewriter: GraphPatternRewriter,
    rewriter_context: RewriterContext,
}

impl UpdateOperationRewriter {
    /// Rewrites a single update operation (`ast::Update1`).
    fn rewrite_operation(
        &self,
        op: &ast::Update1,
        seen_blank_nodes: &mut std::collections::HashMap<String, Span>,
    ) -> Result<Vec<UpdateOperation>, SparqlParseError> {
        match op {
            ast::Update1::InsertData { quads } => {
                self.validate_quad_blank_nodes(quads, seen_blank_nodes, false)?;
                Ok(vec![UpdateOperation::InsertData {
                    quads: self.convert_quad_patterns_to_plain_term_quads(quads)?,
                }])
            }
            ast::Update1::DeleteData { quads } => {
                self.validate_quad_blank_nodes(quads, seen_blank_nodes, true)?;
                Ok(vec![UpdateOperation::DeleteData {
                    quads: self.convert_quad_patterns_to_plain_term_quads(quads)?,
                }])
            }
            ast::Update1::DeleteWhere { pattern } => {
                let mut bnodes = Vec::new();
                collect_quad_blank_nodes(pattern, &mut bnodes);
                if let Some((_, span)) = bnodes.first() {
                    return Err(SparqlParseError::new(
                        *span,
                        "Blank nodes are not allowed in DELETE templates",
                    ));
                }

                // Ensure logical plan builds correctly, just verifying validity here
                let plan = self
                    .rewriter
                    .rewrite_quad_patterns_as_logical_plan(pattern)
                    .and_then(|builder| Ok(builder.with_plain_terms()?.build()?))
                    .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?;

                Ok(vec![UpdateOperation::DeleteInsert {
                    delete: self
                        .rewriter
                        .rewrite_quad_patterns(pattern)
                        .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?
                        .into_iter()
                        .map(|q| {
                            q.try_into().map_err(|_| {
                                SparqlParseError::new_without_span(
                                    "Blank nodes are not allowed in DELETE templates"
                                        .to_string(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    insert: Vec::new(),
                    pattern: plan,
                }])
            }
            ast::Update1::Modify {
                delete,
                insert,
                r#where,
                with,
                using,
            } => {
                let mut bnodes = Vec::new();
                collect_quad_blank_nodes(delete, &mut bnodes);
                if let Some((_, span)) = bnodes.first() {
                    return Err(SparqlParseError::new(
                        *span,
                        "Blank nodes are not allowed in DELETE templates",
                    ));
                }

                // Build the dataset (and active graph) for the WHERE from `WITH`/`USING`.
                let (op_context, active_graph) =
                    self.build_update_context(with.clone(), using)?;
                let builder_context = self.rewriter.builder_context().clone();
                let rewriter = GraphPatternRewriter::new(builder_context, op_context);

                let plan = rewriter
                    .rewrite_graph_pattern(r#where)
                    .and_then(|p| Ok(p.with_plain_terms()?.build()?))
                    .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?;

                let mut delete = rewriter
                    .rewrite_quad_patterns(delete)
                    .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?;
                let mut insert = rewriter
                    .rewrite_quad_patterns(insert)
                    .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?;
                if let Some(graph) = &active_graph {
                    apply_active_graph_to_templates(&mut delete, graph);
                    apply_active_graph_to_templates(&mut insert, graph);
                }

                Ok(vec![UpdateOperation::DeleteInsert {
                    delete: delete
                        .into_iter()
                        .map(|q| {
                            q.try_into().map_err(|_| {
                                SparqlParseError::new_without_span(
                                    "Blank nodes are not allowed in DELETE templates"
                                        .to_string(),
                                )
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?,
                    insert,
                    pattern: plan,
                }])
            }
            ast::Update1::Load { silent, from, to } => {
                let from_nn =
                    self.rewriter
                        .planner_context()
                        .resolve_iri(from)
                        .map_err(|e| {
                            SparqlParseError::new_without_span(format!(
                                "Failed to resolve IRI: {e:?}"
                            ))
                        })?;

                let to_graph = match to {
                    Some(iri) => Some(GraphName::NamedNode(
                        self.rewriter.planner_context().resolve_iri(iri).map_err(
                            |e| {
                                SparqlParseError::new_without_span(format!(
                                    "Failed to resolve IRI: {e:?}"
                                ))
                            },
                        )?,
                    )),
                    None => None,
                };

                Ok(vec![UpdateOperation::Load {
                    silent: *silent,
                    source: from_nn,
                    destination: to_graph.unwrap_or(GraphName::DefaultGraph),
                }])
            }
            ast::Update1::Clear { silent, graph } => Ok(vec![UpdateOperation::Clear {
                silent: *silent,
                graph: self
                    .rewriter
                    .planner_context()
                    .rewrite_graph_ref_all(graph)?,
            }]),
            ast::Update1::Drop { silent, graph } => Ok(vec![UpdateOperation::Drop {
                silent: *silent,
                graph: self
                    .rewriter
                    .planner_context()
                    .rewrite_graph_ref_all(graph)?,
            }]),
            ast::Update1::Create { silent, graph } => {
                let nn = self.rewriter_context.resolve_iri(graph).map_err(|e| {
                    SparqlParseError::new_without_span(format!(
                        "Failed to resolve IRI: {e:?}"
                    ))
                })?;
                Ok(vec![UpdateOperation::Create {
                    silent: *silent,
                    graph: nn,
                }])
            }
            ast::Update1::Add { silent, from, to } => {
                let from_graph = match from {
                    ast::GraphOrDefault::Graph(iri) => GraphName::NamedNode(
                        self.rewriter_context.resolve_iri(iri).map_err(|e| {
                            SparqlParseError::new_without_span(format!(
                                "Failed to resolve IRI: {e:?}"
                            ))
                        })?,
                    ),
                    ast::GraphOrDefault::Default => GraphName::DefaultGraph,
                };
                let to_graph = match to {
                    ast::GraphOrDefault::Graph(iri) => GraphName::NamedNode(
                        self.rewriter_context.resolve_iri(iri).map_err(|e| {
                            SparqlParseError::new_without_span(format!(
                                "Failed to resolve IRI: {e:?}"
                            ))
                        })?,
                    ),
                    ast::GraphOrDefault::Default => GraphName::DefaultGraph,
                };

                if let GraphName::NamedNode(nn) = &from_graph {
                    if let Some(graphs) =
                        self.rewriter_context.dataset().available_named_graphs()
                    {
                        if !graphs.contains(
                            &rdf_fusion_common::NamedOrBlankNode::NamedNode(nn.clone()),
                        ) {
                            if !*silent {
                                return Err(SparqlParseError::new_without_span(format!(
                                    "The graph {nn} does not exist"
                                )));
                            } else {
                                return Ok(Vec::new());
                            }
                        }
                    }
                }

                Ok(vec![self.copy_graph(from_graph, to_graph)?])
            }
            ast::Update1::Move { silent, from, to } => {
                let from_graph = match from {
                    ast::GraphOrDefault::Graph(iri) => GraphName::NamedNode(
                        self.rewriter_context.resolve_iri(iri).map_err(|e| {
                            SparqlParseError::new_without_span(format!(
                                "Failed to resolve IRI: {e:?}"
                            ))
                        })?,
                    ),
                    ast::GraphOrDefault::Default => GraphName::DefaultGraph,
                };
                let to_graph = match to {
                    ast::GraphOrDefault::Graph(iri) => GraphName::NamedNode(
                        self.rewriter_context.resolve_iri(iri).map_err(|e| {
                            SparqlParseError::new_without_span(format!(
                                "Failed to resolve IRI: {e:?}"
                            ))
                        })?,
                    ),
                    ast::GraphOrDefault::Default => GraphName::DefaultGraph,
                };

                if let GraphName::NamedNode(nn) = &from_graph {
                    if let Some(graphs) =
                        self.rewriter_context.dataset().available_named_graphs()
                    {
                        if !graphs.contains(
                            &rdf_fusion_common::NamedOrBlankNode::NamedNode(nn.clone()),
                        ) {
                            if !*silent {
                                return Err(SparqlParseError::new_without_span(format!(
                                    "The graph {nn} does not exist"
                                )));
                            } else {
                                return Ok(Vec::new());
                            }
                        }
                    }
                }

                if from_graph == to_graph {
                    return Ok(Vec::new());
                }

                Ok(vec![
                    UpdateOperation::Drop {
                        silent: true,
                        graph: match &to_graph {
                            GraphName::NamedNode(nn) => rdf_fusion_common::sparql::GraphTarget::NamedNode(nn.clone()),
                            GraphName::DefaultGraph => rdf_fusion_common::sparql::GraphTarget::DefaultGraph,
                            GraphName::BlankNode(_) => return Err(SparqlParseError::new_without_span("Blank nodes are not allowed as graph names in update operations".to_string())),
                        },
                    },
                    self.copy_graph(from_graph.clone(), to_graph)?,
                    UpdateOperation::Drop {
                        silent: *silent,
                        graph: match &from_graph {
                            GraphName::NamedNode(nn) => rdf_fusion_common::sparql::GraphTarget::NamedNode(nn.clone()),
                            GraphName::DefaultGraph => rdf_fusion_common::sparql::GraphTarget::DefaultGraph,
                            GraphName::BlankNode(_) => return Err(SparqlParseError::new_without_span("Blank nodes are not allowed as graph names in update operations".to_string())),
                        },
                    }
                ])
            }
            ast::Update1::Copy { silent, from, to } => {
                let from_graph = match from {
                    ast::GraphOrDefault::Graph(iri) => GraphName::NamedNode(
                        self.rewriter_context.resolve_iri(iri).map_err(|e| {
                            SparqlParseError::new_without_span(format!(
                                "Failed to resolve IRI: {e:?}"
                            ))
                        })?,
                    ),
                    ast::GraphOrDefault::Default => GraphName::DefaultGraph,
                };
                let to_graph = match to {
                    ast::GraphOrDefault::Graph(iri) => GraphName::NamedNode(
                        self.rewriter_context.resolve_iri(iri).map_err(|e| {
                            SparqlParseError::new_without_span(format!(
                                "Failed to resolve IRI: {e:?}"
                            ))
                        })?,
                    ),
                    ast::GraphOrDefault::Default => GraphName::DefaultGraph,
                };

                if let GraphName::NamedNode(nn) = &from_graph {
                    if let Some(graphs) =
                        self.rewriter_context.dataset().available_named_graphs()
                    {
                        if !graphs.contains(
                            &rdf_fusion_common::NamedOrBlankNode::NamedNode(nn.clone()),
                        ) {
                            if !*silent {
                                return Err(SparqlParseError::new_without_span(format!(
                                    "The graph {nn} does not exist"
                                )));
                            } else {
                                return Ok(Vec::new());
                            }
                        }
                    }
                }

                if from_graph == to_graph {
                    return Ok(Vec::new());
                }

                Ok(vec![
                    UpdateOperation::Drop {
                        silent: true,
                        graph: match &to_graph {
                            GraphName::NamedNode(nn) => rdf_fusion_common::sparql::GraphTarget::NamedNode(nn.clone()),
                            GraphName::DefaultGraph => rdf_fusion_common::sparql::GraphTarget::DefaultGraph,
                            GraphName::BlankNode(_) => return Err(SparqlParseError::new_without_span("Blank nodes are not allowed as graph names in update operations".to_string())),
                        },
                    },
                    self.copy_graph(from_graph, to_graph)?
                ])
            }
        }
    }

    /// Builds a [`RewriterContext`] with the dataset formed by the `WITH`/`USING` clauses of a
    /// `MODIFY` operation, along with the active graph (the `WITH` graph, if any).
    fn build_update_context(
        &self,
        with: Option<ast::Iri>,
        using: &[ast::GraphClause],
    ) -> Result<(RewriterContext, Option<GraphName>), SparqlParseError> {
        let mut default = Vec::new();
        let mut named = Vec::new();

        for clause in using {
            match clause {
                ast::GraphClause::Default(iri) => {
                    let nn = self.rewriter_context.resolve_iri(iri).map_err(|e| {
                        SparqlParseError::new_without_span(format!(
                            "Failed to resolve IRI: {e:?}"
                        ))
                    })?;
                    default.push(GraphName::NamedNode(nn));
                }
                ast::GraphClause::Named(iri) => {
                    let nn = self.rewriter_context.resolve_iri(iri).map_err(|e| {
                        SparqlParseError::new_without_span(format!(
                            "Failed to resolve IRI: {e:?}"
                        ))
                    })?;
                    named.push(rdf_fusion_common::NamedOrBlankNode::NamedNode(nn));
                }
            }
        }

        let has_using = !using.is_empty();

        let active_graph = match with {
            Some(iri) => {
                let nn = self.rewriter_context.resolve_iri(&iri).map_err(|e| {
                    SparqlParseError::new_without_span(format!(
                        "Failed to resolve IRI: {e:?}"
                    ))
                })?;
                let graph = GraphName::NamedNode(nn);
                default.push(graph.clone());
                Some(graph)
            }
            None => None,
        };

        let dataset = if !has_using && active_graph.is_none() {
            self.rewriter_context.dataset().clone()
        } else {
            let default = if default.is_empty() {
                None
            } else {
                Some(default)
            };
            // `USING` restricts the named graphs available to `GRAPH` patterns, while `WITH`
            // does not (a `GRAPH` clause overrides a `WITH` clause).
            let named = if has_using { Some(named) } else { None };
            rdf_fusion_common::sparql::QueryDataset::new(default, named)
        };

        let op_context = self
            .rewriter_context
            .clone()
            .into_builder()
            .with_dataset(dataset)
            .build();

        Ok((op_context, active_graph))
    }

    fn copy_graph(
        &self,
        from: GraphName,
        to: GraphName,
    ) -> Result<UpdateOperation, SparqlParseError> {
        let s = rdf_fusion_common::Variable::new_unchecked("s");
        let p = rdf_fusion_common::Variable::new_unchecked("p");
        let o = rdf_fusion_common::Variable::new_unchecked("o");

        let insert = vec![rdf_fusion_common::sparql::QuadPattern {
            subject: rdf_fusion_common::sparql::TermPattern::Variable(s.clone()),
            predicate: rdf_fusion_common::sparql::NamedNodePattern::Variable(p.clone()),
            object: rdf_fusion_common::sparql::TermPattern::Variable(o.clone()),
            graph_name: match to {
                GraphName::NamedNode(nn) => {
                    rdf_fusion_common::sparql::GraphNamePattern::NamedNode(nn)
                }
                GraphName::DefaultGraph => {
                    rdf_fusion_common::sparql::GraphNamePattern::DefaultGraph
                }
                GraphName::BlankNode(_) => {
                    return Err(SparqlParseError::new_without_span(
                        "Blank nodes are not allowed as graph names in update operations"
                            .to_string(),
                    ));
                }
            },
        }];

        let active_graph = match from {
            GraphName::NamedNode(nn) => {
                rdf_fusion_logical::ActiveGraph::Union(vec![GraphName::NamedNode(nn)])
            }
            GraphName::DefaultGraph => rdf_fusion_logical::ActiveGraph::DefaultGraph,
            GraphName::BlankNode(_) => {
                return Err(SparqlParseError::new_without_span(
                    "Blank nodes are not allowed as graph names in update operations"
                        .to_string(),
                ));
            }
        };

        let bgp = self
            .rewriter
            .builder_context()
            .create_bgp(
                &active_graph,
                None,
                &[rdf_fusion_common::sparql::TriplePattern {
                    subject: rdf_fusion_common::sparql::TermPattern::Variable(s),
                    predicate: rdf_fusion_common::sparql::NamedNodePattern::Variable(p),
                    object: rdf_fusion_common::sparql::TermPattern::Variable(o),
                }],
            )
            .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?;

        Ok(UpdateOperation::DeleteInsert {
            delete: Vec::new(),
            insert,
            pattern: bgp
                .with_plain_terms()?
                .build()
                .map_err(|e| SparqlParseError::new_without_span(e.to_string()))?,
        })
    }

    fn convert_quad_patterns_to_plain_term_quads(
        &self,
        quads: &ast::QuadPatterns,
    ) -> Result<RecordBatch, SparqlParseError> {
        for (graph, triples) in &quads.patterns {
            if let Some(ast::VarOrIri::Var(v)) = graph {
                return Err(SparqlParseError::new(
                    v.span,
                    "Variables are not allowed in INSERT/DELETE DATA graph name",
                ));
            }
            for (subject, property_list) in triples.iter() {
                match subject {
                    ast::GraphNode::VarOrTerm(ast::VarOrTerm::Var(v)) => {
                        return Err(SparqlParseError::new(
                            v.span,
                            "Variables/literals are not allowed in INSERT/DELETE DATA subject",
                        ));
                    }
                    ast::GraphNode::VarOrTerm(ast::VarOrTerm::Literal(l)) => {
                        return Err(SparqlParseError::new(
                            l.span(),
                            "Variables/literals are not allowed in INSERT/DELETE DATA subject",
                        ));
                    }
                    _ => {}
                }
                for (verb, objects) in property_list.iter() {
                    if let ast::Verb::Var(v) = verb {
                        return Err(SparqlParseError::new(
                            v.span,
                            "Variables are not allowed in INSERT/DELETE DATA predicate",
                        ));
                    }
                    for object in objects {
                        if let ast::GraphNode::VarOrTerm(ast::VarOrTerm::Var(v)) =
                            &object.graph_node
                        {
                            return Err(SparqlParseError::new(
                                v.span,
                                "Variables are not allowed in INSERT/DELETE DATA object",
                            ));
                        }
                    }
                }
            }
        }

        let mut quads_builder = PlainTermQuadsBuilder::with_capacity(quads.len());

        let quad_patterns = self.rewriter.rewrite_quad_patterns(quads)?;
        for q in quad_patterns {
            let subject = match &q.subject {
                rdf_fusion_common::sparql::TermPattern::NamedNode(nn) => {
                    rdf_fusion_common::NamedOrBlankNodeRef::NamedNode(nn.as_ref())
                }
                rdf_fusion_common::sparql::TermPattern::BlankNode(bn) => {
                    rdf_fusion_common::NamedOrBlankNodeRef::BlankNode(bn.as_ref())
                }
                _ => return Err(SparqlParseError::new_without_span(
                    "Variables/literals are not allowed in INSERT/DELETE DATA subject"
                        .to_string(),
                )),
            };
            let predicate = match &q.predicate {
                rdf_fusion_common::sparql::NamedNodePattern::NamedNode(nn) => nn.as_ref(),
                _ => {
                    return Err(SparqlParseError::new_without_span(
                        "Variables are not allowed in INSERT/DELETE DATA predicate"
                            .to_string(),
                    ));
                }
            };
            let object = match &q.object {
                rdf_fusion_common::sparql::TermPattern::NamedNode(nn) => {
                    rdf_fusion_common::TermRef::NamedNode(nn.as_ref())
                }
                rdf_fusion_common::sparql::TermPattern::BlankNode(bn) => {
                    rdf_fusion_common::TermRef::BlankNode(bn.as_ref())
                }
                rdf_fusion_common::sparql::TermPattern::Literal(lit) => {
                    rdf_fusion_common::TermRef::Literal(lit.as_ref())
                }
                _ => {
                    return Err(SparqlParseError::new_without_span(
                        "Variables are not allowed in INSERT/DELETE DATA object"
                            .to_string(),
                    ));
                }
            };
            let graph_name = match &q.graph_name {
                rdf_fusion_common::sparql::GraphNamePattern::NamedNode(nn) => {
                    rdf_fusion_common::GraphNameRef::NamedNode(nn.as_ref())
                }
                rdf_fusion_common::sparql::GraphNamePattern::DefaultGraph => {
                    rdf_fusion_common::GraphNameRef::DefaultGraph
                }
                _ => {
                    return Err(SparqlParseError::new_without_span(
                        "Variables are not allowed in INSERT/DELETE DATA graph name"
                            .to_string(),
                    ));
                }
            };

            quads_builder.append_quad(rdf_fusion_common::QuadRef::new(
                subject, predicate, object, graph_name,
            ));
        }

        Ok(quads_builder.finish().into_record_batch())
    }

    fn validate_quad_blank_nodes(
        &self,
        quads: &ast::QuadPatterns,
        seen_blank_nodes: &mut std::collections::HashMap<String, Span>,
        is_delete_data: bool,
    ) -> Result<(), SparqlParseError> {
        let mut bnodes = Vec::new();
        collect_quad_blank_nodes(quads, &mut bnodes);

        if is_delete_data {
            if let Some((_, span)) = bnodes.first() {
                return Err(SparqlParseError::new(
                    *span,
                    "Blank nodes are not allowed in DELETE DATA",
                ));
            }
        }

        let mut op_blank_nodes = std::collections::HashMap::new();
        for (bnode, span) in bnodes {
            if let Some(&prev_span) = seen_blank_nodes.get(&bnode) {
                return Err(SparqlParseError::new_with_info(
                    span,
                    format!("Blank node {bnode} used here"),
                    format!("Blank node {bnode} reused across operations"),
                    prev_span,
                    format!("Blank node {bnode} previously used here"),
                ));
            }
            op_blank_nodes.insert(bnode, span);
        }

        seen_blank_nodes.extend(op_blank_nodes);
        Ok(())
    }
}

fn collect_quad_blank_nodes<'a>(
    quads: &ast::QuadPatterns<'a>,
    out: &mut Vec<(String, Span)>,
) {
    for (_graph, triples) in &quads.patterns {
        for (subject, property_list) in triples.iter() {
            collect_graph_node_blank_nodes(subject, out);
            for (_verb, objects) in &property_list.properties {
                for object in objects {
                    collect_graph_node_blank_nodes(&object.graph_node, out);
                }
            }
        }
    }
}

fn collect_graph_node_blank_nodes<'a>(
    node: &ast::GraphNode<'a>,
    out: &mut Vec<(String, Span)>,
) {
    match node {
        ast::GraphNode::VarOrTerm(ast::VarOrTerm::BlankNode(bn)) => {
            let label = bn.value.0.unwrap_or("[]");
            out.push((label.to_string(), bn.span));
        }
        ast::GraphNode::Collection(items) => {
            out.push(("()".to_string(), items.span));
            for item in &items.value {
                collect_graph_node_blank_nodes(item, out);
            }
        }
        ast::GraphNode::BlankNodePropertyList(props) => {
            out.push(("[]".to_string(), props.span));
            for (_verb, objects) in &props.value.properties {
                for object in objects {
                    collect_graph_node_blank_nodes(&object.graph_node, out);
                }
            }
        }
        _ => {}
    }
}

/// Applies the active graph of a `WITH` clause to the default-graph quads of the
/// `DELETE`/`INSERT` templates.
fn apply_active_graph_to_templates(
    patterns: &mut [rdf_fusion_common::sparql::QuadPattern],
    graph: &GraphName,
) {
    let named_node = match graph {
        GraphName::NamedNode(nn) => nn,
        _ => return,
    };
    for pattern in patterns {
        if matches!(
            pattern.graph_name,
            rdf_fusion_common::sparql::GraphNamePattern::DefaultGraph
        ) {
            pattern.graph_name = rdf_fusion_common::sparql::GraphNamePattern::NamedNode(
                named_node.clone(),
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::SparqlParser;
    use rdf_fusion_common::NamedNode;
    use rdf_fusion_common::sparql::QueryDataset;
    use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
    use rdf_fusion_encoding::string::STRING_ENCODING;
    use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
    use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings};
    use rdf_fusion_functions::registry::DefaultRdfFusionFunctionRegistry;
    use std::sync::Arc;

    fn test_context_view() -> RdfFusionContextView {
        let encodings = RdfFusionEncodings::new(
            Arc::clone(&PLAIN_TERM_ENCODING),
            Arc::new(TypedFamilyEncoding::default()),
            None,
            Arc::clone(&STRING_ENCODING),
        );
        let registry = Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));
        RdfFusionContextView::new(registry, encodings, QuadStorageEncoding::PlainTerm)
    }

    fn test_config_with_available_graphs(available_graphs: Vec<&str>) -> ParserOptions {
        let named_nodes = available_graphs
            .into_iter()
            .map(|iri| NamedNode::new_unchecked(iri).into())
            .collect();
        let mut dataset = QueryDataset::default();
        dataset.set_available_named_graphs(named_nodes);
        ParserOptions::builder()
            .with_default_dataset(Some(dataset))
            .build()
    }

    #[test]
    fn test_add_silent_vs_non_silent() {
        let ctx = test_context_view();
        let config =
            test_config_with_available_graphs(vec!["http://example.org/existing"]);

        let non_silent =
            "ADD <http://example.org/nonexistent> TO <http://example.org/existing>";
        let ast = SparqlParser::new(non_silent, Arc::clone(ctx.functions()))
            .parse_update()
            .unwrap();
        let res = UpdateRewriter::new(ctx.clone()).rewrite(&ast, &config);
        assert!(
            res.is_err(),
            "Expected error for non-silent ADD with nonexistent source graph"
        );

        let silent = "ADD SILENT <http://example.org/nonexistent> TO <http://example.org/existing>";
        let ast = SparqlParser::new(silent, Arc::clone(ctx.functions()))
            .parse_update()
            .unwrap();
        let res = UpdateRewriter::new(ctx.clone()).rewrite(&ast, &config);
        assert!(
            res.is_ok(),
            "Expected success for ADD SILENT with nonexistent source graph"
        );
        assert!(res.unwrap().operations().is_empty());
    }

    #[test]
    fn test_copy_silent_vs_non_silent() {
        let ctx = test_context_view();
        let config =
            test_config_with_available_graphs(vec!["http://example.org/existing"]);

        let non_silent =
            "COPY <http://example.org/nonexistent> TO <http://example.org/existing>";
        let ast = SparqlParser::new(non_silent, Arc::clone(ctx.functions()))
            .parse_update()
            .unwrap();
        let res = UpdateRewriter::new(ctx.clone()).rewrite(&ast, &config);
        assert!(
            res.is_err(),
            "Expected error for non-silent COPY with nonexistent source graph"
        );

        let silent = "COPY SILENT <http://example.org/nonexistent> TO <http://example.org/existing>";
        let ast = SparqlParser::new(silent, Arc::clone(ctx.functions()))
            .parse_update()
            .unwrap();
        let res = UpdateRewriter::new(ctx.clone()).rewrite(&ast, &config);
        assert!(
            res.is_ok(),
            "Expected success for COPY SILENT with nonexistent source graph"
        );
        assert!(res.unwrap().operations().is_empty());
    }

    #[test]
    fn test_move_silent_vs_non_silent() {
        let ctx = test_context_view();
        let config =
            test_config_with_available_graphs(vec!["http://example.org/existing"]);

        let non_silent =
            "MOVE <http://example.org/nonexistent> TO <http://example.org/existing>";
        let ast = SparqlParser::new(non_silent, Arc::clone(ctx.functions()))
            .parse_update()
            .unwrap();
        let res = UpdateRewriter::new(ctx.clone()).rewrite(&ast, &config);
        assert!(
            res.is_err(),
            "Expected error for non-silent MOVE with nonexistent source graph"
        );

        let silent = "MOVE SILENT <http://example.org/nonexistent> TO <http://example.org/existing>";
        let ast = SparqlParser::new(silent, Arc::clone(ctx.functions()))
            .parse_update()
            .unwrap();
        let res = UpdateRewriter::new(ctx.clone()).rewrite(&ast, &config);
        assert!(
            res.is_ok(),
            "Expected success for MOVE SILENT with nonexistent source graph"
        );
        assert!(res.unwrap().operations().is_empty());
    }
}
