use crate::sparql::error::QueryEvaluationError;
use crate::sparql::rewriting::GraphPatternRewriter;
use crate::sparql::{
    DatasetOptions, QueryDataset, RdfFusionQuery, RdfFusionUpdate, UpdateOperation,
};
use datafusion::logical_expr::LogicalPlan;
use itertools::izip;
use rdf_fusion_common::sparql::algebra::GraphPattern;
use rdf_fusion_common::sparql::term::GroundTerm;
use rdf_fusion_common::sparql::{GraphUpdateOperation, Query, QueryVariant, Update};
use rdf_fusion_common::{GraphName, Quad, Term, TriplePattern, Variable};
use rdf_fusion_encoding::{EncodingName, quads_to_plain_term_record_batch};
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;

/// Plans a parsed SPARQL [Query] into a datafusion-native [RdfFusionQuery].
///
/// The base IRI used during rewriting is the one resolved by the parser (taking into account any
/// `BASE` directive in the query), which is carried in the parsed [Query].
///
/// The planning still relies on the SPARQL algebra produced by spargebra under the hood.
pub fn plan_query(
    builder_context: RdfFusionLogicalPlanBuilderContext,
    query: Query,
    output_encoding_name: Option<EncodingName>,
    dataset_overrides: &DatasetOptions,
) -> Result<RdfFusionQuery, QueryEvaluationError> {
    match &query {
        Query::Select {
            pattern,
            base_iri: query_base_iri,
            ..
        } => {
            let dataset = query_dataset(&query, dataset_overrides);
            let plan = rewrite_pattern(
                &builder_context,
                dataset,
                query_base_iri.clone(),
                output_encoding_name,
                pattern,
            )?;
            Ok(RdfFusionQuery::new(plan, QueryVariant::Select))
        }
        Query::Construct {
            template,
            pattern,
            base_iri: query_base_iri,
            ..
        } => {
            let dataset = query_dataset(&query, dataset_overrides);
            let plan = rewrite_pattern(
                &builder_context,
                dataset,
                query_base_iri.clone(),
                output_encoding_name,
                pattern,
            )?;
            Ok(RdfFusionQuery::new(
                plan,
                QueryVariant::Construct {
                    template: template.clone(),
                },
            ))
        }
        Query::Ask {
            pattern,
            base_iri: query_base_iri,
            ..
        } => {
            let dataset = query_dataset(&query, dataset_overrides);
            let plan = rewrite_pattern(
                &builder_context,
                dataset,
                query_base_iri.clone(),
                output_encoding_name,
                pattern,
            )?;
            Ok(RdfFusionQuery::new(plan, QueryVariant::Ask))
        }
        Query::Describe {
            pattern,
            base_iri: query_base_iri,
            ..
        } => {
            // TODO: Research what a good DESCRIBE implementation would look like.
            let dataset = query_dataset(&query, dataset_overrides);
            let (pattern, template) = describe_pattern(pattern);
            let plan = rewrite_pattern(
                &builder_context,
                dataset,
                query_base_iri.clone(),
                output_encoding_name,
                &pattern,
            )?;
            Ok(RdfFusionQuery::new(
                plan,
                QueryVariant::Describe { template },
            ))
        }
    }
}

/// Plans a parsed SPARQL [Update] into a datafusion-native [RdfFusionUpdate].
pub fn plan_update(
    builder_context: RdfFusionLogicalPlanBuilderContext,
    update: Update,
    output_encoding_name: Option<EncodingName>,
    dataset_overrides: &DatasetOptions,
) -> Result<RdfFusionUpdate, QueryEvaluationError> {
    let mut operations = Vec::with_capacity(update.operations.len());
    for operation in &update.operations {
        operations.push(plan_update_operation(
            &builder_context,
            operation,
            output_encoding_name,
            dataset_overrides,
        )?);
    }
    Ok(RdfFusionUpdate::new(operations))
}

fn plan_update_operation(
    builder_context: &RdfFusionLogicalPlanBuilderContext,
    operation: &GraphUpdateOperation,
    output_encoding_name: Option<EncodingName>,
    dataset_overrides: &DatasetOptions,
) -> Result<UpdateOperation, QueryEvaluationError> {
    match operation {
        GraphUpdateOperation::InsertData { data } => {
            let quads: Vec<Quad> = data
                .iter()
                .map(|q| Quad {
                    subject: q.subject.clone(),
                    predicate: q.predicate.clone(),
                    object: q.object.clone(),
                    graph_name: convert_graph_name(q.graph_name.clone()),
                })
                .collect();
            Ok(UpdateOperation::InsertData {
                quads: quads_to_plain_term_record_batch(&quads),
            })
        }
        GraphUpdateOperation::DeleteData { data } => {
            let quads: Vec<Quad> = data
                .iter()
                .map(|q| Quad {
                    subject: rdf_fusion_common::NamedOrBlankNode::NamedNode(
                        q.subject.clone(),
                    ),
                    predicate: q.predicate.clone(),
                    object: convert_ground_term(q.object.clone()),
                    graph_name: convert_graph_name(q.graph_name.clone()),
                })
                .collect();
            Ok(UpdateOperation::DeleteData {
                quads: quads_to_plain_term_record_batch(&quads),
            })
        }
        GraphUpdateOperation::DeleteInsert {
            delete,
            insert,
            using,
            pattern,
        } => {
            let dataset = QueryDataset::from_algebra(using);
            let dataset = apply_dataset_overrides(dataset, dataset_overrides);
            let pattern_plan = GraphPatternRewriter::new(
                builder_context.clone(),
                dataset,
                None,
                output_encoding_name,
            )
            .rewrite(pattern)
            .map_err(|e| e.context("Cannot rewrite DELETE/INSERT pattern"))?;
            Ok(UpdateOperation::DeleteInsert {
                delete: delete.clone(),
                insert: insert.clone(),
                pattern: pattern_plan,
            })
        }
        GraphUpdateOperation::Load {
            silent,
            source,
            destination,
        } => Ok(UpdateOperation::Load {
            silent: *silent,
            source: source.clone(),
            destination: convert_graph_name(destination.clone()),
        }),
        GraphUpdateOperation::Clear { silent, graph } => Ok(UpdateOperation::Clear {
            silent: *silent,
            graph: graph.clone(),
        }),
        GraphUpdateOperation::Drop { silent, graph } => Ok(UpdateOperation::Drop {
            silent: *silent,
            graph: graph.clone(),
        }),
        GraphUpdateOperation::Create { silent, graph } => Ok(UpdateOperation::Create {
            silent: *silent,
            graph: graph.clone(),
        }),
    }
}

/// Plans an [`Update`] using default options.
pub fn plan_update_with_options(
    builder_context: RdfFusionLogicalPlanBuilderContext,
    update: Update,
    _options: crate::sparql::UpdateOptions,
) -> Result<RdfFusionUpdate, QueryEvaluationError> {
    plan_update(builder_context, update, None, &DatasetOptions::default())
}

fn query_dataset(query: &Query, dataset_overrides: &DatasetOptions) -> QueryDataset {
    let inner = match query {
        Query::Select { dataset, .. }
        | Query::Construct { dataset, .. }
        | Query::Describe { dataset, .. }
        | Query::Ask { dataset, .. } => dataset,
    };
    apply_dataset_overrides(QueryDataset::from_algebra(inner), dataset_overrides)
}

fn apply_dataset_overrides(
    mut dataset: QueryDataset,
    overrides: &DatasetOptions,
) -> QueryDataset {
    if overrides.default_graph_as_union {
        dataset.set_default_graph_as_union();
    } else {
        if let Some(default_graphs) = &overrides.default_graphs {
            dataset.set_default_graph(
                default_graphs.iter().map(|g| g.clone().into()).collect(),
            );
        }
        if let Some(named_graphs) = &overrides.named_graphs {
            dataset.set_available_named_graphs(
                named_graphs.iter().map(|g| g.clone().into()).collect(),
            );
        }
    }
    dataset
}

fn rewrite_pattern(
    builder_context: &RdfFusionLogicalPlanBuilderContext,
    dataset: QueryDataset,
    base_iri: Option<rdf_fusion_common::Iri<String>>,
    output_encoding_name: Option<EncodingName>,
    pattern: &GraphPattern,
) -> Result<LogicalPlan, QueryEvaluationError> {
    GraphPatternRewriter::new(
        builder_context.clone(),
        dataset,
        base_iri,
        output_encoding_name,
    )
    .rewrite(pattern)
    .map_err(|e| QueryEvaluationError::from(e.context("Cannot rewrite SPARQL query")))
}

/// Builds the DESCRIBE graph pattern and its triple template.
fn describe_pattern(pattern: &GraphPattern) -> (GraphPattern, Vec<TriplePattern>) {
    let mut vars = Vec::new();
    pattern.on_in_scope_variable(|v| vars.push(v.clone()));
    let rdf_types = vars
        .iter()
        .map(|v| Variable::new(format!("{}__type", v.as_str())).unwrap())
        .collect::<Vec<_>>();

    let describe_template = izip!(vars, rdf_types.iter())
        .map(|(variable, rdf_type)| {
            vec![TriplePattern {
                subject: variable.clone().into(),
                predicate: rdf_fusion_common::vocab::rdf::TYPE.into_owned().into(),
                object: rdf_type.clone().into(),
            }]
            .into_iter()
        })
        .flatten()
        .collect::<Vec<_>>();

    let pattern = GraphPattern::Join {
        left: Box::new(pattern.clone()),
        right: Box::new(GraphPattern::Bgp {
            patterns: describe_template.clone(),
        }),
    };
    (pattern, describe_template)
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

fn convert_ground_term(term: GroundTerm) -> Term {
    match term {
        GroundTerm::NamedNode(n) => Term::NamedNode(n),
        GroundTerm::Literal(l) => Term::Literal(l),
    }
}
