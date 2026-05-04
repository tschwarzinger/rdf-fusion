use crate::RdfFusionContext;
use crate::planner::RdfFusionPlanner;
use crate::results::{QueryResults, QuerySolutionStream, QueryTripleStream};
use crate::sparql::error::QueryEvaluationError;
use crate::sparql::optimizer::{create_optimizer_rules, create_pyhsical_optimizer_rules};
use crate::sparql::rewriting::GraphPatternRewriter;
use crate::sparql::{QueryDataset, QueryExplanation, QueryOptions, RdfFusionQuery};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::instant::Instant;
use datafusion::execution::{SessionState, SessionStateBuilder};
use datafusion::physical_expr_common::metrics::Time;
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use futures::StreamExt;
use itertools::izip;
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;
use rdf_fusion_model::Variable;
use rdf_fusion_model::sparql::Query;
use rdf_fusion_model::sparql::algebra::GraphPattern;
use rdf_fusion_model::{Iri, TriplePattern};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// Evaluates a SPARQL query and returns the results along with execution information.
///
/// Most users should refrain from directly using this function, as there are higher-level
/// abstractions that provide APIs for querying.
pub async fn evaluate_query(
    ctx: &RdfFusionContext,
    builder_context: RdfFusionLogicalPlanBuilderContext,
    query: &RdfFusionQuery,
    options: QueryOptions,
) -> Result<(QueryResults, QueryExplanation), QueryEvaluationError> {
    evaluate_query_with_snapshot(
        ctx,
        builder_context,
        query,
        options,
        ctx.storage().snapshot().await?,
    )
    .await
}

/// Evaluates a SPARQL query over a specific snapshot and returns the results along with execution
/// information.
pub async fn evaluate_query_with_snapshot(
    ctx: &RdfFusionContext,
    builder_context: RdfFusionLogicalPlanBuilderContext,
    query: &RdfFusionQuery,
    options: QueryOptions,
    snapshot: Arc<dyn QuadStorageSnapshot>,
) -> Result<(QueryResults, QueryExplanation), QueryEvaluationError> {
    let session_state = SessionStateBuilder::from(ctx.session_context().state())
        .with_optimizer_rules(create_optimizer_rules(
            ctx.create_view(),
            options.optimization_level,
        ))
        .with_physical_optimizer_rules(create_pyhsical_optimizer_rules(
            options.optimization_level,
        ))
        .with_query_planner(Arc::new(RdfFusionPlanner::new_with_snapshot(
            ctx.create_view(),
            snapshot,
        )))
        .build();

    match &query.inner {
        Query::Select {
            pattern, base_iri, ..
        } => {
            let (stream, explanation) = Box::pin(graph_pattern_to_stream(
                session_state,
                builder_context,
                query,
                pattern,
                base_iri,
            ))
            .await?;
            Ok((QueryResults::Solutions(stream), explanation))
        }
        Query::Construct {
            template,
            pattern,
            base_iri,
            ..
        } => {
            let (stream, explanation) = Box::pin(graph_pattern_to_stream(
                session_state,
                builder_context,
                query,
                pattern,
                base_iri,
            ))
            .await?;
            Ok((
                QueryResults::Graph(QueryTripleStream::new(template.clone(), stream)),
                explanation,
            ))
        }
        Query::Ask {
            pattern, base_iri, ..
        } => {
            let (mut stream, explanation) = Box::pin(graph_pattern_to_stream(
                session_state,
                builder_context,
                query,
                pattern,
                base_iri,
            ))
            .await?;
            let count = stream.next().await;
            Ok((QueryResults::Boolean(count.is_some()), explanation))
        }
        Query::Describe {
            pattern, base_iri, ..
        } => {
            // TODO: Research what a good DESCRIBE implementation would look like.

            let mut vars = Vec::new();
            pattern.on_in_scope_variable(|v| vars.push(v.clone()));
            let rdf_types = vars
                .iter()
                .map(|v| Variable::new(format!("{}__type", v.as_str())).unwrap())
                .collect::<Vec<_>>();

            let describe_pattern = izip!(vars, rdf_types.iter())
                .map(|(variable, rdf_type)| {
                    vec![TriplePattern {
                        subject: variable.clone().into(),
                        predicate: rdf_fusion_model::vocab::rdf::TYPE.into_owned().into(),
                        object: rdf_type.clone().into(),
                    }]
                    .into_iter()
                })
                .flatten()
                .collect::<Vec<_>>();

            // Compute the label / comment results
            let pattern = GraphPattern::Join {
                left: Box::new(pattern.clone()),
                right: Box::new(GraphPattern::Bgp {
                    patterns: describe_pattern.clone(),
                }),
            };
            let (stream, explanation) = Box::pin(graph_pattern_to_stream(
                session_state,
                builder_context,
                query,
                &pattern,
                base_iri,
            ))
            .await?;

            Ok((
                QueryResults::Graph(QueryTripleStream::new(describe_pattern, stream)),
                explanation,
            ))
        }
    }
}

/// Converts a SPARQL graph pattern to a stream of query solutions.
async fn graph_pattern_to_stream(
    state: SessionState,
    builder_context: RdfFusionLogicalPlanBuilderContext,
    query: &RdfFusionQuery,
    pattern: &GraphPattern,
    base_iri: &Option<Iri<String>>,
) -> Result<(QuerySolutionStream, QueryExplanation), QueryEvaluationError> {
    let task = state.task_ctx();

    let (execution_plan, explanation) =
        create_execution_plan(state, builder_context, &query.dataset, pattern, base_iri)
            .await?;
    let variables = create_variables(&execution_plan.schema());

    let batch_record_stream = execute_stream(execution_plan, task)?;
    let stream = QuerySolutionStream::try_new(variables, batch_record_stream)?;
    Ok((stream, explanation))
}

/// Creates a physical execution plan from a SPARQL graph pattern, doing further processing on the
/// resulting query plan (e.g., optimization).
async fn create_execution_plan(
    state: SessionState,
    builder_context: RdfFusionLogicalPlanBuilderContext,
    dataset: &QueryDataset,
    pattern: &GraphPattern,
    base_iri: &Option<Iri<String>>,
) -> Result<(Arc<dyn ExecutionPlan>, QueryExplanation), QueryEvaluationError> {
    let planning_compute = Time::new();
    let handle = planning_compute.timer();

    let planning_time_start = Instant::now();
    let logical_plan =
        GraphPatternRewriter::new(builder_context, dataset.clone(), base_iri.clone())
            .rewrite(pattern)
            .map_err(|e| e.context("Cannot rewrite SPARQL query"))?;
    let optimized_plan = state.optimize(&logical_plan)?;
    drop(handle); // Add synchronous computation to the planning time

    let physical_plan_future = state
        .query_planner()
        .create_physical_plan(&optimized_plan, &state);
    let physical_plan = MeasurePoll {
        inner: Box::pin(physical_plan_future),
        time_metric: planning_compute.clone(),
    }
    .await?;

    let explanation = QueryExplanation {
        planning_latency: planning_time_start.elapsed(),
        planning_compute,
        initial_logical_plan: logical_plan,
        optimized_logical_plan: optimized_plan,
        execution_plan: Arc::clone(&physical_plan),
    };
    Ok((Arc::clone(&physical_plan), explanation))
}

#[allow(clippy::expect_used)]
fn create_variables(schema: &Schema) -> Arc<[Variable]> {
    schema
        .fields()
        .iter()
        .map(|f| Variable::new(f.name()).expect("Variables already checked."))
        .collect::<Vec<_>>()
        .into()
}

/// A wrapper that measures strictly the CPU time spent actively polling.
struct MeasurePoll<F> {
    inner: Pin<Box<F>>,
    time_metric: Time,
}

impl<F: Future> Future for MeasurePoll<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let cloned_inner = self.time_metric.clone();
        let handle = cloned_inner.timer();
        let result = self.inner.as_mut().poll(cx);
        drop(handle);
        result
    }
}
