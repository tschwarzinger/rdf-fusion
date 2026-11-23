use crate::RdfFusionContext;
use crate::planner::RdfFusionPlanner;
use crate::results::{QueryResults, QuerySolutionStream, QueryTripleStream};
use crate::sparql::QueryEvaluationError;
use crate::sparql::{QueryExplanation, QueryOptions, QueryVariant, RdfFusionQuery};
use datafusion::arrow::datatypes::Schema;
use datafusion::common::instant::Instant;
use datafusion::execution::SessionState;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_expr_common::metrics::Time;
use datafusion::physical_plan::execute_stream;
use futures::StreamExt;
use rdf_fusion_common::{MeasurePoll, Variable};
use rdf_fusion_extensions::storage::QuadStorageSnapshot;
use std::sync::Arc;
use std::time::Duration;

/// Evaluates a SPARQL query and returns the results along with execution information.
///
/// Most users should refrain from directly using this function, as there are higher-level
/// abstractions that provide APIs for querying.
pub async fn evaluate_query(
    ctx: &RdfFusionContext,
    query: &RdfFusionQuery,
    options: QueryOptions,
) -> Result<(QueryResults, QueryExplanation), QueryEvaluationError> {
    evaluate_query_with_snapshot(ctx, query, options, ctx.storage().snapshot().await?)
        .await
}

/// Evaluates a SPARQL query over a specific snapshot and returns the results along with execution
/// information.
pub async fn evaluate_query_with_snapshot(
    ctx: &RdfFusionContext,
    query: &RdfFusionQuery,
    _options: QueryOptions,
    snapshot: Arc<dyn QuadStorageSnapshot>,
) -> Result<(QueryResults, QueryExplanation), QueryEvaluationError> {
    let session_state = QueryPlanningState {
        state: ctx.session_context().state(),
        planner: RdfFusionPlanner::new_with_snapshot(ctx.create_view(), snapshot),
    };

    match query.variant() {
        QueryVariant::Select => {
            let (stream, explanation) = Box::pin(logical_plan_to_stream(
                session_state,
                query.logical_plan().clone(),
            ))
            .await?;
            Ok((QueryResults::Solutions(stream), explanation))
        }
        QueryVariant::Construct { template } => {
            let (stream, explanation) = Box::pin(logical_plan_to_stream(
                session_state,
                query.logical_plan().clone(),
            ))
            .await?;
            Ok((
                QueryResults::Graph(QueryTripleStream::new(template.clone(), stream)),
                explanation,
            ))
        }
        QueryVariant::Ask => {
            let (mut stream, explanation) = Box::pin(logical_plan_to_stream(
                session_state,
                query.logical_plan().clone(),
            ))
            .await?;
            let count = stream.next().await;
            Ok((QueryResults::Boolean(count.is_some()), explanation))
        }
        QueryVariant::Describe { template } => {
            let (stream, explanation) = Box::pin(logical_plan_to_stream(
                session_state,
                query.logical_plan().clone(),
            ))
            .await?;
            Ok((
                QueryResults::Graph(QueryTripleStream::new(template.clone(), stream)),
                explanation,
            ))
        }
    }
}

/// The state that is required for planning and executing a single query.
struct QueryPlanningState {
    /// The session state of the query. Like DataFusion's [`SessionContext`], each query uses a
    /// clone of the session state, which carries per-query state (e.g., the alias generator).
    ///
    /// [`SessionContext`]: datafusion::prelude::SessionContext
    state: SessionState,
    /// The planner that uses the snapshot of the query.
    planner: RdfFusionPlanner,
}

/// Converts a LogicalPlan to a stream of query solutions.
async fn logical_plan_to_stream(
    planning_state: QueryPlanningState,
    logical_plan: LogicalPlan,
) -> Result<(QuerySolutionStream, QueryExplanation), QueryEvaluationError> {
    let QueryPlanningState { state, planner } = planning_state;
    let task = state.task_ctx();

    let planning_compute = Time::new();
    let handle = planning_compute.timer();

    let planning_time_start = Instant::now();
    let optimized_plan = state.optimize(&logical_plan)?;
    drop(handle); // Add synchronous computation to the planning time

    let physical_plan_future = planner.create_physical_plan(&optimized_plan, &state);
    let physical_plan = MeasurePoll {
        inner: Box::pin(physical_plan_future),
        time_metric: planning_compute.clone(),
    }
    .await?;

    let planning_compute_nanos =
        u64::try_from(planning_compute.value()).unwrap_or(u64::MAX);
    let explanation = QueryExplanation {
        planning_latency: planning_time_start.elapsed(),
        planning_compute: Duration::from_nanos(planning_compute_nanos),
        initial_logical_plan: logical_plan,
        optimized_logical_plan: optimized_plan,
        execution_plan: Arc::clone(&physical_plan),
    };

    let variables = create_variables(&physical_plan.schema());

    let batch_record_stream = execute_stream(physical_plan, task)?;
    let stream = QuerySolutionStream::try_new(variables, batch_record_stream)?;
    Ok((stream, explanation))
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
