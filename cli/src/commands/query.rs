use anyhow::Context;
use datafusion::common::instant::Instant;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::displayable;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::execution::sparql::{QueryOptions, RdfFusionQuery};
use rdf_fusion::store::Store;

/// Executes a SPARQL query against the given store and prints the result to stdout.
pub async fn query(
    store: Store,
    query_str: String,
    explain: bool,
    analyze: bool,
) -> anyhow::Result<()> {
    let parsed_query = RdfFusionQuery::parse(&query_str, None)
        .context("Failed to parse SPARQL query")?;

    if explain {
        let (results, explanation) = store
            .explain_query_opt(parsed_query, QueryOptions::default())
            .await?;

        println!(
            "\nPlanning Compute: {}ms",
            explanation.planning_compute.as_millis()
        );
        println!(
            "\nPlanning Latency: {}ms",
            explanation.planning_latency.as_millis()
        );

        if analyze {
            let start_execution = Instant::now();
            println!("\nResults:");
            results
                .write(std::io::stdout(), QueryResultsFormat::Tsv)
                .await?;
            let execution_latency = start_execution.elapsed();

            println!("\nExecution Plan:");
            println!(
                "{}",
                DisplayableExecutionPlan::with_metrics(
                    explanation.execution_plan.as_ref()
                )
                .indent(true)
            );

            println!("\nExecution Latency: {}ms", execution_latency.as_millis())
        } else {
            println!("\nExecution Plan:");
            println!(
                "{}",
                displayable(explanation.execution_plan.as_ref()).indent(false)
            );
        }
    } else {
        let results = store.query(parsed_query).await?;
        results
            .write(std::io::stdout(), QueryResultsFormat::Tsv)
            .await?;
    }

    Ok(())
}
