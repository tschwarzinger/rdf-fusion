#![allow(dead_code)]

use datafusion::physical_plan::displayable;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::store::Store;

pub fn is_verbose() -> bool {
    let is_verbose = std::env::var("RDF_FUSION_CRITERION_VERBOSE")
        .map(|s| s == "1")
        .unwrap_or(false);

    if !is_verbose {
        println!(
            "Running in quiet mode. Set RDF_FUSION_CRITERION_VERBOSE=1 to enable verbose output."
        );
    }

    is_verbose
}

pub async fn print_query_details(
    store: &Store,
    query_options: QueryOptions,
    query_name: &str,
    query: &str,
) -> anyhow::Result<()> {
    println!("Executing query ({query_name}):");
    println!("\n========== SPARQL Query ==========\n");
    println!("{query}");
    println!("\n==================================\n\n");

    let (_, explanation) = store.explain_query_opt(query, query_options).await?;

    println!("\n========== Logical Plan ==========\n");
    println!("{}", explanation.initial_logical_plan);
    println!("\n==================================\n\n");

    println!("\n===== Optimized Logical Plan =====\n");
    println!("{}", explanation.optimized_logical_plan);
    println!("\n==================================\n\n");

    println!("\n========= Execution Plan =========\n");
    println!(
        "{}",
        displayable(explanation.execution_plan.as_ref()).indent(false)
    );
    println!("\n==================================\n\n");

    Ok(())
}
