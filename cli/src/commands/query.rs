use anyhow::bail;
use datafusion::arrow::util::display::FormatOptions;
use datafusion::arrow::util::pretty::pretty_format_batches_with_options;
use datafusion::catalog::Session;
use datafusion::common::instant::Instant;
use datafusion::logical_expr::extension_types::DFArrayFormatterFactory;
use datafusion::physical_plan::common::collect;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use datafusion::physical_plan::displayable;
use rdf_fusion::common::RdfFormat;
use rdf_fusion::execution::results::{QueryResults, QueryResultsFormat};
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::store::Store;
use std::io::Write;
use std::str::FromStr;
use std::sync::Arc;

/// The format in which the `query` command prints the results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResultsOutputFormat {
    /// A human-readable table. RDF terms are printed in N-Triples syntax by the formatters of the
    /// RDF term extension types.
    Table,
    /// A SPARQL results format.
    Sparql(QueryResultsFormat),
}

/// Parses the `--format` argument of the `query` command.
impl FromStr for ResultsOutputFormat {
    type Err = anyhow::Error;

    fn from_str(format: &str) -> anyhow::Result<Self> {
        if format.eq_ignore_ascii_case("table") {
            return Ok(Self::Table);
        }
        match QueryResultsFormat::from_extension(format) {
            Some(format) => Ok(Self::Sparql(format)),
            None => bail!(
                "Unknown results format '{format}'. Supported formats: table, tsv, csv, json, xml."
            ),
        }
    }
}

/// Executes a SPARQL query against the given store and prints the result to stdout.
pub async fn query(
    store: Store,
    query_str: String,
    explain: bool,
    analyze: bool,
    format: ResultsOutputFormat,
) -> anyhow::Result<()> {
    if explain {
        let (results, explanation) = store
            .explain_query_opt(&query_str, QueryOptions::default())
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
            print_results(&store, results, format).await?;
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
            println!("\nLogical Plan:");
            println!("{}", explanation.optimized_logical_plan);

            println!("\n\nExecution Plan:");
            println!(
                "{}",
                displayable(explanation.execution_plan.as_ref()).indent(false)
            );
        }
    } else {
        let results = store.query(&query_str).await?;
        print_results(&store, results, format).await?;
    }

    Ok(())
}

/// Prints the `results` to stdout in the given `format`.
async fn print_results(
    store: &Store,
    results: QueryResults,
    format: ResultsOutputFormat,
) -> anyhow::Result<()> {
    let stdout = std::io::stdout();
    match (format, results) {
        (ResultsOutputFormat::Sparql(format), results) => {
            results.write(stdout, format).await?;
        }
        (ResultsOutputFormat::Table, QueryResults::Solutions(solutions)) => {
            let batches = collect(solutions.into_record_batch_stream()?).await?;

            // Use the formatters of the extension types registered in the session (e.g., for
            // printing plain terms in N-Triples syntax).
            let state = store.context().session_context().state();
            let formatter_factory =
                DFArrayFormatterFactory::new(Arc::clone(state.extension_type_registry()));
            let options =
                FormatOptions::default().with_formatter_factory(Some(&formatter_factory));
            let table = pretty_format_batches_with_options(&batches, &options)?;
            writeln!(stdout.lock(), "{table}")?;
        }
        (ResultsOutputFormat::Table, QueryResults::Boolean(value)) => {
            writeln!(stdout.lock(), "{value}")?;
        }
        (ResultsOutputFormat::Table, results @ QueryResults::Graph(_)) => {
            results.write_graph(stdout, RdfFormat::NTriples).await?;
        }
    }
    Ok(())
}
