use crate::benchmarks::windfarm::queries::WindFarmQueryName;
use crate::report::BenchmarkReport;
use crate::runs::BenchmarkRun;
use anyhow::Context;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::display::DisplayableExecutionPlan;
use prettytable::{Table, row};
use rdf_fusion::execution::sparql::QueryExplanation;
use std::collections::HashMap;
use std::fs;
use std::io::Write;
use std::path::Path;

/// Holds details of a single
pub struct QueryDetails {
    /// The query itself
    pub query: String,
    /// The total time taken
    pub total_time: std::time::Duration,
    /// The explanation returned from the engine
    pub explanation: QueryExplanation,
    /// The number of results returned
    pub num_results: usize,
}

/// Stores the final report of executing a wind farm benchmark.
pub struct WindFarmReport {
    /// Stores the runs of the benchmark.
    runs: HashMap<WindFarmQueryName, BenchmarkRun>,
    /// Query details for each run.
    details: HashMap<WindFarmQueryName, QueryDetails>,
}

impl WindFarmReport {
    /// Writes a tabular summary of the query execution time.
    fn write_summary<W: Write + ?Sized>(&self, writer: &mut W) -> anyhow::Result<()> {
        // Create the table
        let mut table = Table::new();
        table.add_row(row!["Query", "Duration (ms)", "Number of Results"]);
        for query in WindFarmQueryName::list_queries() {
            let run = self.runs.get(&query).context("Cannot get run")?;
            let details = self.details.get(&query).context("Cannot get run")?;
            table.add_row(row![
                query.to_string(),
                run.duration.as_millis(),
                details.num_results
            ]);
        }
        table.print(writer)?;

        Ok(())
    }

    /// Writes the query details to disk in when verbose results are active.
    fn write_query_details(
        &self,
        output_directory: &Path,
        query_name: WindFarmQueryName,
    ) -> anyhow::Result<()> {
        let query_path =
            output_directory.join(query_name.file_name().replace(".sparql", ""));
        fs::create_dir_all(&query_path).context("Cannot create query directory")?;

        let details = self
            .details
            .get(&query_name)
            .context("Cannot get explanation")?;

        self.dump_query_text(&query_path.join("0_query.txt"), details)?;
        self.dump_query_result_summary(&query_path.join("1_summary.txt"), details)?;
        self.dump_logical_plan(
            &query_path.join("2_initial_logical_plan.txt"),
            &details.explanation.initial_logical_plan,
        )?;
        self.dump_logical_plan(
            &query_path.join("3_opt_logical_plan.txt"),
            &details.explanation.optimized_logical_plan,
        )?;
        self.dump_execution_plan(
            &query_path.join("4_execution_plan.txt"),
            details.explanation.execution_plan.as_ref(),
        )?;

        Ok(())
    }

    /// Dumps the query text for easier inspection.
    fn dump_query_text(
        &self,
        output_file: &Path,
        details: &QueryDetails,
    ) -> anyhow::Result<()> {
        fs::write(output_file, details.query.as_str()).with_context(|| {
            format!("Failed to dump query text to '{}'", output_file.display())
        })
    }

    /// Dumps a summary of the query exeuction.
    fn dump_query_result_summary(
        &self,
        output_file: &Path,
        details: &QueryDetails,
    ) -> anyhow::Result<()> {
        let text = format!(
            "\
Total Time: {:?}
Planning Latency: {:?}
Planning Compute: {}
",
            details.total_time,
            details.explanation.planning_latency,
            details.explanation.planning_compute
        );
        fs::write(output_file, text).with_context(|| {
            format!(
                "Failed to dump query result summary to '{}'",
                output_file.display()
            )
        })
    }

    /// Dumps a [LogicalPLan].
    fn dump_logical_plan(
        &self,
        output_file: &Path,
        plan: &LogicalPlan,
    ) -> anyhow::Result<()> {
        fs::write(output_file, format!("Initial Logical Plan:\n\n{plan}")).with_context(
            || {
                format!(
                    "Failed to write initial logical plan to {}",
                    output_file.display()
                )
            },
        )
    }

    /// Dumps an [ExecutionPlan].
    fn dump_execution_plan(
        &self,
        output_file: &Path,
        plan: &dyn ExecutionPlan,
    ) -> anyhow::Result<()> {
        let execution_plan = DisplayableExecutionPlan::with_metrics(plan).indent(false);
        fs::write(output_file, format!("Execution Plan:\n\n{execution_plan}"))
            .with_context(|| {
                format!(
                    "Failed to write execution plan to '{}'",
                    output_file.display()
                )
            })
    }
}

impl BenchmarkReport for WindFarmReport {
    fn write_results(&self, output_dir: &Path) -> anyhow::Result<()> {
        let summary_txt = output_dir.join("summary.txt");
        let mut summary_file =
            fs::File::create(summary_txt).context("Cannot create summary file")?;
        self.write_summary(&mut summary_file)
            .context("Cannot write summary")?;

        if !self.details.is_empty() {
            let queries_path = output_dir.join("queries");
            fs::create_dir_all(&queries_path)
                .context("Cannot create queries directory")?;
            for query_name in self.details.keys() {
                self.write_query_details(&queries_path, *query_name)
                    .context("Cannot write query details")?;
            }
        }

        Ok(())
    }
}

/// Builder for the [`WindFarmReport`].
///
/// This should only be accessible to the benchmark code.
pub(super) struct WindFarmReportBuilder {
    /// The inner report that is being built.
    report: WindFarmReport,
}

impl WindFarmReportBuilder {
    /// Creates a new builder.
    pub(super) fn new() -> Self {
        Self {
            report: WindFarmReport {
                runs: HashMap::new(),
                details: HashMap::new(),
            },
        }
    }

    /// Adds a run to a particular query.
    pub fn add_run(&mut self, name: WindFarmQueryName, run: BenchmarkRun) {
        self.report.runs.insert(name, run);
    }

    /// Adds a detail for a particular query.
    ///
    /// It is expected that the n-th call of this method is the detail of the n-th query.
    pub fn add_explanation(
        &mut self,
        query_name: WindFarmQueryName,
        details: QueryDetails,
    ) {
        self.report.details.insert(query_name, details);
    }

    /// Finalizes the report.
    pub fn build(self) -> WindFarmReport {
        self.report
    }
}
