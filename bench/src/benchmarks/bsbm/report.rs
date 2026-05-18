use crate::benchmarks::bsbm::use_case::BsbmUseCase;
use crate::report::BenchmarkReport;
use crate::runs::{BenchmarkRun, BenchmarkRuns};
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
    /// The query type
    pub query_type: String,
    /// The total time taken
    pub total_time: std::time::Duration,
    /// The explanation returned from the engine
    pub explanation: QueryExplanation,
    /// The number of results returned
    pub num_results: usize,
}

/// Stores the final report of executing a BSBM explore benchmark.
pub struct BsbmReport<TUseCase: BsbmUseCase> {
    /// Stores all runs of the benchmark grouped by the query name.
    /// A single query name can have multiple instances (with random variables) in BSBM.
    runs: HashMap<TUseCase::QueryName, BenchmarkRuns>,
    /// Query details for each run.
    details: Vec<QueryDetails>,
}

impl<TUseCase: BsbmUseCase> BsbmReport<TUseCase> {
    /// Writes a tabular summary of the query execution time.
    fn write_summary<W: Write + ?Sized>(&self, writer: &mut W) -> anyhow::Result<()> {
        // Create the table
        let mut table = Table::new();
        table.add_row(row![
            "Query",
            "Samples",
            "Average Duration",
            "Average Results"
        ]);
        for query in TUseCase::list_queries() {
            let summary = self
                .runs
                .get(&query)
                .map(BenchmarkRuns::summarize)
                .transpose()?;

            let samples = summary
                .as_ref()
                .map_or_else(|| "-".to_owned(), |s| s.number_of_samples.to_string());
            let average_duration = summary
                .as_ref()
                .map_or_else(|| "-".to_owned(), |s| format!("{:?}", s.avg_duration));

            let details = self
                .details
                .iter()
                .filter(|d| d.query_type == query.to_string())
                .collect::<Vec<_>>();
            let total_results = details.iter().map(|d| d.num_results).sum::<usize>();
            let average_results = total_results as f64 / details.len() as f64;

            table.add_row(row![
                query.to_string(),
                samples,
                average_duration,
                average_results
            ]);
        }
        table.print(writer)?;

        Ok(())
    }

    /// Writes a csv file that contains detailed information.
    fn write_details<W: Write + ?Sized>(&self, writer: &mut W) -> anyhow::Result<()> {
        let mut writer = csv::Writer::from_writer(writer);

        writer.write_record(["id", "type", "duration (us)", "number of results"])?;
        for (i, details) in self.details.iter().enumerate() {
            writer.write_record([
                i.to_string(),
                details.query_type.to_string(),
                details.total_time.as_micros().to_string(),
                details.num_results.to_string(),
            ])?;
        }

        Ok(())
    }

    /// Writes the query details to disk in when verbose results are active.
    fn write_query_details(
        &self,
        output_directory: &Path,
        index: usize,
    ) -> anyhow::Result<()> {
        let query_i_path = output_directory.join(format!("query{index}"));
        fs::create_dir_all(&query_i_path).context("Cannot create query directory")?;

        let details = self.details.get(index).context("Cannot get explanation")?;

        self.dump_query_text(&query_i_path.join("0_query.txt"), details)?;
        self.dump_query_result_summary(&query_i_path.join("1_summary.txt"), details)?;
        self.dump_logical_plan(
            &query_i_path.join("2_initial_logical_plan.txt"),
            &details.explanation.initial_logical_plan,
        )?;
        self.dump_logical_plan(
            &query_i_path.join("3_opt_logical_plan.txt"),
            &details.explanation.optimized_logical_plan,
        )?;
        self.dump_execution_plan(
            &query_i_path.join("4_execution_plan.txt"),
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
Query Type: {:?}
Total Time: {:?}
Planning Latency: {:?}
Planning Compute: {:?}
",
            details.query_type,
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

    /// Dumps a [`LogicalPlan`].
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

impl<TUseCase: BsbmUseCase> BenchmarkReport for BsbmReport<TUseCase> {
    fn write_results(&self, output_dir: &Path) -> anyhow::Result<()> {
        let summary_txt = output_dir.join("summary.txt");
        let mut summary_file = fs::File::create(summary_txt)?;
        self.write_summary(&mut summary_file)?;

        let details_csv = output_dir.join("details.csv");
        let mut details_file = fs::File::create(details_csv)?;
        self.write_details(&mut details_file)?;

        if !self.details.is_empty() {
            let queries_path = output_dir.join("queries");
            fs::create_dir_all(&queries_path)
                .context("Cannot create queries directory")?;
            for i in 0..self.details.len() {
                self.write_query_details(&queries_path, i)?;
            }
        }

        Ok(())
    }
}

/// Builder for the [`BsbmReport`].
///
/// This should only be accessible to the benchmark code.
pub(super) struct ExploreReportBuilder<TUseCase: BsbmUseCase> {
    /// The inner report that is being built.
    report: BsbmReport<TUseCase>,
}

impl<TUseCase: BsbmUseCase> ExploreReportBuilder<TUseCase> {
    /// Creates a new builder.
    pub(super) fn new() -> Self {
        Self {
            report: BsbmReport {
                runs: HashMap::new(),
                details: Vec::new(),
            },
        }
    }

    /// Adds a run to a particular query.
    pub fn add_run(&mut self, name: TUseCase::QueryName, run: BenchmarkRun) {
        let runs = self.report.runs.entry(name).or_default();
        runs.add_run(run);
    }

    /// Adds a detail for a particular query.
    ///
    /// It is expected that the n-th call of this method is the detail of the n-th query.
    pub fn add_explanation(&mut self, details: QueryDetails) {
        self.report.details.push(details)
    }

    /// Finalizes the report.
    pub fn build(self) -> BsbmReport<TUseCase> {
        self.report
    }
}
