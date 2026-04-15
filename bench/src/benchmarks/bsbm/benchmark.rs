use crate::benchmarks::bsbm::operation::list_raw_operations;
use crate::benchmarks::bsbm::report::{BsbmReport, ExploreReportBuilder, QueryDetails};
use crate::benchmarks::bsbm::requirements::{
    copy_pre_generated_queries, download_bsbm_tools, generate_dataset_requirement,
};
use crate::benchmarks::bsbm::use_case::BsbmUseCase;
use crate::benchmarks::bsbm::{BusinessIntelligenceUseCase, ExploreUseCase, NumProducts};
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::BenchmarkContext;
use crate::operation::{SparqlOperation, SparqlRawOperation};
use crate::prepare::PrepRequirement;
use crate::report::BenchmarkReport;
use crate::utils::print_store_stats;
use async_trait::async_trait;
use rdf_fusion::execution::ingest::RdfParserOptions;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::store::Store;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

/// Holds file paths for the files required for executing a BSBM run.
#[derive(Clone)]
struct BsbmFilePaths {
    /// A path to the dataset NTriples file.
    dataset: PathBuf,
    /// A path to the csv file that contains the pre-generated queries.
    queries: PathBuf,
}

/// The [Berlin SPARQL Benchmark](http://wbsg.informatik.uni-mannheim.de/bizer/berlinsparqlbenchmark/)
/// is a widely adopted benchmark built around an e-commerce use case.
///
/// This struct implements the logic for preparing and executing a BSBM benchmark. For that, it
/// requires a concrete [BsbmUseCase] implementation.
#[derive(Clone)]
pub struct BsbmBenchmark<TUseCase: BsbmUseCase> {
    /// The name of the benchmark.
    name: BenchmarkName,
    /// The number of products.
    num_products: NumProducts,
    /// How many queries to execute at most.
    max_query_count: Option<u64>,
    /// Path file.
    paths: BsbmFilePaths,
    /// The use case
    phantom_data: PhantomData<TUseCase>,
}

impl<TUseCase: BsbmUseCase> BsbmBenchmark<TUseCase> {
    /// Creates a new [BsbmBenchmark] with the given sizes.
    pub fn try_new(
        num_products: NumProducts,
        max_query_count: Option<u64>,
    ) -> anyhow::Result<Self> {
        let dataset_path = PathBuf::from("./dataset.nt".to_string());
        let queries_path = TUseCase::queries_file_path();
        let paths = BsbmFilePaths {
            dataset: dataset_path,
            queries: queries_path,
        };

        Ok(Self {
            name: TUseCase::name().into_benchmark_name(num_products, max_query_count),
            num_products,
            max_query_count,
            paths,
            phantom_data: PhantomData,
        })
    }

    /// The BSBM generator produces a list of queries that are tailored to the generated data. This
    /// method returns a list of these queries that should be executed during this run.
    pub fn list_operations(
        &self,
        ctx: &BenchmarkContext,
    ) -> anyhow::Result<Vec<SparqlOperation<TUseCase::QueryName>>> {
        println!("Loading queries ...");

        let result = match self.max_query_count {
            None => self
                .list_raw_operations(ctx)?
                .map(|q| q.parse().unwrap())
                .collect(),
            Some(max_query_count) => self
                .list_raw_operations(ctx)?
                .map(|q| q.parse().unwrap())
                .take(usize::try_from(max_query_count)?)
                .collect(),
        };

        println!("Queries loaded.");
        Ok(result)
    }

    /// The BSBM generator produces a list of queries that are tailored to the generated data. This
    /// method returns a list of these queries that should be executed during this run.
    pub fn list_raw_operations(
        &self,
        ctx: &BenchmarkContext,
    ) -> anyhow::Result<impl Iterator<Item = SparqlRawOperation<TUseCase::QueryName>>>
    {
        let queries_path = ctx.parent().join_data_dir(&self.paths.queries)?;
        let result = list_raw_operations::<TUseCase::QueryName>(queries_path.clone())?
            .map(|q| match q {
                SparqlRawOperation::Query(name, text) => {
                    SparqlRawOperation::Query(name, text.replace(" #", ""))
                }
            });
        Ok(result)
    }
}

#[async_trait]
impl<TUseCase: BsbmUseCase + 'static> Benchmark for BsbmBenchmark<TUseCase> {
    fn name(&self) -> BenchmarkName {
        self.name
    }

    #[allow(clippy::expect_used)]
    fn requirements(&self, bench_files_path: &Path) -> Vec<PrepRequirement> {
        vec![
            download_bsbm_tools(),
            generate_dataset_requirement(self.paths.dataset.clone(), self.num_products),
            copy_pre_generated_queries(
                bench_files_path,
                "explore",
                ExploreUseCase::queries_file_path(),
                self.num_products,
            ),
            copy_pre_generated_queries(
                bench_files_path,
                "businessIntelligence",
                BusinessIntelligenceUseCase::queries_file_path(),
                self.num_products,
            ),
        ]
    }

    async fn prepare_store(
        &self,
        ctx: &BenchmarkContext<'_>,
        print_info: bool,
    ) -> anyhow::Result<Store> {
        let start = datafusion::common::instant::Instant::now();
        if print_info {
            println!("Creating store and loading data ...");
        }

        let dataset_path = ctx.parent().join_data_dir(&self.paths.dataset)?;
        let data = tokio::fs::File::open(&dataset_path).await?;

        let memory_store = ctx.parent().create_store().await;
        memory_store
            .load_from_reader(data, RdfParserOptions::with_format(RdfFormat::NTriples))
            .await?;
        let duration = start.elapsed();

        if print_info {
            println!(
                "Store created and data loaded. Took {} ms.",
                duration.as_millis()
            );
        }

        let start = datafusion::common::instant::Instant::now();
        memory_store.optimize().await?;

        if print_info {
            let duration = start.elapsed();
            println!("Store optimized. Took {} ms.", duration.as_millis());
            print_store_stats(&memory_store).await?;
        }

        Ok(memory_store)
    }

    async fn execute(
        &self,
        bench_context: &BenchmarkContext<'_>,
    ) -> anyhow::Result<Box<dyn BenchmarkReport>> {
        let operations = self.list_operations(bench_context)?;
        let memory_store = self.prepare_store(bench_context, true).await?;
        let report =
            execute_benchmark::<TUseCase>(bench_context, operations, &memory_store)
                .await?;

        Ok(Box::new(report))
    }
}

async fn execute_benchmark<TUseCase: BsbmUseCase>(
    context: &BenchmarkContext<'_>,
    operations: Vec<SparqlOperation<TUseCase::QueryName>>,
    memory_store: &Store,
) -> anyhow::Result<BsbmReport<TUseCase>> {
    println!("Evaluating queries ...");

    let mut report = ExploreReportBuilder::new();
    let len = operations.len();
    for (idx, operation) in operations.iter().enumerate() {
        if idx % 25 == 0 {
            println!("Progress: {idx}/{len}");
        }

        run_operation(context, &mut report, memory_store, operation).await?;
    }
    let report = report.build();

    println!("Progress: {len}/{len}");
    println!("All queries evaluated.");

    Ok(report)
}

/// Executes a single [SparqlOperation] and stores the results of the profiling in the `report`.
async fn run_operation<TUseCase: BsbmUseCase>(
    context: &BenchmarkContext<'_>,
    report: &mut ExploreReportBuilder<TUseCase>,
    store: &Store,
    operation: &SparqlOperation<TUseCase::QueryName>,
) -> anyhow::Result<()> {
    let (run, explanation, num_results) = operation.run(store).await?;
    report.add_run(operation.query_name(), run.clone());
    if context.parent().options().verbose_results {
        let details = QueryDetails {
            query: operation.query().to_string(),
            query_type: operation.query_name().to_string(),
            total_time: run.duration,
            explanation,
            num_results,
        };
        report.add_explanation(details);
    }
    Ok(())
}
