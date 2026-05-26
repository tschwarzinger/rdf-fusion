use crate::BenchQuadStorageType;
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
use rdf_fusion::common::{RdfFormat, RdfSortOrder};
use rdf_fusion::storage::rdf_files::{RdfFileScanOptions, RdfFileSourceConfig};
use rdf_fusion::store::Store;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use url::Url;

/// Holds file paths for the files required for executing a BSBM run.
#[derive(Clone)]
struct BsbmFilePaths {
    /// A path to the bsbmtools directory.
    bsbmtools: PathBuf,
    /// A path to the td_data directory.
    td_data: PathBuf,
    /// A path to the dataset NTriples file.
    dataset: PathBuf,
    /// A path to the csv file that contains the pre-generated queries.
    queries: PathBuf,
    /// A path to the source of the explore queries.
    queries_explore_source: PathBuf,
    /// A path to the target of the explore queries.
    queries_explore_target: PathBuf,
    /// A path to the source of the business intelligence queries.
    queries_bi_source: PathBuf,
    /// A path to the target of the business intelligence queries.
    queries_bi_target: PathBuf,
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
        context: &BenchmarkContext,
        num_products: NumProducts,
        max_query_count: Option<u64>,
    ) -> anyhow::Result<Self> {
        let bsbmtools = context.data_dir().join("bsbmtools");
        let td_data = context.data_dir().join("td_data");
        let dataset_path = context.data_dir().join("dataset.nt");
        let queries_path = context.data_dir().join(TUseCase::queries_file_path());

        let queries_explore_source = context
            .parent()
            .bench_files_dir()
            .join("bsbm_queries")
            .join(format!("explore-{num_products}.csv.bz2"));
        let queries_explore_target =
            context.data_dir().join(ExploreUseCase::queries_file_path());

        let queries_bi_source = context
            .parent()
            .bench_files_dir()
            .join("bsbm_queries")
            .join(format!("businessIntelligence-{num_products}.csv.bz2"));
        let queries_bi_target = context
            .data_dir()
            .join(BusinessIntelligenceUseCase::queries_file_path());

        let paths = BsbmFilePaths {
            bsbmtools,
            td_data,
            dataset: dataset_path,
            queries: queries_path,
            queries_explore_source,
            queries_explore_target,
            queries_bi_source,
            queries_bi_target,
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
        _ctx: &BenchmarkContext,
    ) -> anyhow::Result<impl Iterator<Item = SparqlRawOperation<TUseCase::QueryName>>>
    {
        let queries_path = self.paths.queries.clone();
        let result =
            list_raw_operations::<TUseCase::QueryName>(queries_path)?.map(|q| match q {
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
    fn requirements(&self, _bench_files_path: &Path) -> Vec<PrepRequirement> {
        vec![
            download_bsbm_tools(self.paths.bsbmtools.clone()),
            generate_dataset_requirement(
                self.paths.bsbmtools.clone(),
                self.paths.dataset.clone(),
                self.paths.td_data.clone(),
                self.num_products,
            ),
            copy_pre_generated_queries(
                self.paths.queries_explore_source.clone(),
                self.paths.queries_explore_target.clone(),
            ),
            copy_pre_generated_queries(
                self.paths.queries_bi_source.clone(),
                self.paths.queries_bi_target.clone(),
            ),
        ]
    }

    async fn prepare_store(
        &self,
        ctx: &BenchmarkContext<'_>,
        print_info: bool,
    ) -> anyhow::Result<Store> {
        match &ctx.parent().options().storage_type {
            BenchQuadStorageType::Delta => {
                self.prepare_delta_store(ctx, print_info).await
            }
            BenchQuadStorageType::Parquet { sort_order } => {
                self.prepare_parquet_store(ctx, print_info, sort_order.clone())
                    .await
            }
        }
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

impl<TUseCase: BsbmUseCase> BsbmBenchmark<TUseCase> {
    async fn prepare_parquet_store(
        &self,
        ctx: &BenchmarkContext<'_>,
        print_info: bool,
        sort_order: Option<RdfSortOrder>,
    ) -> anyhow::Result<Store> {
        if print_info {
            println!("Generating Parquet dataset ...");
        }
        let url = ctx.resolve_path_to_url(&self.paths.dataset)?;

        let source = RdfFileSourceConfig {
            url: Url::parse(&url)?,
            format: RdfFormat::NTriples,
        };

        ctx.dump_to_parquet(
            vec![(rdf_fusion::common::GraphName::DefaultGraph, source)],
            sort_order,
        )
        .await?;

        if print_info {
            println!("Parquet dataset generated.");
        }
        Ok(ctx.create_store().await)
    }

    async fn prepare_delta_store(
        &self,
        ctx: &BenchmarkContext<'_>,
        print_info: bool,
    ) -> anyhow::Result<Store> {
        let start = datafusion::common::instant::Instant::now();
        if print_info {
            println!("Creating store and loading data ...");
        }

        let data = tokio::fs::File::open(&self.paths.dataset).await?;

        let memory_store: Store = ctx.create_store().await;
        memory_store
            .load_from_reader(data, RdfFileScanOptions::with_format(RdfFormat::NTriples))
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
}

async fn execute_benchmark<TUseCase: BsbmUseCase>(
    context: &BenchmarkContext<'_>,
    operations: Vec<SparqlOperation<TUseCase::QueryName>>,
    memory_store: &Store,
) -> anyhow::Result<BsbmReport<TUseCase>> {
    println!("Evaluating queries ...");

    let mut recorder = crate::utils::cache::CacheMetricsRecorder::new(context)?;

    let mut report = ExploreReportBuilder::new();
    let len = operations.len();
    for (idx, operation) in operations.iter().enumerate() {
        if idx % 25 == 0 {
            println!("Progress: {idx}/{len}");
        }

        run_operation(context, &mut report, memory_store, operation).await?;

        recorder.record_run(context, &operation.query_name().to_string())?;
    }
    let report = report.build();

    recorder.write_summary(context)?;

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
