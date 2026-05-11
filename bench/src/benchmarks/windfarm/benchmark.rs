use crate::benchmarks::windfarm::NumTurbines;
use crate::benchmarks::windfarm::generate::{generate_static, generate_time_series};
use crate::benchmarks::windfarm::queries::WindFarmQueryName;
use crate::benchmarks::windfarm::report::{
    QueryDetails, WindFarmReport, WindFarmReportBuilder,
};
use crate::benchmarks::{Benchmark, BenchmarkName};
use crate::environment::BenchmarkContext;
use crate::operation::SparqlRawOperation;
use crate::prepare::{ArchiveType, FileAction, PrepRequirement};
use crate::report::BenchmarkReport;
use crate::utils::print_store_stats;
use anyhow::Context;
use async_trait::async_trait;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::storage::rdf_files::RdfParserOptions;
use rdf_fusion::store::Store;
use reqwest::Url;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

/// Holds file paths for the files required for executing a BSBM run.
struct WindFarmFilePaths {
    /// A path to the wind farm data NTriples file.
    wind_farm_data: PathBuf,
    /// A path to the time series NTriples file.
    time_series_data: PathBuf,
    /// A path to the folder that contains all the queries.
    query_folder: PathBuf,
}

/// The "Wind Farm" benchmark is derived from the benchmarks used to evaluate Chrontext \[1\], an
/// ontology-based data access framework for time series data.
///
/// Based on the original benchmark, we have implemented a data generator in Rust that ...
/// - generates the RDF triples for the original static data (e.g., wind farm sites, turbines)
/// - generates the time series data as RDF triples instead of CSV files
///
/// As a result, these data can be used with any regular triple store.
///
/// # References
///
/// \[1\] M. Bakken and A. Soylu, “Chrontext: Portable SPARQL queries over contextualised time
///     series data in industrial settings,” Expert Systems with Applications, vol. 226, p. 120149,
///     Sept. 2023, doi: 10.1016/j.eswa.2023.120149.
pub struct WindFarmBenchmark {
    name: BenchmarkName,
    num_turbines: NumTurbines,
}

impl WindFarmBenchmark {
    /// Creates a new [WindFarmBenchmark] with the given sizes.
    pub fn new(num_turbines: NumTurbines) -> Self {
        let name = BenchmarkName::WindFarm { num_turbines };
        Self { name, num_turbines }
    }

    fn generate_data_set(&self, num_turbines: usize) -> PrepRequirement {
        PrepRequirement::RunClosure {
            execute: Box::new(move |ctx| {
                let files = create_files(ctx)?;

                let mut wind_farm_static_file = BufWriter::new(
                    File::create(&files.wind_farm_data)
                        .context("Could not create file for static wind farm data")?,
                );
                generate_static(&mut wind_farm_static_file, num_turbines)?;

                let mut time_series_file =
                    BufWriter::new(File::create(&files.time_series_data).context(
                        "Could not create file for time series wind farm data",
                    )?);
                generate_time_series(&mut time_series_file, num_turbines)
            }),
            check_requirement: Box::new(move |ctx| {
                let files = create_files(ctx)?;
                File::open(files.wind_farm_data.clone())
                    .context("Could not open file for static wind farm data")?;
                File::open(files.time_series_data.clone())
                    .context("Could not open file for time series wind farm data")?;
                Ok(())
            }),
        }
    }

    fn download_source() -> PrepRequirement {
        PrepRequirement::FileDownload {
            url: Url::parse("https://github.com/magbak/chrontext_benchmarks/archive/7947750d4f929b3483b5f4250aaf275676ec1139.zip").unwrap(),
            file_name: PathBuf::from("./source"),
            action: Some(FileAction::Unpack(ArchiveType::Zip)),
        }
    }
}

#[async_trait]
impl Benchmark for WindFarmBenchmark {
    fn name(&self) -> BenchmarkName {
        self.name
    }

    #[allow(clippy::expect_used)]
    fn requirements(&self, _bench_files_path: &Path) -> Vec<PrepRequirement> {
        let num_turbines = self.num_turbines.into_usize();
        let generate_dataset = self.generate_data_set(num_turbines);
        let download_source = Self::download_source();
        vec![generate_dataset, download_source]
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
        let dataset_path = create_files(ctx)?;
        let memory_store = ctx.parent().create_store().await;

        if print_info {
            println!("Loading static data ...");
        }
        let data = tokio::fs::File::open(&dataset_path.wind_farm_data).await?;
        memory_store
            .load_from_reader(data, RdfParserOptions::with_format(RdfFormat::N3))
            .await?;

        if print_info {
            println!("Loading time series data ...");
        }
        let data = tokio::fs::File::open(&dataset_path.time_series_data).await?;
        memory_store
            .load_from_reader(data, RdfParserOptions::with_format(RdfFormat::N3))
            .await?;

        if print_info {
            let duration = start.elapsed();
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
        let memory_store = self.prepare_store(bench_context, true).await?;
        let report = execute_benchmark(bench_context, &memory_store).await?;
        Ok(Box::new(report))
    }
}

async fn execute_benchmark(
    context: &BenchmarkContext<'_>,
    store: &Store,
) -> anyhow::Result<WindFarmReport> {
    println!("Evaluating queries ...");

    let mut report = WindFarmReportBuilder::new();
    for query_name in WindFarmQueryName::list_queries() {
        println!("Executing query: {query_name}");

        let operation = get_wind_farm_raw_sparql_operation(context, query_name)?;
        let query_text = operation.text().to_owned();
        let (run, explanation, num_results) =
            operation.parse().unwrap().run(store).await?;
        report.add_run(query_name, run.clone());
        if context.parent().options().verbose_results {
            let details = QueryDetails {
                query: query_text,
                total_time: run.duration,
                num_results,
                explanation,
            };
            report.add_explanation(query_name, details);
        }
    }
    let report = report.build();

    println!("All queries evaluated.");

    Ok(report)
}

pub fn get_wind_farm_raw_sparql_operation(
    context: &BenchmarkContext<'_>,
    query_name: WindFarmQueryName,
) -> anyhow::Result<SparqlRawOperation<WindFarmQueryName>> {
    let files = create_files(context)?;
    let query_file = files.query_folder.join(query_name.file_name());
    let query = fs::read_to_string(&query_file).context(format!(
        "Could not read query file: {}",
        query_file.display()
    ))?;
    Ok(SparqlRawOperation::Query(query_name, query.clone()))
}

fn create_files(ctx: &BenchmarkContext) -> anyhow::Result<WindFarmFilePaths> {
    let wind_farm_data = ctx.parent().join_data_dir(Path::new("wind-farm.ttl"))?;
    let time_series_data = ctx.parent().join_data_dir(Path::new("timeseries.ttl"))?;
    let query_folder = ctx
        .parent()
        .join_data_dir(Path::new("./source/benchmark-docker/queries_chrontext/"))?;
    Ok(WindFarmFilePaths {
        wind_farm_data,
        time_series_data,
        query_folder,
    })
}
