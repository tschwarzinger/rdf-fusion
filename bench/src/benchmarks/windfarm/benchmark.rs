use crate::BenchQuadStorageType;
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
use rdf_fusion::common::{RdfFormat, RdfSortOrder};
use rdf_fusion::storage::rdf_files::{RdfFileScanOptions, RdfFileSourceConfig};
use rdf_fusion::store::Store;
use reqwest::Url;
use std::fs;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};

/// Holds file paths for the files required for executing a BSBM run.
#[derive(Clone)]
struct WindFarmFilePaths {
    /// A path to the wind farm data NTriples file.
    wind_farm_data: PathBuf,
    /// A path to the time series NTriples file.
    time_series_data: PathBuf,
    /// A path to the folder that contains all the queries.
    query_folder: PathBuf,
    /// A path to the downloaded source.
    source: PathBuf,
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
    paths: WindFarmFilePaths,
}

impl WindFarmBenchmark {
    /// Creates a new [WindFarmBenchmark] with the given sizes.
    pub fn try_new(
        ctx: &BenchmarkContext,
        num_turbines: NumTurbines,
    ) -> anyhow::Result<Self> {
        let name = BenchmarkName::WindFarm { num_turbines };
        let paths = create_files(ctx)?;
        Ok(Self {
            name,
            num_turbines,
            paths,
        })
    }

    fn generate_data_set(&self) -> PrepRequirement {
        let paths = self.paths.clone();
        let num_turbines = self.num_turbines.into_usize();
        let paths_clone = paths.clone();
        PrepRequirement::RunClosure {
            execute: Box::new(move |_ctx| {
                let mut wind_farm_static_file = BufWriter::new(
                    File::create(&paths.wind_farm_data)
                        .context("Could not create file for static wind farm data")?,
                );
                generate_static(&mut wind_farm_static_file, num_turbines)?;

                let mut time_series_file =
                    BufWriter::new(File::create(&paths.time_series_data).context(
                        "Could not create file for time series wind farm data",
                    )?);
                generate_time_series(&mut time_series_file, num_turbines)
            }),
            check_requirement: Box::new(move |_ctx| {
                File::open(paths_clone.wind_farm_data.clone())
                    .context("Could not open file for static wind farm data")?;
                File::open(paths_clone.time_series_data.clone())
                    .context("Could not open file for time series wind farm data")?;
                Ok(())
            }),
        }
    }

    fn download_source(&self) -> PrepRequirement {
        PrepRequirement::FileDownload {
            url: Url::parse("https://github.com/magbak/chrontext_benchmarks/archive/7947750d4f929b3483b5f4250aaf275676ec1139.zip").unwrap(),
            file_name: self.paths.source.clone(),
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
        let generate_dataset = self.generate_data_set();
        let download_source = self.download_source();
        vec![generate_dataset, download_source]
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
        let memory_store = self.prepare_store(bench_context, true).await?;
        let report = execute_benchmark(self, bench_context, &memory_store).await?;
        Ok(Box::new(report))
    }
}

impl WindFarmBenchmark {
    async fn prepare_parquet_store(
        &self,
        ctx: &BenchmarkContext<'_>,
        print_info: bool,
        _sort_order: Option<RdfSortOrder>,
    ) -> anyhow::Result<Store> {
        if print_info {
            println!("Generating Parquet dataset ...");
        }

        let source = RdfFileSourceConfig {
            url: Url::parse(&ctx.resolve_path_to_url(&self.paths.wind_farm_data)?)?,
            format: RdfFormat::N3,
        };
        let ts_source = RdfFileSourceConfig {
            url: Url::parse(&ctx.resolve_path_to_url(&self.paths.time_series_data)?)?,
            format: RdfFormat::N3,
        };

        ctx.dump_to_parquet(
            vec![
                (rdf_fusion::common::GraphName::DefaultGraph, source),
                (rdf_fusion::common::GraphName::DefaultGraph, ts_source),
            ],
            None,
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
        let memory_store: Store = ctx.create_store().await;

        if print_info {
            println!("Loading static data ...");
        }
        let data = tokio::fs::File::open(&self.paths.wind_farm_data).await?;
        memory_store
            .load_from_reader(data, RdfFileScanOptions::with_format(RdfFormat::N3))
            .await?;

        if print_info {
            println!("Loading time series data ...");
        }
        let data = tokio::fs::File::open(&self.paths.time_series_data).await?;
        memory_store
            .load_from_reader(data, RdfFileScanOptions::with_format(RdfFormat::N3))
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

    pub fn get_wind_farm_raw_sparql_operation(
        &self,
        _context: &BenchmarkContext<'_>,
        query_name: WindFarmQueryName,
    ) -> anyhow::Result<SparqlRawOperation<WindFarmQueryName>> {
        let query_file = self.paths.query_folder.join(query_name.file_name());
        let query = fs::read_to_string(&query_file).context(format!(
            "Could not read query file: {}",
            query_file.display()
        ))?;
        Ok(SparqlRawOperation::Query(query_name, query.clone()))
    }
}

async fn execute_benchmark(
    benchmark: &WindFarmBenchmark,
    context: &BenchmarkContext<'_>,
    store: &Store,
) -> anyhow::Result<WindFarmReport> {
    println!("Evaluating queries ...");

    let mut recorder = crate::utils::cache::CacheMetricsRecorder::new(context)?;
    let mut report = WindFarmReportBuilder::new();
    for query_name in WindFarmQueryName::list_queries() {
        println!("Executing query: {query_name}");

        let operation =
            benchmark.get_wind_farm_raw_sparql_operation(context, query_name)?;
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

        recorder.record_run(context, &query_name.to_string())?;
    }
    let report = report.build();

    recorder.write_summary(context)?;

    println!("All queries evaluated.");

    Ok(report)
}

fn create_files(ctx: &BenchmarkContext) -> anyhow::Result<WindFarmFilePaths> {
    let wind_farm_data = ctx.data_dir().join("wind-farm.ttl");
    let time_series_data = ctx.data_dir().join("timeseries.ttl");
    let query_folder = ctx
        .data_dir()
        .join("source/benchmark-docker/queries_chrontext/");
    let source = ctx.data_dir().join("source");
    Ok(WindFarmFilePaths {
        wind_farm_data,
        time_series_data,
        query_folder,
        source,
    })
}
