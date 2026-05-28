use crate::benchmarks::BenchmarkName;
use crate::prepare::{
    PrepRequirement, prepare_copy_file, prepare_run_closure, prepare_run_command,
};
use crate::prepare::{ensure_file_download, prepare_file_download};
use crate::{BenchQuadStorageTypeArg, BenchmarkingConfig, QuadStorageLocationArg};
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::object_store::memory::InMemory;
use datafusion::prelude::SessionConfig;
use deltalake::logstore::{IORuntime, StorageConfig, logstore_with};
use rdf_fusion::common::config::RdfFusionSessionConfigExt;
use rdf_fusion::common::{GraphName, RdfInput, RdfSortOrder};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::execution::load::RdfParquetLoader;
use rdf_fusion::storage::delta::DeltaQuadStorageBuilder;
use rdf_fusion::storage::parquet::ParquetQuadStorage;
use rdf_fusion::storage::rdf_files::RdfFileSourceConfig;
use rdf_fusion::store::Store;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::runtime::Handle;
use url::Url;

pub struct RdfFusionBenchContext {
    /// General options for the benchmarks.
    config: BenchmarkingConfig,
    /// The path to existing benchmark files. This will always point to the root of the bench_files
    /// directory.
    bench_files_dir: PathBuf,
    /// The path to the data dir.
    data_dir: PathBuf,
    /// The path to the database dir.
    databases_dir: PathBuf,
    /// The path to the results dir.
    results_dir: PathBuf,
}

/// A builder for [`RdfFusionBenchContext`].
pub struct RdfFusionBenchContextBuilder {
    config: BenchmarkingConfig,
    bench_files_dir: PathBuf,
    data_dir: PathBuf,
    databases_dir: PathBuf,
    results_dir: PathBuf,
}

impl RdfFusionBenchContextBuilder {
    pub fn new(
        config: BenchmarkingConfig,
        bench_files_dir: PathBuf,
        data_dir: PathBuf,
        databases_dir: PathBuf,
        results_dir: PathBuf,
    ) -> Self {
        Self {
            config,
            bench_files_dir,
            data_dir,
            databases_dir,
            results_dir,
        }
    }

    pub fn with_storage_type(mut self, storage_type: BenchQuadStorageTypeArg) -> Self {
        self.config.storage_type = storage_type;
        self
    }

    pub fn with_storage_location(
        mut self,
        storage_location: QuadStorageLocationArg,
    ) -> Self {
        self.config.storage_location = storage_location;
        self
    }

    pub fn build(self) -> RdfFusionBenchContext {
        RdfFusionBenchContext {
            config: self.config,
            bench_files_dir: self.bench_files_dir,
            data_dir: self.data_dir,
            databases_dir: self.databases_dir,
            results_dir: self.results_dir,
        }
    }
}

impl RdfFusionBenchContext {
    /// Creates a new [RdfFusionBenchContextBuilder].
    pub fn builder(
        options: BenchmarkingConfig,
        bench_files_dir: PathBuf,
        data_dir: PathBuf,
        database_dir: PathBuf,
        results_dir: PathBuf,
    ) -> RdfFusionBenchContextBuilder {
        RdfFusionBenchContextBuilder::new(
            options,
            bench_files_dir,
            data_dir,
            database_dir,
            results_dir,
        )
    }

    /// Creates a new [RdfFusionBenchContext] used in the criterion benchmarks.
    pub fn new_for_criterion(
        data_dir: PathBuf,
        storage_encoding: QuadStorageEncodingName,
        target_partitions: usize,
    ) -> RdfFusionBenchContextBuilder {
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = target_partitions;
        config.options_mut().execution.parquet.pushdown_filters = true;

        let options = BenchmarkingConfig::new_for_criterion()
            .with_storage_encoding(storage_encoding)
            .with_storage_type(BenchQuadStorageTypeArg::Delta)
            .with_data_fusion_config(config);

        RdfFusionBenchContextBuilder::new(
            options,
            PathBuf::from("./bench_files"),
            data_dir,
            PathBuf::from("/tmp/database"),
            PathBuf::from("/tmp/results"),
        )
    }

    /// Returns the [BenchmarkingConfig] for this context.
    pub fn options(&self) -> &BenchmarkingConfig {
        &self.config
    }

    pub fn bench_files_dir(&self) -> &Path {
        &self.bench_files_dir
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn databases_dir(&self) -> &Path {
        &self.databases_dir
    }

    pub fn results_dir(&self) -> &Path {
        &self.results_dir
    }

    /// Resolves a path to a `file://` URL.
    pub fn resolve_path_to_url(&self, path: &Path) -> anyhow::Result<String> {
        let full_path = if path.is_absolute() {
            path.to_path_buf()
        } else {
            std::env::current_dir()?.join(path)
        };
        Ok(format!("file://{}", full_path.display()))
    }

    /// Creates a new bencher and modifies the context for this benchmark.
    pub fn create_benchmark_context(
        &self,
        benchmark_name: BenchmarkName,
    ) -> anyhow::Result<BenchmarkContext<'_>> {
        Ok(BenchmarkContext {
            context: self,
            benchmark_name,
        })
    }
}

/// A benchmarker that can be used to execute benchmarks.
///
/// It holds a reference to the current context to store its results.
pub struct BenchmarkContext<'ctx> {
    /// Reference to the benchmarking context.
    context: &'ctx RdfFusionBenchContext,
    /// Name of the benchmark that is being executed.
    benchmark_name: BenchmarkName,
}

impl<'ctx> BenchmarkContext<'ctx> {
    /// Returns a reference to the benchmarking context.
    pub fn parent(&self) -> &RdfFusionBenchContext {
        self.context
    }

    /// Returns the name of the benchmark that is being executed.
    pub fn benchmark_name(&self) -> BenchmarkName {
        self.benchmark_name
    }

    /// Provides access to the benchmark files. This will always point to the root of the
    /// bench_files directory.
    pub fn bench_files_dir(&self) -> PathBuf {
        self.context.bench_files_dir.clone()
    }

    /// Returns the path to the results directory of this benchmark.
    pub fn data_dir(&self) -> PathBuf {
        self.context
            .data_dir
            .join(self.benchmark_name.data_dir_name())
    }

    /// Returns the path to the database directory of this benchmark.
    pub fn databases_dir(&self) -> PathBuf {
        self.context
            .databases_dir
            .join(self.benchmark_name.data_dir_name())
    }

    /// Returns the path to the results directory of this benchmark.
    pub fn results_dir(&self) -> PathBuf {
        let mut name = self.benchmark_name.results_dir_name();
        if let Some(postfix) = &self.context.config.results_postfix {
            name = format!("{name}-{postfix}");
        }
        self.context.results_dir.join(name)
    }

    /// Returns the RDF Fusion configuration.
    pub fn get_rdf_fusion_config(&self) -> rdf_fusion::common::config::RdfFusionOptions {
        self.context
            .config
            .data_fusion_config
            .rdf_fusion_options_or_from_env()
            .expect("Failed to get RDF Fusion options")
    }

    /// Creates a [RuntimeEnv] with the configured memory limits and caching.
    pub async fn create_runtime_env(&self) -> Arc<RuntimeEnv> {
        let mut builder = RuntimeEnvBuilder::new();
        if let Some(memory_limit) = self.context.config.memory_limit {
            builder = builder.with_memory_limit(memory_limit, 1.0);
        }

        let registry = Arc::clone(&builder.object_store_registry);
        registry.register_store(
            &Url::parse("memory:///").unwrap(),
            Arc::new(InMemory::new()),
        );

        builder = builder.with_object_store_registry(registry);

        builder.build_arc().expect("Failed to build RuntimeEnv")
    }

    /// Resolves a path to a `file://` URL.
    pub fn resolve_path_to_url(&self, path: &Path) -> anyhow::Result<String> {
        self.context.resolve_path_to_url(path)
    }

    /// Dumps the given sources to a Parquet file in the database directory.
    pub async fn dump_to_parquet(
        &self,
        sources: Vec<(GraphName, RdfFileSourceConfig)>,
        sort_order: Option<RdfSortOrder>,
    ) -> anyhow::Result<()> {
        let mut parquet_path = self.databases_dir();
        parquet_path.push("dataset.parquet");

        let url_raw = match self.context.config.storage_location {
            QuadStorageLocationArg::InMemory => {
                format!(
                    "memory:///databases/{}/dataset.parquet",
                    self.benchmark_name.data_dir_name()
                )
            }
            QuadStorageLocationArg::OnDisk => {
                if parquet_path.exists() {
                    std::fs::remove_file(&parquet_path)?;
                } else {
                    let parent = parquet_path.parent().expect("Invalid parquet path");
                    if !parent.exists() {
                        std::fs::create_dir_all(parent)?;
                    }
                }

                self.resolve_path_to_url(&parquet_path)?
            }
        };
        let url = Url::parse(&url_raw).expect("Invalid URL");

        let mut rdf_fusion_config = self.get_rdf_fusion_config();
        rdf_fusion_config.storage.parquet.sort_order = sort_order;

        let delta_storage = Arc::new(DeltaQuadStorageBuilder::new().build().await?);
        let runtime_env = self.create_runtime_env().await;

        let mut session_config = self.context.config.data_fusion_config.clone();
        session_config
            .options_mut()
            .extensions
            .insert(rdf_fusion_config);

        let context = RdfFusionContextBuilder::new(delta_storage)
            .with_session_config(Some(session_config))
            .with_runtime_env(Some(runtime_env))
            .build()?;

        let loader = RdfParquetLoader::try_new(context, QuadStorageEncodingName::String)?;
        let inputs: Vec<_> = sources
            .into_iter()
            .map(|(_, s)| {
                RdfInput::new_with_format(s.url, GraphName::DefaultGraph, s.format)
            })
            .collect();

        loader.load_many(inputs, url).await?;
        Ok(())
    }

    pub async fn create_store(&self) -> Store {
        let runtime_env = self.create_runtime_env().await;
        let rdf_fusion_config = self.get_rdf_fusion_config();

        let (base_url, object_store_url) = match self.context.config.storage_location {
            QuadStorageLocationArg::InMemory => (
                format!(
                    "memory:///databases/{}",
                    self.benchmark_name.data_dir_name()
                ),
                "memory://".to_string(),
            ),
            QuadStorageLocationArg::OnDisk => {
                let full_iri = self.prepare_database_directory();
                (full_iri, "file://".to_string())
            }
        };

        let storage_backend: Arc<dyn rdf_fusion::api::storage::QuadStorage> =
            match self.context.config.storage_type {
                BenchQuadStorageTypeArg::Delta => {
                    let url = Url::parse(&base_url).unwrap();
                    let object_store_url =
                        ObjectStoreUrl::parse(&object_store_url).unwrap();

                    let object_store = runtime_env
                        .object_store(&object_store_url)
                        .expect("Failed to get object store");
                    let log_store = logstore_with(
                        Arc::clone(&object_store),
                        &url,
                        StorageConfig::default()
                            .with_io_runtime(IORuntime::RT(Handle::current())),
                    )
                    .expect("Failed to create log store");

                    Arc::new(
                        DeltaQuadStorageBuilder::new()
                            .with_log_store(log_store)
                            .with_encoding(self.context.config.storage_encoding)
                            .with_log_max_age(rdf_fusion_config.storage.delta.log_max_age)
                            .build()
                            .await
                            .expect("Failed to create DeltaQuadStorage"),
                    )
                }
                BenchQuadStorageTypeArg::Parquet => {
                    if matches!(
                        self.context.config.storage_encoding,
                        QuadStorageEncodingName::ObjectId
                    ) {
                        panic!("Parquet encoding not supported for ObjectId");
                    }

                    let url = Url::parse(&format!("{base_url}/dataset.parquet")).unwrap();
                    let storage = ParquetQuadStorage::try_load(
                        url,
                        self.context.config.storage_encoding,
                        runtime_env.object_store_registry.as_ref(),
                    )
                    .await
                    .expect("Failed to create ParquetQuadStorage");
                    Arc::new(storage)
                }
            };

        let context = RdfFusionContextBuilder::new(storage_backend)
            .with_session_config(Some(self.context.config.data_fusion_config.clone()))
            .with_runtime_env(Some(runtime_env))
            .build()
            .expect("Failed to create RdfFusionContext");
        Store::new(context)
    }

    /// Prepares the directory where the database is stored.
    fn prepare_database_directory(&self) -> String {
        let database_dir = self.databases_dir();

        let full_path = if database_dir.is_absolute() {
            database_dir.clone()
        } else {
            std::env::current_dir()
                .expect("Failed to get current directory")
                .join(database_dir.as_path())
        };

        match self.context.config.storage_type {
            BenchQuadStorageTypeArg::Delta => {
                if full_path.exists() {
                    std::fs::remove_dir_all(&full_path)
                        .expect("Failed to remove existing directory");
                }
                std::fs::create_dir_all(&full_path).expect("Failed to create directory");
            }
            BenchQuadStorageTypeArg::Parquet => {
                if !full_path.exists() {
                    std::fs::create_dir_all(&full_path)
                        .expect("Failed to create directory");
                }
            }
        }

        let full_path = full_path
            .canonicalize()
            .expect("Failed to resolve absolute path");

        format!("file://{}", full_path.display())
    }

    /// Prepares the context such that `requirement` is fulfilled.
    pub async fn prepare_requirement(
        &self,
        requirement: PrepRequirement,
    ) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::CopyFile {
                source_path,
                target_path,
                action,
            } => prepare_copy_file(&source_path, &target_path, action.as_ref()),
            PrepRequirement::FileDownload {
                url,
                file_name,
                action,
            } => prepare_file_download(url, file_name, action).await,
            PrepRequirement::RunClosure { execute, .. } => {
                prepare_run_closure(self, &execute)
            }
            PrepRequirement::RunCommand {
                workdir,
                program,
                args,
                ..
            } => prepare_run_command(&workdir, &program, &args),
        }
    }

    /// Ensures that the `requirement` is fulfilled in this context.
    pub fn ensure_requirement(&self, requirement: PrepRequirement) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::CopyFile { target_path, .. } => {
                ensure_file_download(target_path.as_path())
            }
            PrepRequirement::FileDownload { file_name, .. } => {
                ensure_file_download(file_name.as_path())
            }
            PrepRequirement::RunClosure {
                check_requirement, ..
            }
            | PrepRequirement::RunCommand {
                check_requirement, ..
            } => check_requirement(self),
        }
    }
}
