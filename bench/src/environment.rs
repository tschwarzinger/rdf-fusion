use crate::benchmarks::BenchmarkName;
use crate::prepare::{
    PrepRequirement, prepare_copy_file, prepare_run_closure, prepare_run_command,
};
use crate::prepare::{ensure_file_download, prepare_file_download};
use crate::{BenchmarkStorageBackend, BenchmarkingOptions};
use anyhow::bail;
use datafusion::execution::object_store::ObjectStoreUrl;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::object_store::memory::InMemory;
use datafusion::prelude::SessionConfig;
use deltalake::logstore::{IORuntime, StorageConfig, logstore_with};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::execution::cache::CachingObjectStoreRegistry;
use rdf_fusion::storage::delta::DeltaQuadStorageBuilder;
use rdf_fusion::store::Store;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;
use url::Url;

/// Represents a context used to execute benchmarks.
pub struct RdfFusionBenchContext {
    /// General options for the benchmarks.
    options: BenchmarkingOptions,
    /// The path to existing benchmark files. This will always point to the root of the bench_files
    /// directory.
    bench_files_dir: PathBuf,
    /// The path to the data dir.
    data_dir: Mutex<PathBuf>,
    /// The path to the database dir.
    databases_dir: Mutex<PathBuf>,
    /// The path to the results dir.
    results_dir: Mutex<PathBuf>,
}

impl RdfFusionBenchContext {
    /// Creates a new [RdfFusionBenchContext].
    pub fn new(
        options: BenchmarkingOptions,
        bench_files_dir: PathBuf,
        data_dir: PathBuf,
        database_dir: PathBuf,
        results_dir: PathBuf,
    ) -> Self {
        Self {
            options,
            bench_files_dir,
            data_dir: Mutex::new(data_dir),
            databases_dir: Mutex::new(database_dir),
            results_dir: Mutex::new(results_dir),
        }
    }

    /// Creates a new [RdfFusionBenchContext] used in the criterion benchmarks.
    pub fn new_for_criterion(
        data_dir: PathBuf,
        storage_encoding: QuadStorageEncodingName,
        target_partitions: usize,
    ) -> Self {
        let mut config = SessionConfig::new();
        config.options_mut().execution.target_partitions = target_partitions;
        config.options_mut().execution.parquet.pushdown_filters = true;

        Self {
            options: BenchmarkingOptions {
                verbose_results: false,
                memory_size: None,
                storage_backend: BenchmarkStorageBackend::DeltaLakeInMemory,
                storage_encoding,
                config,
            },
            data_dir: Mutex::new(data_dir),
            databases_dir: Mutex::new(PathBuf::from("/tmp/database")),
            bench_files_dir: PathBuf::from("./bench_files"),
            results_dir: Mutex::new(PathBuf::from("/tmp/results")),
        }
    }

    /// Returns the [BenchmarkingOptions] for this context.
    pub fn options(&self) -> &BenchmarkingOptions {
        &self.options
    }

    /// Resolves a relative path `file` against the data directory.
    pub fn join_data_dir(&self, file: &Path) -> anyhow::Result<PathBuf> {
        if !file.is_relative() {
            bail!("Only relative paths can be resolved.")
        }

        Ok(self.data_dir.lock().expect("Poisoned").join(file))
    }

    pub async fn create_store(&self) -> Store {
        let mut builder = RuntimeEnvBuilder::new();
        if let Some(memory_size) = self.options.memory_size {
            builder = builder.with_memory_limit(memory_size * 1024 * 1024, 1.0);
        }

        let registry = Arc::clone(&builder.object_store_registry);
        registry.register_store(
            &Url::parse("memory:///").unwrap(),
            Arc::new(InMemory::new()),
        );

        let registry = Arc::new(CachingObjectStoreRegistry::new(
            registry,
            1024 * 1024 * 1024,
        ));
        builder = builder.with_object_store_registry(registry);

        let runtime_env = builder.build_arc().expect("Failed to build RuntimeEnv");

        let storage_backend = match self.options.storage_backend {
            BenchmarkStorageBackend::DeltaLakeInMemory => {
                let url = ObjectStoreUrl::parse("memory://").unwrap();
                let object_store = runtime_env
                    .object_store(&url)
                    .expect("Failed to get in-memory object store");
                let log_store = logstore_with(
                    Arc::clone(&object_store),
                    url.as_ref(),
                    StorageConfig::default()
                        .with_io_runtime(IORuntime::RT(Handle::current())),
                )
                .expect("Failed to create log store");

                DeltaQuadStorageBuilder::new()
                    .with_log_store(log_store)
                    .with_encoding(self.options.storage_encoding)
                    .build()
                    .await
                    .expect("Failed to create DeltaQuadStorage")
            }
            BenchmarkStorageBackend::DeltaLakeOnDisk => {
                let full_iri = prepare_database_directory(self);
                let full_iri = Url::parse(&full_iri).unwrap();

                let base_url =
                    ObjectStoreUrl::parse("file://").expect("Invalid database URL");
                let object_store = runtime_env
                    .object_store(&base_url)
                    .expect("Failed to get object store");
                let log_store = logstore_with(
                    Arc::clone(&object_store),
                    &full_iri,
                    StorageConfig::default()
                        .with_io_runtime(IORuntime::RT(Handle::current())),
                )
                .expect("Failed to create log store");

                DeltaQuadStorageBuilder::new()
                    .with_log_store(log_store)
                    .with_encoding(self.options.storage_encoding)
                    .build()
                    .await
                    .expect("Failed to create DeltaQuadStorage")
            }
        };

        let context = RdfFusionContextBuilder::new(Arc::new(storage_backend))
            .with_session_config(Some(self.options.config.clone()))
            .with_runtime_env(Some(runtime_env))
            .build()
            .expect("Failed to create RdfFusionContext");
        return Store::new(context);

        /// Prepares the directory where the database is stored.
        fn prepare_database_directory(context: &RdfFusionBenchContext) -> String {
            let database_dir = context.databases_dir.lock().expect("Poisoned");

            let full_path = if database_dir.is_absolute() {
                database_dir.clone()
            } else {
                std::env::current_dir()
                    .expect("Failed to get current directory")
                    .join(database_dir.as_path())
            };

            if full_path.exists() {
                std::fs::remove_dir_all(&full_path)
                    .expect("Failed to remove existing directory");
            }
            std::fs::create_dir_all(&full_path).expect("Failed to create directory");

            let full_path = full_path
                .canonicalize()
                .expect("Failed to resolve absolute path");

            format!("file://{}", full_path.display())
        }
    }

    /// Creates a new folder in the results directory and uses it until [Self::pop_dir] is
    /// called.
    ///
    /// This can be used to create folder hierarchies to separate the results of different
    /// benchmarks.
    #[allow(clippy::create_dir)]
    #[allow(clippy::unwrap_used, reason = "Mutex poisoning")]
    pub fn push_dir(
        &self,
        data_dir_name: &str,
        results_dir_name: &str,
    ) -> anyhow::Result<()> {
        let mut data_dir = self.data_dir.lock().unwrap();
        let mut databases_dir = self.databases_dir.lock().unwrap();
        let mut results_dir = self.results_dir.lock().unwrap();

        data_dir.push(data_dir_name);
        databases_dir.push(data_dir_name);
        results_dir.push(results_dir_name);

        Ok(())
    }

    /// Pops the last directory from the stack.
    pub fn pop_dir(&self) {
        let mut data_dir = self.data_dir.lock().unwrap();
        let mut databases_dir = self.databases_dir.lock().unwrap();
        let mut results_dir = self.results_dir.lock().unwrap();

        data_dir.pop();
        databases_dir.pop();
        results_dir.pop();
    }

    /// Creates a new bencher and modifies the context for this benchmark.
    pub fn create_benchmark_context(
        &self,
        benchmark_name: BenchmarkName,
    ) -> anyhow::Result<BenchmarkContext<'_>> {
        self.push_dir(
            &benchmark_name.data_dir_name(),
            &benchmark_name.results_dir_name(),
        )?;
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
        self.context.data_dir.lock().unwrap().clone()
    }

    /// Returns the path to the results directory of this benchmark.
    pub fn results_dir(&self) -> PathBuf {
        self.context.results_dir.lock().unwrap().clone()
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
            } => prepare_copy_file(self, &source_path, &target_path, action.as_ref()),
            PrepRequirement::FileDownload {
                url,
                file_name,
                action,
            } => prepare_file_download(self, url, file_name, action).await,
            PrepRequirement::RunClosure { execute, .. } => {
                prepare_run_closure(self, &execute)
            }
            PrepRequirement::RunCommand {
                workdir,
                program,
                args,
                ..
            } => {
                let workdir = self.context.join_data_dir(&workdir)?;
                prepare_run_command(&workdir, &program, &args)
            }
        }
    }

    /// Ensures that the `requirement` is fulfilled in this context.
    pub fn ensure_requirement(&self, requirement: PrepRequirement) -> anyhow::Result<()> {
        match requirement {
            PrepRequirement::CopyFile { target_path, .. } => {
                ensure_file_download(self, target_path.as_path())
            }
            PrepRequirement::FileDownload { file_name, .. } => {
                ensure_file_download(self, file_name.as_path())
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

/// Pops the results directory from the context when the bencher is dropped.
impl Drop for BenchmarkContext<'_> {
    fn drop(&mut self) {
        self.context.pop_dir();
    }
}
