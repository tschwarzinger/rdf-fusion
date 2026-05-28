use clap::ValueEnum;
use datafusion::prelude::SessionConfig;
use rdf_fusion::common::config::RdfFusionOptions;
use rdf_fusion::encoding::QuadStorageEncodingName;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum BenchmarkingConfigError {
    #[error(
        "Invalid value for {env_var}: '{value}' is invalid. Expected type: {expected_type}"
    )]
    InvalidEnvVar {
        env_var: String,
        value: String,
        expected_type: String,
    },
    #[error(transparent)]
    DataFusionError(#[from] datafusion::error::DataFusionError),
}

/// Provides options for the benchmarking process.
#[derive(Debug)]
pub struct BenchmarkingConfig {
    /// Indicates whether the benchmarking results should be verbose.
    pub verbose_results: bool,
    /// The number of MiBs that DataFusion is allowed to use.
    pub memory_limit: Option<usize>,
    /// The storage location to use for the benchmark.
    pub storage_location: QuadStorageLocationArg,
    /// The storage type to use for the benchmark.
    pub storage_type: BenchQuadStorageTypeArg,
    /// The storage encoding to use for the benchmark.
    pub storage_encoding: QuadStorageEncodingName,
    /// The number of parallel tasks to use for the benchmark.
    pub max_parallel_tasks: usize,
    /// An optional suffix for the results directory.
    pub results_postfix: Option<String>,
    /// The DataFusion config.
    pub data_fusion_config: SessionConfig,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum QuadStorageLocationArg {
    /// The storage location is in-memory.
    InMemory,
    /// The storage location is on disk.
    OnDisk,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum BenchQuadStorageTypeArg {
    /// Uses a storage based on Delta Lake.
    Delta,
    /// The storage type is a single parquet file.
    Parquet,
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, ValueEnum)]
pub enum QuadStorageEncodingNameArg {
    /// The plain term encoding
    PlainTerm,
    /// The string encoding
    String,
    /// Use the object id
    ObjectId,
}

impl From<QuadStorageEncodingNameArg> for QuadStorageEncodingName {
    fn from(value: QuadStorageEncodingNameArg) -> Self {
        match value {
            QuadStorageEncodingNameArg::PlainTerm => QuadStorageEncodingName::PlainTerm,
            QuadStorageEncodingNameArg::String => QuadStorageEncodingName::String,
            QuadStorageEncodingNameArg::ObjectId => QuadStorageEncodingName::ObjectId,
        }
    }
}

impl BenchmarkingConfig {
    /// Creates a new [BenchmarkingConfig] with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new [BenchmarkingConfig] for use in Criterion benchmarks.
    pub fn new_for_criterion() -> Self {
        Self::new()
            .with_storage_location(QuadStorageLocationArg::InMemory)
            .with_storage_type(BenchQuadStorageTypeArg::Parquet)
    }

    /// Initializes a [BenchmarkingConfig] from environment variables.
    pub fn from_env() -> Result<Self, BenchmarkingConfigError> {
        let mut config = Self::new();

        // Populate our config variables using standard environment variables
        config.apply_env_vars(|key| std::env::var(key).ok())?;

        // Initialize DataFusion session config from env
        let mut df_config = SessionConfig::from_env()?;
        df_config
            .options_mut()
            .extensions
            .insert(RdfFusionOptions::from_env()?);
        config.data_fusion_config = df_config;

        Ok(config)
    }

    /// Applies benchmark-specific environment variables using a provider function.
    /// This decouples parsing from the OS environment, allowing safe testing.
    pub fn apply_env_vars<F, S>(
        &mut self,
        mut get_env: F,
    ) -> Result<(), BenchmarkingConfigError>
    where
        F: FnMut(&str) -> Option<S>,
        S: AsRef<str>,
    {
        if let Some(val) = get_env("RDF_FUSION_BENCH_VERBOSE_RESULTS") {
            let val_str = val.as_ref();
            self.verbose_results =
                val_str
                    .parse()
                    .map_err(|_| BenchmarkingConfigError::InvalidEnvVar {
                        env_var: "RDF_FUSION_BENCH_VERBOSE_RESULTS".to_string(),
                        value: val_str.to_string(),
                        expected_type: "Boolean".to_string(),
                    })?;
        }

        if let Some(val) = get_env("RDF_FUSION_BENCH_MEMORY_LIMIT") {
            let val_str = val.as_ref();
            let limit_mib: usize =
                val_str
                    .parse()
                    .map_err(|_| BenchmarkingConfigError::InvalidEnvVar {
                        env_var: "RDF_FUSION_BENCH_MEMORY_LIMIT".to_string(),
                        value: val_str.to_string(),
                        expected_type: "Number (MiB)".to_string(),
                    })?;
            self.memory_limit = Some(limit_mib * 1024 * 1024);
        }

        if let Some(val) = get_env("RDF_FUSION_BENCH_MAX_PARALLEL_TASKS") {
            let val_str = val.as_ref();
            self.max_parallel_tasks =
                val_str
                    .parse()
                    .map_err(|_| BenchmarkingConfigError::InvalidEnvVar {
                        env_var: "RDF_FUSION_BENCH_MAX_PARALLEL_TASKS".to_string(),
                        value: val_str.to_string(),
                        expected_type: "number".to_string(),
                    })?;
        }

        if let Some(val) = get_env("RDF_FUSION_BENCH_RESULTS_POSTFIX") {
            self.results_postfix = Some(val.as_ref().to_string());
        }

        Ok(())
    }

    pub fn with_verbose_results(mut self, verbose_results: bool) -> Self {
        self.verbose_results = verbose_results;
        self
    }

    pub fn with_memory_limit(mut self, memory_limit: Option<usize>) -> Self {
        self.memory_limit = memory_limit;
        self
    }

    pub fn with_storage_location(
        mut self,
        storage_location: QuadStorageLocationArg,
    ) -> Self {
        self.storage_location = storage_location;
        self
    }

    pub fn with_storage_type(mut self, storage_type: BenchQuadStorageTypeArg) -> Self {
        self.storage_type = storage_type;
        self
    }

    pub fn with_storage_encoding(
        mut self,
        storage_encoding: QuadStorageEncodingName,
    ) -> Self {
        self.storage_encoding = storage_encoding;
        self
    }

    pub fn with_max_parallel_tasks(mut self, max_parallel_tasks: usize) -> Self {
        self.max_parallel_tasks = max_parallel_tasks;
        self
    }

    pub fn with_results_suffix(mut self, results_suffix: Option<String>) -> Self {
        self.results_postfix = results_suffix;
        self
    }

    pub fn with_data_fusion_config(mut self, data_fusion_config: SessionConfig) -> Self {
        self.data_fusion_config = data_fusion_config;
        self
    }
}

impl Default for BenchmarkingConfig {
    fn default() -> Self {
        Self {
            verbose_results: false,
            memory_limit: None,
            storage_location: QuadStorageLocationArg::OnDisk,
            storage_type: BenchQuadStorageTypeArg::Delta,
            storage_encoding: QuadStorageEncodingName::ObjectId,
            max_parallel_tasks: 1,
            results_postfix: None,
            data_fusion_config: SessionConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_config_defaults() {
        let config = BenchmarkingConfig::default();
        assert!(!config.verbose_results);
        assert_eq!(config.memory_limit, None);
        assert_eq!(config.max_parallel_tasks, 1);
    }

    #[test]
    fn test_config_builder() {
        let config = BenchmarkingConfig::new()
            .with_verbose_results(true)
            .with_memory_limit(Some(1024))
            .with_max_parallel_tasks(4);

        assert!(config.verbose_results);
        assert_eq!(config.memory_limit, Some(1024));
        assert_eq!(config.max_parallel_tasks, 4);
    }

    #[test]
    fn test_config_from_env_valid() {
        let mut mock_env = HashMap::new();
        mock_env.insert("RDF_FUSION_BENCH_VERBOSE_RESULTS", "true");
        mock_env.insert("RDF_FUSION_BENCH_MEMORY_LIMIT", "100");
        mock_env.insert("RDF_FUSION_BENCH_MAX_PARALLEL_TASKS", "8");
        mock_env.insert("RDF_FUSION_BENCH_RESULTS_POSTFIX", "pso");

        let mut config = BenchmarkingConfig::new();
        config.apply_env_vars(|k| mock_env.get(k)).unwrap();

        assert!(config.verbose_results);
        assert_eq!(config.memory_limit, Some(104857600));
        assert_eq!(config.max_parallel_tasks, 8);
        assert_eq!(config.results_postfix, Some("pso".to_string()));
    }

    #[test]
    fn test_config_from_env_parallel_tasks_invalid() {
        let mut mock_env = HashMap::new();
        mock_env.insert("RDF_FUSION_BENCH_MAX_PARALLEL_TASKS", "invalid");

        let mut config = BenchmarkingConfig::new();
        let result = config.apply_env_vars(|k| mock_env.get(k));

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid value for RDF_FUSION_BENCH_MAX_PARALLEL_TASKS: 'invalid' is invalid. Expected type: number"
        );
    }

    #[test]
    fn test_config_from_env_memory_limit_invalid() {
        let mut mock_env = HashMap::new();
        mock_env.insert("RDF_FUSION_BENCH_MEMORY_LIMIT", "notanumber");

        let mut config = BenchmarkingConfig::new();
        let result = config.apply_env_vars(|k| mock_env.get(k));

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid value for RDF_FUSION_BENCH_MEMORY_LIMIT: 'notanumber' is invalid. Expected type: Number (MiB)"
        );
    }

    #[test]
    fn test_config_from_env_verbose_results_invalid() {
        let mut mock_env = HashMap::new();
        mock_env.insert("RDF_FUSION_BENCH_VERBOSE_RESULTS", "maybe");

        let mut config = BenchmarkingConfig::new();
        let result = config.apply_env_vars(|k| mock_env.get(k));

        assert_eq!(
            result.unwrap_err().to_string(),
            "Invalid value for RDF_FUSION_BENCH_VERBOSE_RESULTS: 'maybe' is invalid. Expected type: Boolean"
        );
    }
}
