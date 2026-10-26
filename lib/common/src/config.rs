use crate::{DFResult, RdfSortOrder};
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::{SessionConfig, SessionContext};
use std::any::Any;
use std::fmt::{Display, Formatter};
use std::str::FromStr;
use std::time::Duration;

/// Configuration for RDF Fusion.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RdfFusionOptions {
    /// Storage configuration.
    pub storage: StorageOptions,
    /// Execution configuration.
    pub execution: ExecutionOptions,
    /// Local configuration.
    pub local: LocalOptions,
}

impl ConfigExtension for RdfFusionOptions {
    const PREFIX: &'static str = "rdf_fusion";
}

/// Execution configuration for RDF Fusion.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ExecutionOptions {
    /// The size in bytes below which a scan is eagerly buffered during planning. This allows small
    /// scans to be evaluated in parallel instead of sequentially waiting on dynamic filters. A
    /// value of `0` disables this optimization.
    pub small_scan_buffering_threshold: usize,
    /// Whether Bloom filters are used for pruning Parquet row groups during planning.
    pub planning_bloom_filters: PlanningBloomFilters,
}

/// Defines whether Bloom filters are used for pruning Parquet row groups during planning. Pruning
/// during planning improves the cardinality estimates but requires reading the Bloom filters.
/// Regardless of this option, Bloom filters are used during execution.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PlanningBloomFilters {
    /// Uses Bloom filters during planning if the scan caches the data it reads. Hence, the Bloom
    /// filters are only read once.
    #[default]
    Auto,
    /// Always uses Bloom filters during planning.
    Always,
    /// Never uses Bloom filters during planning.
    Never,
}

impl Display for PlanningBloomFilters {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            PlanningBloomFilters::Auto => "auto",
            PlanningBloomFilters::Always => "always",
            PlanningBloomFilters::Never => "never",
        };
        f.write_str(value)
    }
}

impl FromStr for PlanningBloomFilters {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "auto" => Ok(PlanningBloomFilters::Auto),
            "always" => Ok(PlanningBloomFilters::Always),
            "never" => Ok(PlanningBloomFilters::Never),
            _ => Err(format!(
                "Expected one of \"auto\", \"always\", or \"never\" but got \"{value}\""
            )),
        }
    }
}

/// Local configuration for RDF Fusion.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct LocalOptions {
    /// Local workspace directory for storing cache/DB files.
    pub work_dir: Option<String>,
}

/// Storage configuration for RDF Fusion.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StorageOptions {
    /// Delta storage configuration.
    pub delta: DeltaStorageOptions,
    /// Parquet storage configuration.
    pub parquet: ParquetStorageOptions,
    /// RDF file storage options.
    pub rdf_files: RdfFileStorageOptions,
}

/// Parquet storage configuration for RDF Fusion.
#[derive(Debug, Clone, PartialEq)]
pub struct ParquetStorageOptions {
    /// The sort orders of the Parquet files that are written by the loader. Each sort order is
    /// written as a separate quad table, such that the storage can choose the best sort order for
    /// each triple pattern. If empty, the quads are not sorted.
    pub sort_orders: Vec<RdfSortOrder>,
    /// Whether data caching is enabled for Parquet storage.
    pub data_cache_enabled: bool,
    /// The size of the blocks used for caching object store reads in bytes.
    pub data_cache_block_size: usize,
    /// The number of blocks to cache for object store reads.
    pub data_cache_num_blocks: usize,
    /// The number of leading sort columns that the Parquet writers write with page-level
    /// statistics. The remaining sort columns only have chunk-level statistics, which keeps the
    /// page index small. By default, all columns have page-level statistics.
    pub page_statistics_columns: usize,
}

impl Default for ParquetStorageOptions {
    fn default() -> Self {
        Self {
            sort_orders: Vec::new(),
            data_cache_enabled: false,
            data_cache_block_size: 2 * 1024 * 1024,
            data_cache_num_blocks: 1024,
            page_statistics_columns: 4,
        }
    }
}

/// Delta storage configuration for RDF Fusion.
#[derive(Debug, Clone, PartialEq)]
pub struct DeltaStorageOptions {
    /// The maximum age of the operations log that should be queried before refreshing.
    pub log_max_age: Option<Duration>,
    /// The size of the claimed ranges for object IDs.
    pub object_id_claim_size: i64,
    /// Maximum number of rows to buffer before committing.
    pub max_buffered_rows: Option<usize>,
    /// Maximum number of pending IDs to buffer before committing.
    pub max_buffered_ids: Option<usize>,
    /// The size of the cache in the local object id dictionary.
    pub object_id_cache_size: Option<usize>,
    /// Whether the system can assume that no other node is writing to the storage.
    pub assume_single_node: bool,
    /// The size of the blocks used for caching object store reads.
    pub data_cache_block_size: usize,
    /// The number of blocks to cache for object store reads.
    pub data_cache_num_blocks: usize,
}

impl Default for DeltaStorageOptions {
    fn default() -> Self {
        Self {
            log_max_age: None,
            object_id_claim_size: 100_000,
            max_buffered_rows: None,
            max_buffered_ids: None,
            object_id_cache_size: Some(100_000),
            assume_single_node: false,
            data_cache_block_size: 2 * 1024 * 1024, // 2 MiB
            data_cache_num_blocks: 1024,
        }
    }
}

/// Options related to working with RDF files (e.g., Turtle).
#[derive(Debug, Clone, PartialEq)]
pub struct RdfFileStorageOptions {
    /// Whether the query engine should assume that the quads within one file are unique.
    pub assume_quads_unique_in_single_file: bool,
    /// The minimum size in bytes of a chunk of a line-based RDF file (N-Triples, N-Quads) that is
    /// parsed in parallel. A file is split into at most `target_partitions` chunks.
    pub parallel_parsing_min_chunk_size: usize,
}

impl Default for RdfFileStorageOptions {
    fn default() -> Self {
        Self {
            assume_quads_unique_in_single_file: false,
            parallel_parsing_min_chunk_size: 16 * 1024 * 1024, // 16 MiB
        }
    }
}

impl ExtensionOptions for RdfFusionOptions {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn cloned(&self) -> Box<dyn ExtensionOptions> {
        Box::new(self.clone())
    }

    fn set(&mut self, key: &str, value: &str) -> DFResult<()> {
        let key = key
            .strip_prefix(&format!("{}.", Self::PREFIX))
            .unwrap_or(key);
        match key {
            "execution.small_scan_buffering_threshold" => {
                self.execution.small_scan_buffering_threshold =
                    SessionContext::parse_capacity_limit(key, value)?;
            }
            "execution.planning_bloom_filters" => {
                self.execution.planning_bloom_filters = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for execution.planning_bloom_filters: {e}"
                    ))
                })?;
            }
            "local.work_dir" => {
                self.local.work_dir = Some(value.to_string());
            }
            "storage.delta.assume_single_node" => {
                let value: bool = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.delta.assume_single_node: {e}"
                    ))
                })?;
                self.storage.delta.assume_single_node = value;
            }
            "storage.delta.data_cache_block_size" => {
                let size = SessionContext::parse_capacity_limit(key, value)?;
                self.storage.delta.data_cache_block_size = size;
            }
            "storage.delta.data_cache_num_blocks" => {
                let size: usize = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.delta.data_cache_num_blocks: {e}"
                    ))
                })?;
                self.storage.delta.data_cache_num_blocks = size;
            }
            "storage.delta.log_max_age" => {
                if value.to_lowercase() == "inf" || value.to_lowercase() == "none" {
                    self.storage.delta.log_max_age = None;
                } else {
                    let ms: u64 = value.parse().map_err(|e| {
                        DataFusionError::Configuration(format!(
                            "Invalid value for storage.delta.log_max_age: {e}"
                        ))
                    })?;

                    self.storage.delta.log_max_age = Some(Duration::from_millis(ms));
                }
            }
            "storage.delta.max_buffered_ids" => {
                if value.to_lowercase() == "none" || value.is_empty() {
                    self.storage.delta.max_buffered_ids = None;
                } else {
                    let ids = SessionContext::parse_capacity_limit(key, value)?;
                    self.storage.delta.max_buffered_ids = Some(ids);
                }
            }
            "storage.delta.max_buffered_rows" => {
                if value.to_lowercase() == "none" || value.is_empty() {
                    self.storage.delta.max_buffered_rows = None;
                } else {
                    let rows = SessionContext::parse_capacity_limit(key, value)?;
                    self.storage.delta.max_buffered_rows = Some(rows);
                }
            }
            "storage.delta.object_id_cache_size" => {
                let size = SessionContext::parse_capacity_limit(key, value)?;
                self.storage.delta.object_id_cache_size = Some(size);
            }
            "storage.delta.object_id_claim_size" => {
                let size = SessionContext::parse_capacity_limit(key, value)? as i64;
                self.storage.delta.object_id_claim_size = size;
            }
            "storage.parquet.data_cache_block_size" => {
                let size = SessionContext::parse_capacity_limit(key, value)?;
                self.storage.parquet.data_cache_block_size = size;
            }
            "storage.parquet.data_cache_enabled" => {
                let value: bool = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.parquet.data_cache_enabled: {e}"
                    ))
                })?;
                self.storage.parquet.data_cache_enabled = value;
            }
            "storage.parquet.data_cache_num_blocks" => {
                let size: usize = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.parquet.data_cache_num_blocks: {e}"
                    ))
                })?;
                self.storage.parquet.data_cache_num_blocks = size;
            }
            "storage.parquet.page_statistics_columns" => {
                let columns: usize = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.parquet.page_statistics_columns: {e}"
                    ))
                })?;
                self.storage.parquet.page_statistics_columns = columns;
            }
            "storage.parquet.sort_orders" => {
                let value = value
                    .split(';')
                    .map(str::trim)
                    .filter(|order| !order.is_empty())
                    .map(|order| order.parse::<RdfSortOrder>())
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| {
                        DataFusionError::Configuration(format!(
                            "Invalid value for storage.parquet.sort_orders: {e}"
                        ))
                    })?;
                self.storage.parquet.sort_orders = value;
            }
            "storage.rdf.assume_quads_unique_in_single_file" => {
                let value: bool = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.rdf.assume_quads_unique_in_single_file: {e}"
                    ))
                })?;

                self.storage.rdf_files.assume_quads_unique_in_single_file = value;
            }
            "storage.rdf.parallel_parsing_min_chunk_size" => {
                self.storage.rdf_files.parallel_parsing_min_chunk_size =
                    SessionContext::parse_capacity_limit(key, value)?;
            }
            _ => {
                return Err(DataFusionError::Configuration(format!(
                    "Unknown configuration key: {key}"
                )));
            }
        }
        Ok(())
    }

    fn entries(&self) -> Vec<ConfigEntry> {
        vec![
            ConfigEntry {
                key: format!("{}.execution.small_scan_buffering_threshold", Self::PREFIX),
                value: Some(self.execution.small_scan_buffering_threshold.to_string()),
                description: "The size in bytes below which a quad pattern scan is eagerly buffered during planning to allow parallel execution (e.g. \"1K\", \"1M\", \"1G\"; 0 disables).",
            },
            ConfigEntry {
                key: format!("{}.execution.planning_bloom_filters", Self::PREFIX),
                value: Some(self.execution.planning_bloom_filters.to_string()),
                description: "Whether Bloom filters are used for pruning Parquet row groups during planning (\"auto\", \"always\", or \"never\"). \"auto\" only uses them if the scan caches the data it reads.",
            },
            ConfigEntry {
                key: format!("{}.local.work_dir", Self::PREFIX),
                value: self.local.work_dir.clone(),
                description: "Local workspace directory for storing cache/DB files.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.assume_single_node", Self::PREFIX),
                value: Some(self.storage.delta.assume_single_node.to_string()),
                description: "Whether the node can assume that no other node is currently working on the DeltaQuadss database.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.data_cache_block_size", Self::PREFIX),
                value: Some(self.storage.delta.data_cache_block_size.to_string()),
                description: "The size of the blocks used for caching object store reads in bytes.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.data_cache_num_blocks", Self::PREFIX),
                value: Some(self.storage.delta.data_cache_num_blocks.to_string()),
                description: "The number of blocks to cache for object store reads.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.log_max_age", Self::PREFIX),
                value: self
                    .storage
                    .delta
                    .log_max_age
                    .map(|v| v.as_millis().to_string()),
                description: "The maximum age of the operations log that should be queried before refreshing.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.max_buffered_ids", Self::PREFIX),
                value: self.storage.delta.max_buffered_ids.map(|i| i.to_string()),
                description: "Maximum number of pending IDs to buffer before committing delta dictionary transaction.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.max_buffered_rows", Self::PREFIX),
                value: self.storage.delta.max_buffered_rows.map(|r| r.to_string()),
                description: "Maximum number of rows to buffer before committing delta dictionary transaction.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.object_id_cache_size", Self::PREFIX),
                value: self
                    .storage
                    .delta
                    .object_id_cache_size
                    .map(|i| i.to_string()),
                description: "The size of the cache in the local object id dictionary.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.object_id_claim_size", Self::PREFIX),
                value: Some(self.storage.delta.object_id_claim_size.to_string()),
                description: "The size of claimed ID ranges for delta dictionary coordination.",
            },
            ConfigEntry {
                key: format!("{}.storage.parquet.data_cache_block_size", Self::PREFIX),
                value: Some(self.storage.parquet.data_cache_block_size.to_string()),
                description: "The size of the blocks used for caching object store reads in bytes.",
            },
            ConfigEntry {
                key: format!("{}.storage.parquet.data_cache_enabled", Self::PREFIX),
                value: Some(self.storage.parquet.data_cache_enabled.to_string()),
                description: "Whether data caching is enabled for Parquet storage.",
            },
            ConfigEntry {
                key: format!("{}.storage.parquet.data_cache_num_blocks", Self::PREFIX),
                value: Some(self.storage.parquet.data_cache_num_blocks.to_string()),
                description: "The number of blocks to cache for object store reads.",
            },
            ConfigEntry {
                key: format!("{}.storage.parquet.page_statistics_columns", Self::PREFIX),
                value: Some(self.storage.parquet.page_statistics_columns.to_string()),
                description: "The number of leading sort columns that are written with page-level statistics. The remaining sort columns only have chunk-level statistics, which keeps the page index small. The default (4) writes page-level statistics for all columns.",
            },
            ConfigEntry {
                key: format!("{}.storage.parquet.sort_orders", Self::PREFIX),
                value: (!self.storage.parquet.sort_orders.is_empty()).then(|| {
                    self.storage
                        .parquet
                        .sort_orders
                        .iter()
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join(";")
                }),
                description: "The sort orders of the Parquet files that are written by the loader, separated by \";\" (e.g., \"NATIVE(GPOS);NATIVE(GSPO)\"). Each sort order is written as a separate quad table.",
            },
            ConfigEntry {
                key: format!(
                    "{}.storage.rdf.assume_quads_unique_in_single_file",
                    Self::PREFIX
                ),
                value: Some(
                    self.storage
                        .rdf_files
                        .assume_quads_unique_in_single_file
                        .to_string(),
                ),
                description: "Sets whether the query engine should assume that the quads within a single file are unique.",
            },
            ConfigEntry {
                key: format!(
                    "{}.storage.rdf.parallel_parsing_min_chunk_size",
                    Self::PREFIX
                ),
                value: Some(
                    self.storage
                        .rdf_files
                        .parallel_parsing_min_chunk_size
                        .to_string(),
                ),
                description: "The minimum size in bytes of a chunk of an N-Triples or N-Quads file that is parsed in parallel (e.g. \"1K\", \"16M\").",
            },
        ]
    }
}

impl RdfFusionOptions {
    /// Create a new [`RdfFusionOptions`] with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns the environment variable name corresponding to a configuration ID or key.
    fn env_var_for_id(id: &str) -> String {
        let prefix = format!("{}.", Self::PREFIX);
        let full_key = if id.starts_with(&prefix) {
            id.to_string()
        } else {
            format!("{prefix}{id}")
        };
        full_key.replace('.', "_").to_ascii_uppercase()
    }

    /// Returns a list of `(env_var_name, config_id)` mappings for all configuration options.
    fn env_mappings() -> Vec<(String, String)> {
        let prefix = format!("{}.", Self::PREFIX);
        Self::default()
            .entries()
            .into_iter()
            .map(|entry| {
                let env_var = Self::env_var_for_id(&entry.key);
                let id = entry
                    .key
                    .strip_prefix(&prefix)
                    .unwrap_or(&entry.key)
                    .to_string();
                (env_var, id)
            })
            .collect()
    }

    /// Create a new [`RdfFusionOptions`] by reading environment variables.
    pub fn from_env() -> DFResult<Self> {
        let mut config = Self::default();
        for (env_var, id) in Self::env_mappings() {
            if let Ok(val) = std::env::var(&env_var) {
                config.set(&id, &val)?;
            }
        }
        Ok(config)
    }
}

pub trait RdfFusionSessionConfigExt {
    /// Extracts [`RdfFusionOptions`].
    fn rdf_fusion_options(&self) -> Option<&RdfFusionOptions>;

    /// Extracts [`RdfFusionOptions`], falling back to a default.
    fn rdf_fusion_options_or_default(&self) -> RdfFusionOptions {
        self.rdf_fusion_options().cloned().unwrap_or_default()
    }
}

impl RdfFusionSessionConfigExt for SessionConfig {
    fn rdf_fusion_options(&self) -> Option<&RdfFusionOptions> {
        self.options().extensions.get::<RdfFusionOptions>()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_from_env() {
        unsafe {
            std::env::set_var("RDF_FUSION_STORAGE_DELTA_LOG_MAX_AGE", "12345");
        }
        let config = RdfFusionOptions::from_env().unwrap();
        assert_eq!(
            config.storage.delta.log_max_age,
            Some(Duration::from_millis(12345))
        );

        unsafe {
            std::env::set_var("RDF_FUSION_STORAGE_DELTA_LOG_MAX_AGE", "inf");
        }
        let config = RdfFusionOptions::from_env().unwrap();
        assert_eq!(config.storage.delta.log_max_age, None);

        unsafe {
            std::env::remove_var("RDF_FUSION_STORAGE_DELTA_LOG_MAX_AGE");
        }
        let config = RdfFusionOptions::from_env().unwrap();
        assert_eq!(config.storage.delta.log_max_age, None);
    }

    #[test]
    fn test_config_from_env_parquet_cache() {
        unsafe {
            std::env::set_var("RDF_FUSION_STORAGE_PARQUET_DATA_CACHE_ENABLED", "true");
            std::env::set_var("RDF_FUSION_STORAGE_PARQUET_DATA_CACHE_NUM_BLOCKS", "2048");
        }
        let config = RdfFusionOptions::from_env().unwrap();
        assert!(config.storage.parquet.data_cache_enabled);
        assert_eq!(config.storage.parquet.data_cache_num_blocks, 2048);

        unsafe {
            std::env::remove_var("RDF_FUSION_STORAGE_PARQUET_DATA_CACHE_ENABLED");
            std::env::remove_var("RDF_FUSION_STORAGE_PARQUET_DATA_CACHE_NUM_BLOCKS");
        }
    }

    #[test]
    fn test_env_mappings() {
        let mappings = RdfFusionOptions::env_mappings();
        assert_eq!(mappings.len(), 18);
        assert!(mappings.iter().any(|(env, id)| {
            env == "RDF_FUSION_STORAGE_DELTA_LOG_MAX_AGE"
                && id == "storage.delta.log_max_age"
        }));
        assert!(mappings.iter().any(|(env, id)| {
            env == "RDF_FUSION_STORAGE_PARQUET_DATA_CACHE_ENABLED"
                && id == "storage.parquet.data_cache_enabled"
        }));
        assert!(mappings.iter().any(|(env, id)| {
            env == "RDF_FUSION_EXECUTION_SMALL_SCAN_BUFFERING_THRESHOLD"
                && id == "execution.small_scan_buffering_threshold"
        }));
    }

    #[test]
    fn test_set_sort_orders() {
        let mut config = RdfFusionOptions::default();
        config
            .set(
                "rdf_fusion.storage.parquet.sort_orders",
                "NATIVE(GPOS); NATIVE(GSPO)",
            )
            .unwrap();
        assert_eq!(
            config.storage.parquet.sort_orders,
            vec![
                "NATIVE(GPOS)".parse::<RdfSortOrder>().unwrap(),
                "NATIVE(GSPO)".parse::<RdfSortOrder>().unwrap()
            ]
        );
        assert!(
            config
                .set("rdf_fusion.storage.parquet.sort_orders", "NATIVE(GPOS);X")
                .is_err()
        );
    }

    #[test]
    fn test_set_page_statistics_columns() {
        let mut config = RdfFusionOptions::default();
        assert_eq!(config.storage.parquet.page_statistics_columns, 4);
        config
            .set("rdf_fusion.storage.parquet.page_statistics_columns", "3")
            .unwrap();
        assert_eq!(config.storage.parquet.page_statistics_columns, 3);
        assert!(
            config
                .set("rdf_fusion.storage.parquet.page_statistics_columns", "x")
                .is_err()
        );
    }

    #[test]
    fn test_set_planning_bloom_filters() {
        let mut config = RdfFusionOptions::default();
        assert_eq!(
            config.execution.planning_bloom_filters,
            PlanningBloomFilters::Auto
        );
        config
            .set("rdf_fusion.execution.planning_bloom_filters", "Always")
            .unwrap();
        assert_eq!(
            config.execution.planning_bloom_filters,
            PlanningBloomFilters::Always
        );
        assert!(
            config
                .set("execution.planning_bloom_filters", "sometimes")
                .is_err()
        );
    }

    #[test]
    fn test_set_with_prefix() {
        let mut config = RdfFusionOptions::default();
        config
            .set("rdf_fusion.storage.parquet.data_cache_enabled", "true")
            .unwrap();
        assert!(config.storage.parquet.data_cache_enabled);
    }

    #[test]
    fn test_config_extension_options() {
        let config = RdfFusionOptions::default();
        let entries = config.entries();
        assert_eq!(entries.len(), 18);
    }
}
