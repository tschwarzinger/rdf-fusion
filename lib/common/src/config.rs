use crate::{DFResult, RdfSortOrder};
use datafusion::common::config::{ConfigEntry, ConfigExtension, ExtensionOptions};
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;
use std::any::Any;
use std::time::Duration;

/// Configuration for RDF Fusion.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RdfFusionOptions {
    /// Storage configuration.
    pub storage: StorageOptions,
    /// Local configuration.
    pub local: LocalOptions,
}

impl ConfigExtension for RdfFusionOptions {
    const PREFIX: &'static str = "rdf_fusion";
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
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ParquetStorageOptions {
    /// The sort order for the Parquet files.
    pub sort_order: Option<RdfSortOrder>,
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
    pub object_id_cache_size: usize,
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
            object_id_cache_size: 1_000_000, // 1m items
            assume_single_node: false,
            data_cache_block_size: 2 * 1024 * 1024, // 2 MiB
            data_cache_num_blocks: 1024,
        }
    }
}

/// Options related to working with RDF files (e.g., Turtle).
#[derive(Debug, Clone, PartialEq, Default)]
pub struct RdfFileStorageOptions {
    /// Whether the query engine should assume that the quads within one file are unique.
    pub assume_quads_unique_in_single_file: bool,
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
        match key {
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
            "storage.delta.object_id_claim_size" => {
                let size = datafusion::prelude::SessionContext::parse_capacity_limit(
                    key, value,
                )? as i64;
                self.storage.delta.object_id_claim_size = size;
            }
            "storage.delta.max_buffered_rows" => {
                if value.to_lowercase() == "none" || value.is_empty() {
                    self.storage.delta.max_buffered_rows = None;
                } else {
                    let rows = datafusion::prelude::SessionContext::parse_capacity_limit(
                        key, value,
                    )?;
                    self.storage.delta.max_buffered_rows = Some(rows);
                }
            }
            "storage.delta.max_buffered_ids" => {
                if value.to_lowercase() == "none" || value.is_empty() {
                    self.storage.delta.max_buffered_ids = None;
                } else {
                    let ids = datafusion::prelude::SessionContext::parse_capacity_limit(
                        key, value,
                    )?;
                    self.storage.delta.max_buffered_ids = Some(ids);
                }
            }
            "storage.delta.object_id_cache_size" => {
                let size = datafusion::prelude::SessionContext::parse_capacity_limit(
                    key, value,
                )?;
                self.storage.delta.object_id_cache_size = size;
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
                let size = datafusion::prelude::SessionContext::parse_capacity_limit(
                    key, value,
                )?;
                self.storage.delta.data_cache_block_size = size;
            }
            "storage.delta.data_cache_num_blocks" => {
                let size = datafusion::prelude::SessionContext::parse_capacity_limit(
                    key, value,
                )?;
                self.storage.delta.data_cache_num_blocks = size;
            }
            "storage.parquet.sort_order" => {
                let value: RdfSortOrder = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.parquet.sort_order: {e}"
                    ))
                })?;
                self.storage.parquet.sort_order = Some(value);
            }
            "storage.rdf.assume_quads_unique_in_single_file" => {
                let value: bool = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.delta.log_max_age: {e}"
                    ))
                })?;

                self.storage.rdf_files.assume_quads_unique_in_single_file = value;
            }
            "local.work_dir" => {
                self.local.work_dir = Some(value.to_string());
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
                key: format!("{}.storage.delta.log_max_age", Self::PREFIX),
                value: self
                    .storage
                    .delta
                    .log_max_age
                    .map(|v| v.as_millis().to_string()),
                description: "The maximum age of the operations log that should be queried before refreshing.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.object_id_claim_size", Self::PREFIX),
                value: Some(self.storage.delta.object_id_claim_size.to_string()),
                description: "The size of claimed ID ranges for delta dictionary coordination.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.max_buffered_rows", Self::PREFIX),
                value: self.storage.delta.max_buffered_rows.map(|r| r.to_string()),
                description: "Maximum number of rows to buffer before committing delta dictionary transaction.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.max_buffered_ids", Self::PREFIX),
                value: self.storage.delta.max_buffered_ids.map(|i| i.to_string()),
                description: "Maximum number of pending IDs to buffer before committing delta dictionary transaction.",
            },
            ConfigEntry {
                key: format!("{}.storage.delta.object_id_cache_size", Self::PREFIX),
                value: Some(self.storage.delta.object_id_cache_size.to_string()),
                description: "The size of the cache in the local object id dictionary.",
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
                key: format!("{}.storage.parquet.sort_order", Self::PREFIX),
                value: self
                    .storage
                    .parquet
                    .sort_order
                    .as_ref()
                    .map(|so| so.to_string())
                    .clone(),
                description: "The sort order for the Parquet files.",
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
                key: format!("{}.local.work_dir", Self::PREFIX),
                value: self.local.work_dir.clone(),
                description: "Local workspace directory for storing cache/DB files.",
            },
        ]
    }
}

impl RdfFusionOptions {
    /// Create a new [`RdfFusionOptions`] with default options.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new [`RdfFusionOptions`] by reading environment variables.
    pub fn from_env() -> DFResult<Self> {
        let mut config = Self::default();
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_DELTA_LOG_MAX_AGE") {
            config.set("storage.delta.log_max_age", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_DELTA_OBJECT_ID_CLAIM_SIZE") {
            config.set("storage.delta.object_id_claim_size", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_DELTA_MAX_BUFFERED_ROWS") {
            config.set("storage.delta.max_buffered_rows", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_DELTA_MAX_BUFFERED_IDS") {
            config.set("storage.delta.max_buffered_ids", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_DELTA_OBJECT_ID_CACHE_SIZE") {
            config.set("storage.delta.object_id_cache_size", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_DELTA_ASSUME_SINGLE_NODE") {
            config.set("storage.delta.assume_single_node", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_DELTA_DATA_CACHE_BLOCK_SIZE") {
            config.set("storage.delta.block_cache_block_size", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_DELTA_DATA_CACHE_NUM_BLOCKS") {
            config.set("storage.delta.block_cache_num_blocks", &val)?;
        }

        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_PARQUET_TARGET_FILE_COUNT") {
            config.set("storage.parquet.target_file_count", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_STORAGE_PARQUET_SORT_ORDER") {
            config.set("storage.parquet.sort_order", &val)?;
        }

        if let Ok(val) =
            std::env::var("RDF_FUSION_STORAGE_RDF_ASSUME_QUADS_UNIQUE_IN_SINGLE_FILE")
        {
            config.set("storage.rdf.assume_quads_unique_in_single_file", &val)?;
        }
        if let Ok(val) = std::env::var("RDF_FUSION_LOCAL_WORK_DIR") {
            config.set("local.work_dir", &val)?;
        }
        Ok(config)
    }
}

pub trait RdfFusionSessionConfigExt {
    /// Extracts [`RdfFusionOptions`], falling back to a default.
    fn rdf_fusion_options_or_default(&self) -> RdfFusionOptions;

    /// Extracts [`RdfFusionOptions`], falling back to environment variables.
    fn rdf_fusion_options_or_from_env(&self) -> DFResult<RdfFusionOptions>;
}

impl RdfFusionSessionConfigExt for SessionConfig {
    fn rdf_fusion_options_or_default(&self) -> RdfFusionOptions {
        self.options()
            .extensions
            .get::<RdfFusionOptions>()
            .cloned()
            .unwrap_or_default()
    }

    fn rdf_fusion_options_or_from_env(&self) -> DFResult<RdfFusionOptions> {
        match self.options().extensions.get::<RdfFusionOptions>() {
            None => RdfFusionOptions::from_env(),
            Some(config) => Ok(config.clone()),
        }
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
    fn test_config_extension_options() {
        let config = RdfFusionOptions::default();
        let entries = config.entries();
        assert_eq!(entries.len(), 11);
    }
}
