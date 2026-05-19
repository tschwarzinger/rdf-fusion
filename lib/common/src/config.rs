use crate::DFResult;
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
}

impl ConfigExtension for RdfFusionOptions {
    const PREFIX: &'static str = "rdf_fusion";
}

/// Storage configuration for RDF Fusion.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct StorageOptions {
    /// Delta storage configuration.
    pub delta: DeltaStorageOptions,
    /// RDF file storage options.
    pub rdf_files: RdfFileStorageOptions,
}

/// Delta storage configuration for RDF Fusion.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DeltaStorageOptions {
    /// The maximum age of the operations log that should be queried before refreshing.
    pub log_max_age: Option<Duration>,
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
            "storage.rdf.assume_quads_unique_in_single_file" => {
                let value: bool = value.parse().map_err(|e| {
                    DataFusionError::Configuration(format!(
                        "Invalid value for storage.delta.log_max_age: {e}"
                    ))
                })?;

                self.storage.rdf_files.assume_quads_unique_in_single_file = value;
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
        if let Ok(val) =
            std::env::var("RDF_FUSION_STORAGE_RDF_ASSUME_QUADS_UNIQUE_IN_SINGLE_FILE")
        {
            config.set("storage.rdf.assume_quads_unique_in_single_file", &val)?;
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
        assert_eq!(entries.len(), 2);
    }
}
