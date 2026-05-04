use crate::RdfFusionContext;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
use datafusion::prelude::SessionConfig;
use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_model::DFResult;
use std::sync::Arc;
use url::Url;

/// A builder for creating a new [`RdfFusionContext`] instance.
///
/// This builder is not in the `rdf-fusion-execution` crate because it should also have access to,
/// for example, the storage implementation such that we can create a sensible default.
pub struct RdfFusionContextBuilder {
    quad_storage: Arc<dyn QuadStorage>,
    session_config: Option<SessionConfig>,
    query_runtime: Option<Arc<RuntimeEnv>>,
    register_in_memory_store: bool,
}

impl RdfFusionContextBuilder {
    /// Creates a new builder with the given quad storage.
    pub fn new(quad_storage: Arc<dyn QuadStorage>) -> Self {
        Self {
            quad_storage,
            session_config: None,
            query_runtime: None,
            register_in_memory_store: true,
        }
    }

    /// Creates a new builder with the given quad storage and loads the config from the environment.
    pub fn new_from_env(quad_storage: Arc<dyn QuadStorage>) -> DFResult<Self> {
        Ok(Self {
            quad_storage,
            session_config: Some(SessionConfig::from_env()?),
            query_runtime: None,
            register_in_memory_store: true,
        })
    }

    /// Sets the DataFusion [`SessionConfig`] for this RDF Fusion instance.
    pub fn with_session_config(mut self, session_config: Option<SessionConfig>) -> Self {
        self.session_config = session_config;
        self
    }

    /// Sets the DataFusion [`SessionConfig`] for this RDF Fusion instance.
    pub fn with_single_partition_session_config(self) -> Self {
        let mut session_config = SessionConfig::new()
            .with_batch_size(8192)
            .with_target_partitions(1);
        let options = session_config.options_mut();
        options.execution.parquet.pushdown_filters = true;
        self.with_session_config(Some(session_config))
    }

    /// Sets the [`RuntimeEnv`] for this RDF Fusion instance. This can be used to limit the amount
    /// of memory used by the instance.
    pub fn with_runtime_env(mut self, query_runtime: Option<Arc<RuntimeEnv>>) -> Self {
        self.query_runtime = query_runtime;
        self
    }

    /// Sets whether an in-memory object store should be registered.
    pub fn with_register_in_memory_store(
        mut self,
        register_in_memory_store: bool,
    ) -> Self {
        self.register_in_memory_store = register_in_memory_store;
        self
    }

    /// Consumes the builder to create the Store
    pub fn build(self) -> DFResult<RdfFusionContext> {
        let typed_family_encoding = Arc::new(TypedFamilyEncoding::default());
        let session_config = match self.session_config {
            None => SessionConfig::from_env()?,
            Some(session_config) => session_config,
        };
        let runtime_env = self.query_runtime.unwrap_or_else(|| {
            RuntimeEnvBuilder::default()
                .build_arc()
                .expect("Default runtime env")
        });

        // Ensure that we have an in-memory object store registered.
        if self.register_in_memory_store {
            let memory_url = Url::parse("memory://").unwrap();
            if runtime_env
                .object_store_registry
                .get_store(&memory_url)
                .is_err()
            {
                runtime_env.register_object_store(
                    &memory_url,
                    Arc::new(object_store::memory::InMemory::new()),
                );
            }
        }

        Ok(RdfFusionContext::new(
            session_config,
            runtime_env,
            self.quad_storage,
            typed_family_encoding,
        ))
    }
}
