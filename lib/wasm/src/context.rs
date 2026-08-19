use crate::indexeddb_store::IndexedDbObjectStore;
use bytes::Bytes;
use datafusion::datasource::object_store::{
    DefaultObjectStoreRegistry, ObjectStoreRegistry,
};
use datafusion::execution::DiskManager;
use datafusion::execution::disk_manager::DiskManagerMode;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use object_store::ObjectStoreExt;
use object_store::http::HttpBuilder;
use object_store::memory::InMemory;
use rdf_fusion::common::config::RdfFusionOptions;
use rdf_fusion::common::{GraphName, RdfInput, RdfSortOrder};
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::{RdfFusionContext, RdfFusionContextBuilder};
use rdf_fusion::extensions::RdfFusionContextView;
use rdf_fusion::extensions::functions::RdfFusionFunctionRegistry;
use rdf_fusion::functions::registry::DefaultRdfFusionFunctionRegistry;
use rdf_fusion::storage::parquet::{ParquetQuadStorage, RdfParquetLoader};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion_encoding::{QuadStorageEncoding, RdfFusionEncodings};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use url::Url;
use wasm_bindgen::prelude::*;

/// Defines the encoding used for the quad storage.
#[wasm_bindgen]
pub enum JsQuadStorageEncoding {
    /// Store RDF terms as `struct` using separate fields for values and datatypes.
    PlainTerm,
    /// Stores RDF terms as strings.
    String,
    /// Stores RDF terms as integers. This requires a global dictionary that maps the ids to the
    /// terms.
    ObjectId,
}

impl From<JsQuadStorageEncoding> for QuadStorageEncodingName {
    fn from(enc: JsQuadStorageEncoding) -> Self {
        match enc {
            JsQuadStorageEncoding::PlainTerm => QuadStorageEncodingName::PlainTerm,
            JsQuadStorageEncoding::String => QuadStorageEncodingName::String,
            JsQuadStorageEncoding::ObjectId => QuadStorageEncodingName::ObjectId,
        }
    }
}

/// Represents an instance of an RDF Fusion engine that can be used to execute queries.
#[wasm_bindgen]
pub struct JsRdfFusionContext {
    pub(crate) inner: RdfFusionContext,
    pub(crate) metrics: JsExplainConfig,
}

/// Defines the configuration options for the RDF Fusion engine.
#[wasm_bindgen]
#[derive(Clone, Copy)]
pub struct JsExplainConfig {
    pub show_metrics: bool,
    pub show_statistics: bool,
}

#[wasm_bindgen]
impl JsExplainConfig {
    #[wasm_bindgen(constructor)]
    pub fn new(show_metrics: bool, show_statistics: bool) -> Self {
        Self {
            show_metrics,
            show_statistics,
        }
    }
}

/// Defines the configuration options for the RDF Fusion engine.
#[wasm_bindgen]
#[derive(Clone)]
pub struct JsEngineConfig {
    memory_limit_mb: usize,
    metrics: JsExplainConfig,
    custom_config: HashMap<String, String>,
}

#[wasm_bindgen]
impl JsEngineConfig {
    /// Create a new [`JsEngineConfig`].
    #[wasm_bindgen(constructor)]
    pub fn new(
        memory_limit_mb: usize,
        metrics: JsExplainConfig,
        custom_config_obj: js_sys::Object,
    ) -> Self {
        let mut custom_config = HashMap::new();
        for key in js_sys::Object::keys(&custom_config_obj).iter() {
            if let Some(key_str) = key.as_string() {
                if let Ok(value) = js_sys::Reflect::get(&custom_config_obj, &key) {
                    if let Some(val_str) = value.as_string() {
                        custom_config.insert(key_str, val_str);
                    }
                }
            }
        }

        Self {
            memory_limit_mb,
            metrics,
            custom_config,
        }
    }

    /// The configured memory limit of the engine in MB.
    pub fn memory_limit_mb(&self) -> usize {
        self.memory_limit_mb
    }

    /// The configuration for displaying metrics.
    pub fn metrics(&self) -> JsExplainConfig {
        self.metrics
    }
}

/// Creates a new [`JsRdfFusionContext`] that can be used to query the Parquet file stored at the
/// given URL.
#[wasm_bindgen]
pub async fn create_http_parquet_context(
    url: &str,
    encoding: JsQuadStorageEncoding,
    settings: JsEngineConfig,
) -> Result<JsRdfFusionContext, JsValue> {
    let url = Url::parse(url).map_err(|e| e.to_string())?;
    let registry = Arc::new(DefaultObjectStoreRegistry::new());

    let base_url = Url::parse(&format!(
        "{}://{}",
        url.scheme(),
        url.host_str().unwrap_or("")
    ))
    .map_err(|e| e.to_string())?;

    let fetch_store = HttpBuilder::new()
        .with_url(base_url.clone())
        .build()
        .map_err(|e| e.to_string())?;
    registry.register_store(&base_url, Arc::new(fetch_store));

    create_parquet_context(url, encoding, settings, registry).await
}

/// Creates a new [`JsRdfFusionContext`] that can be used to query the Parquet file stored in the
/// given byte buffer. This can be used
#[wasm_bindgen]
pub async fn create_parquet_context_from_buffer(
    data: &[u8],
    encoding: JsQuadStorageEncoding,
    settings: JsEngineConfig,
) -> Result<JsRdfFusionContext, JsValue> {
    let registry = Arc::new(DefaultObjectStoreRegistry::new());
    let memory_store = Arc::new(InMemory::new());
    let path = object_store::path::Path::from("data.parquet");

    memory_store
        .put(&path, Bytes::copy_from_slice(data).into())
        .await
        .map_err(|e| e.to_string())?;

    registry.register_store(&Url::parse("memory://").unwrap(), memory_store);
    let url = Url::parse("memory:///data.parquet").unwrap();

    create_parquet_context(url, encoding, settings, registry).await
}

/// Creates a new [`JsRdfFusionContext`] using `IndexedDbObjectStore` to read lazily
/// from IndexedDB, avoiding loading the whole file into memory.
#[wasm_bindgen]
pub async fn create_parquet_context_from_indexeddb(
    db_name: String,
    key: String,
    encoding: JsQuadStorageEncoding,
    settings: JsEngineConfig,
) -> Result<JsRdfFusionContext, JsValue> {
    let registry = Arc::new(DefaultObjectStoreRegistry::new());
    let idb_store = Arc::new(IndexedDbObjectStore::new(db_name));

    registry.register_store(&Url::parse("indexeddb://").unwrap(), idb_store);
    let key_trimmed = key.trim_start_matches('/');
    let url = Url::parse(&format!("indexeddb:///{}", key_trimmed))
        .map_err(|e| e.to_string())?;

    create_parquet_context(url, encoding, settings, registry).await
}

/// Converts a custom RDF file stored in IndexedDB into a Parquet v0.1 blob and streams it into the given db.
#[wasm_bindgen]
pub async fn convert_rdf_to_parquet_stream(
    db_name: String,
    input_key: String,
    output_key: String,
    encoding: JsQuadStorageEncoding,
    sort_order_str: Option<String>,
) -> Result<(), JsValue> {
    let session_context = SessionContext::new();
    let registry = session_context.runtime_env().object_store_registry.clone();

    let idb_store = Arc::new(IndexedDbObjectStore::new(db_name.clone()));
    registry.register_store(&Url::parse("indexeddb://").unwrap(), idb_store);

    let typed_family_encoding = Arc::new(TypedFamilyEncoding::default());
    let encodings = RdfFusionEncodings::new(
        Arc::clone(&PLAIN_TERM_ENCODING),
        typed_family_encoding,
        None,
        Arc::clone(&STRING_ENCODING),
    );

    let function_registry: Arc<dyn RdfFusionFunctionRegistry> =
        Arc::new(DefaultRdfFusionFunctionRegistry::new(encodings.clone()));

    let quad_storage_encoding = match encoding {
        JsQuadStorageEncoding::PlainTerm => QuadStorageEncoding::PlainTerm,
        JsQuadStorageEncoding::String => QuadStorageEncoding::String,
        JsQuadStorageEncoding::ObjectId => {
            return Err(JsValue::from_str(
                "Object ID encoding is not supported for loader without explicit dictionary mapping",
            ));
        }
    };

    let context_view =
        RdfFusionContextView::new(function_registry, encodings, quad_storage_encoding);

    let sort_order = if let Some(s) = sort_order_str {
        Some(RdfSortOrder::from_str(&s).map_err(|e| JsValue::from_str(&e.to_string()))?)
    } else {
        None
    };

    let loader = RdfParquetLoader::try_new(
        session_context,
        context_view,
        encoding.into(),
        sort_order,
    )
    .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let in_key_trimmed = input_key.trim_start_matches('/');
    let input = RdfInput::try_new(
        Url::parse(&format!("indexeddb:///{}", in_key_trimmed)).unwrap(),
        GraphName::DefaultGraph,
    )
    .map_err(|e| JsValue::from_str(&e.to_string()))?;

    let out_key_trimmed = output_key.trim_start_matches('/');
    let output_url = Url::parse(&format!("indexeddb:///{}", out_key_trimmed)).unwrap();

    crate::runtime::run(async move {
        loader
            .load_many(vec![input], output_url)
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))
    })
    .await??;

    Ok(())
}

/// Internal function for creating a Parquet context.
async fn create_parquet_context(
    url: Url,
    encoding: JsQuadStorageEncoding,
    settings: JsEngineConfig,
    registry: Arc<DefaultObjectStoreRegistry>,
) -> Result<JsRdfFusionContext, JsValue> {
    crate::runtime::run(async move {
        let mut session_config = SessionConfig::new()
            .with_option_extension(RdfFusionOptions::default())
            .with_target_partitions(1)
            .with_batch_size(8192);
        session_config
            .options_mut()
            .execution
            .parquet
            .pushdown_filters = true;

        for (k, v) in &settings.custom_config {
            let _ = session_config.options_mut().set(k, v);
        }

        let rdf_fusion_options = session_config
            .options()
            .extensions
            .get::<RdfFusionOptions>()
            .cloned()
            .unwrap_or_default();

        let storage = ParquetQuadStorage::builder(url)
            .with_encoding(encoding.into())
            .with_object_store_registry(registry.as_ref())
            .with_options(rdf_fusion_options.storage.parquet)
            .build()
            .await
            .map_err(|e| e.to_string())?;

        let runtime = RuntimeEnvBuilder::default()
            .with_object_store_registry(registry)
            .with_disk_manager_builder(
                DiskManager::builder().with_mode(DiskManagerMode::Disabled),
            )
            .with_memory_limit(settings.memory_limit_mb * 1024 * 1024, 1.0)
            .build_arc()
            .map_err(|e| e.to_string())?;

        let context = RdfFusionContextBuilder::new(Arc::new(storage))
            .with_session_config(Some(session_config))
            .with_runtime_env(Some(runtime))
            .build()
            .map_err(|e| e.to_string())?;

        Ok(JsRdfFusionContext {
            inner: context,
            metrics: settings.metrics,
        })
    })
    .await?
}
