use crate::w3c::StoreFactory;
use crate::w3c::files::W3CTestRuntime;
use crate::w3c::utils::W3CTestUtils;
use ::rdf_fusion::encoding::QuadStorageEncodingName;
use ::rdf_fusion::execution::RdfFusionContextBuilder;
use ::rdf_fusion::storage::delta::DeltaQuadsStorageBuilder;
use ::rdf_fusion::storage::parquet::{ParquetQuadStorage, RdfParquetLoader};
use ::rdf_fusion::store::Store;
use datafusion::execution::runtime_env::RuntimeEnv;
use deltalake::logstore::{IORuntime, LogStoreRef, StorageConfig, logstore_with};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use rdf_fusion_common::RdfInput;
use std::sync::Arc;
use tokio::runtime::Handle;

/// Registers a shared [`InMemory`] object store in the given runtime environment and returns a
/// log store backed by it. The same object store must be used both when writing the delta data
/// files and when scanning them (delta scans resolve `memory:///` through the session's object
/// store registry), otherwise the written files are not found during querying.
fn in_memory_delta_log_store(runtime_env: &RuntimeEnv) -> anyhow::Result<LogStoreRef> {
    let memory_store = Arc::new(InMemory::new()) as Arc<dyn ObjectStore>;
    let registry = &runtime_env.object_store_registry;
    let url = url::Url::parse("memory:///").unwrap();
    registry.register_store(&url, Arc::clone(&memory_store));
    let log_store = logstore_with(
        memory_store,
        &url,
        StorageConfig::default().with_io_runtime(IORuntime::RT(Handle::current())),
    )?;
    Ok(log_store)
}

/// Creates the [`Store`] using the plain term encoding that is used for the plain term encoding
/// tests.
pub fn delta_quads_store_factory(encoding: QuadStorageEncodingName) -> StoreFactory {
    Arc::new(move |store_config| {
        Box::pin(async move {
            let log_store = in_memory_delta_log_store(&store_config.runtime_env)?;

            let delta_storage = DeltaQuadsStorageBuilder::new()
                .with_encoding(encoding)
                .with_log_store(log_store)
                .build()
                .await?;

            let context = RdfFusionContextBuilder::new(Arc::new(delta_storage))
                .with_runtime_env(Some(store_config.runtime_env))
                .with_single_partition_session_config()
                .build()?;
            let store = Store::new(context);

            let utils = W3CTestUtils::new(W3CTestRuntime::new(
                store.context().session_context().runtime_env(),
            ));
            for (name, source) in store_config.default_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }
            for (name, source) in store_config.named_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }

            Ok(store)
        })
    })
}

/// Creates the [`Store`] using the data dump storage.
pub fn parquet_store_factory(encoding: QuadStorageEncodingName) -> StoreFactory {
    Arc::new(move |config| {
        Box::pin(async move {
            let output_url = url::Url::parse("memory:///dataset.parquet").unwrap();

            let delta_storage =
                Arc::new(DeltaQuadsStorageBuilder::new().build().await.unwrap());
            let context = RdfFusionContextBuilder::new(delta_storage)
                .with_runtime_env(Some(Arc::clone(&config.runtime_env)))
                .with_single_partition_session_config()
                .build()
                .unwrap();

            let loader = RdfParquetLoader::try_new(
                context.session_context().clone(),
                context.create_view(),
                encoding,
                None,
            )
            .unwrap();
            let mut inputs = Vec::new();
            for (name, source) in config.default_graphs {
                inputs.push(RdfInput::new_with_format(source.url, name, source.format));
            }
            for (name, source) in config.named_graphs {
                inputs.push(RdfInput::new_with_format(source.url, name, source.format));
            }

            loader.load_many(inputs, output_url.clone()).await.unwrap();

            let storage = ParquetQuadStorage::try_load(
                output_url,
                encoding,
                config.runtime_env.object_store_registry.as_ref(),
            )
            .await
            .unwrap();

            let context = RdfFusionContextBuilder::new(Arc::new(storage))
                .with_runtime_env(Some(config.runtime_env))
                .with_single_partition_session_config()
                .build()
                .unwrap();

            Ok(Store::new(context))
        })
    })
}
