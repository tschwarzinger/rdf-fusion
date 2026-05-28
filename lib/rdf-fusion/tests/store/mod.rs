use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use rdf_fusion::store::Store;
use rdf_fusion_encoding::QuadStorageEncodingName;
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_storage::parquet::ParquetQuadStorage;
use std::sync::Arc;
use url::Url;

mod dump_fallback;
mod store_tests;

pub async fn create_store_for_result(
    runtime_env: Arc<RuntimeEnv>,
    path: &str,
    encoding: QuadStorageEncodingName,
) -> Store {
    let url = Url::parse(path).unwrap();
    let storage = ParquetQuadStorage::try_load(
        url,
        encoding,
        runtime_env.object_store_registry.as_ref(),
    )
    .await
    .unwrap();
    let context = RdfFusionContextBuilder::new(Arc::new(storage))
        .with_runtime_env(Some(runtime_env))
        .build()
        .unwrap();
    Store::new(context)
}

pub async fn read_dump(ctx: &SessionContext, output_url: &str) -> String {
    let url = Url::parse(output_url).unwrap();
    let object_store = ctx
        .runtime_env()
        .object_store(&url.as_object_store_url())
        .unwrap();
    let path = Path::from(url.path());
    let bytes = object_store
        .get(&path)
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap();
    String::from_utf8(bytes.to_vec()).unwrap()
}
