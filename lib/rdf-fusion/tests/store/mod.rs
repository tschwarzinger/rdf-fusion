use datafusion::execution::runtime_env::RuntimeEnv;
use datafusion::prelude::SessionContext;
use deltalake::delta_datafusion::engine::AsObjectStoreUrl;
use object_store::ObjectStoreExt;
use object_store::path::Path;
use rdf_fusion::common::GraphName;
use rdf_fusion::store::Store;
use rdf_fusion_common::RdfFormat;
use rdf_fusion_execution::RdfFusionContextBuilder;
use rdf_fusion_storage::rdf_files::{RdfFileQuadStorage, RdfFileSourceConfig};
use std::sync::Arc;
use url::Url;

mod dump_fallback;
mod store_tests;

fn create_store_for_result(
    runtime_env: Arc<RuntimeEnv>,
    path: &str,
    format: RdfFormat,
) -> Store {
    let storage = RdfFileQuadStorage::new(vec![(
        GraphName::DefaultGraph,
        RdfFileSourceConfig {
            url: path.to_string(),
            format: format,
        },
    )]);

    let context = RdfFusionContextBuilder::new(Arc::new(storage))
        .with_runtime_env(Some(runtime_env))
        .build()
        .unwrap();
    Store::new(context)
}

async fn read_dump(context: &SessionContext, output_url: &str) -> anyhow::Result<String> {
    let url = Url::parse(output_url)?;
    let runtime_env = context.runtime_env();
    let object_store = runtime_env.object_store(url.as_object_store_url())?;
    let path = Path::from(url.path());
    let bytes = object_store.get(&path).await?.bytes().await?;
    Ok(String::from_utf8(bytes.to_vec())?)
}
