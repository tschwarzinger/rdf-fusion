use datafusion::common::{DataFusionError, Result as DFResult};
use datafusion::datasource::object_store::ObjectStoreUrl;
use url::Url;

/// Transforms a regular [`Url`] into an [`ObjectStoreUrl`] that can be used to identify an object
/// store in a registry.
///
/// Taken from <https://github.com/delta-io/delta-rs/blob/fd7e96910243f9e67b4eae994d52ef246cfcea38/crates/core/src/delta_datafusion/engine/storage.rs#L153>
pub fn url_to_object_store_url(url: &Url) -> DFResult<ObjectStoreUrl> {
    let object_store_url = format!(
        "{}://{}",
        url.scheme(),
        &url[url::Position::BeforeHost..url::Position::AfterPort],
    );
    ObjectStoreUrl::parse(object_store_url).map_err(|e| {
        DataFusionError::External(format!("Invalid object store URL '{url}': {e}").into())
    })
}
