use crate::logstore::{IORuntime, ObjectStoreRef, StorageConfig, logstore_with};
use deltalake::DeltaResult;
use deltalake::logstore::LogStoreRef;
use tokio::runtime::Handle;
use url::Url;

pub fn logstore_with_current_runtime(
    root_store: ObjectStoreRef,
    location: &Url,
    mut storage_config: StorageConfig,
) -> DeltaResult<LogStoreRef> {
    if storage_config.runtime.is_none() {
        storage_config.runtime = Some(IORuntime::RT(Handle::current()))
    }

    logstore_with(root_store, location, storage_config)
}
