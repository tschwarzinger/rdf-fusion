use crate::local_object_ids::{LocalObjectIdError, ObjectIdClaimer};
use async_trait::async_trait;
use bytes::Bytes;
use deltalake::logstore::LogStoreRef;
use object_store::{ObjectStoreExt, PutMode, PutOptions, UpdateVersion};
use tracing::info;

#[derive(Debug)]
pub struct DeltaObjectIdClaimer {
    log_store: LogStoreRef,
    claim_file_path: object_store::path::Path,
    claim_size: i64,
    put_mode: ObjectIdClaimerPutMode,
}

#[derive(Debug)]
pub enum ObjectIdClaimerPutMode {
    AlwaysOverwrite,
    EnsureVersion,
}

impl ObjectIdClaimerPutMode {
    /// Creates the actual object store put mode.
    fn put_mode_for(
        &self,
        version_info: Option<(Option<String>, Option<String>)>,
    ) -> PutMode {
        match self {
            ObjectIdClaimerPutMode::AlwaysOverwrite => PutMode::Overwrite,
            ObjectIdClaimerPutMode::EnsureVersion => {
                if let Some((e_tag, version)) = version_info {
                    PutMode::Update(UpdateVersion { e_tag, version })
                } else {
                    PutMode::Create
                }
            }
        }
    }
}

impl DeltaObjectIdClaimer {
    pub fn new(
        log_store: LogStoreRef,
        claim_size: i64,
        put_mode: ObjectIdClaimerPutMode,
    ) -> Self {
        let claim_file_path =
            object_store::path::Path::from("_delta_quads/next-free-id.txt");
        Self {
            log_store,
            claim_file_path,
            claim_size,
            put_mode,
        }
    }
}

#[async_trait]
impl ObjectIdClaimer for DeltaObjectIdClaimer {
    async fn claim_next_range(&self) -> Result<(i64, i64), LocalObjectIdError> {
        let object_store = self.log_store.object_store(None);

        let mut retries = 0;
        loop {
            if retries >= 10 {
                return Err(LocalObjectIdError::ObjectIdClaimer(
                    "Exceeded maximum retries (10) for claiming object IDs".to_string(),
                ));
            }

            // Read current value
            let (current_id, version_info) = match object_store
                .get(&self.claim_file_path)
                .await
            {
                Ok(result) => {
                    let version = result.meta.version.clone();
                    let e_tag = result
                        .meta
                        .e_tag
                        .clone()
                        .map(|s| s.trim_matches('"').to_string());

                    let bytes = result.bytes().await.map_err(|e| {
                        LocalObjectIdError::ObjectIdClaimer(e.to_string())
                    })?;
                    let content = String::from_utf8(bytes.to_vec()).map_err(|e| {
                        LocalObjectIdError::ObjectIdClaimer(e.to_string())
                    })?;
                    let id: i64 = content.trim().parse().map_err(
                        |e: std::num::ParseIntError| {
                            LocalObjectIdError::ObjectIdClaimer(e.to_string())
                        },
                    )?;
                    (id, Some((e_tag, version)))
                }
                Err(object_store::Error::NotFound { .. }) => (0, None),
                Err(e) => return Err(LocalObjectIdError::ObjectIdClaimer(e.to_string())),
            };

            let next_free_id = current_id + self.claim_size;
            let next_id = next_free_id - 1;

            let new_content = next_free_id.to_string();
            let payload = Bytes::from(new_content);

            let mode = self.put_mode.put_mode_for(version_info);
            let opts = PutOptions {
                mode: mode.clone(),
                ..Default::default()
            };

            let put_result = object_store
                .put_opts(&self.claim_file_path, payload.clone().into(), opts.clone())
                .await;
            match put_result {
                Ok(_) => return Ok((current_id, next_id)),
                Err(object_store::Error::NotImplemented { .. })
                | Err(object_store::Error::NotSupported { .. }) => {
                    return Err(LocalObjectIdError::ObjectIdClaimer(
                        "The object store does not support conditional puts. If you are running a single node, you can set the RDF_FUSION_STORAGE_DELTA_ASSUME_SINGLE_NODE=true environment variable to bypass this check.".to_string(),
                    ));
                }
                Err(object_store::Error::AlreadyExists { .. })
                | Err(object_store::Error::Precondition { .. }) => {
                    // Precondition failed (someone else claimed in the meantime), retry
                    retries += 1;
                    info!(
                        "Claiming object ids failed. Another node already claimed the object ids with the put mode '{mode:?}'.",
                    );
                    continue;
                }
                Err(e) => return Err(LocalObjectIdError::ObjectIdClaimer(e.to_string())),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use deltalake::logstore::{StorageConfig, logstore_with};
    use object_store::memory::InMemory;
    use std::sync::Arc;
    use url::Url;

    #[tokio::test]
    async fn test_delta_object_id_claimer() {
        run_delta_object_id_claimer_test(ObjectIdClaimerPutMode::AlwaysOverwrite).await;
    }

    #[tokio::test]
    async fn test_delta_object_id_claimer_ensure_version() {
        run_delta_object_id_claimer_test(ObjectIdClaimerPutMode::EnsureVersion).await;
    }

    async fn run_delta_object_id_claimer_test(put_mode: ObjectIdClaimerPutMode) {
        let object_store = Arc::new(InMemory::new());
        let base_url = Url::parse("memory:///").unwrap();
        let log_store = logstore_with(
            Arc::clone(&object_store) as Arc<dyn object_store::ObjectStore>,
            &base_url,
            StorageConfig::default(),
        )
        .unwrap();

        let claimer = DeltaObjectIdClaimer::new(Arc::clone(&log_store), 1000, put_mode);

        let (start1, limit1) = claimer.claim_next_range().await.unwrap();
        assert_eq!(start1, 0);
        assert_eq!(limit1, 999);

        let (start2, limit2) = claimer.claim_next_range().await.unwrap();
        assert_eq!(start2, 1000);
        assert_eq!(limit2, 1999);

        let (start3, limit3) = claimer.claim_next_range().await.unwrap();
        assert_eq!(start3, 2000);
        assert_eq!(limit3, 2999);
    }
}
