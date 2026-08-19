use async_trait::async_trait;
use bytes::Bytes;
use chrono::DateTime;
use futures::channel::oneshot;
use futures::stream::BoxStream;
use js_sys::futures::JsFuture;
use object_store::{
    CopyOptions, GetOptions, GetResult, GetResultPayload, ListResult, MultipartUpload,
    ObjectMeta, ObjectStore, PutMode, PutMultipartOptions, PutOptions, PutPayload,
    PutResult, Result as OsResult, path::Path,
};
use std::cell::RefCell;
use std::collections::HashMap;
use std::ops::Range;
use wasm_bindgen::JsCast;
use wasm_bindgen::prelude::*;
use wasm_bindgen_futures::spawn_local;
use web_sys::{
    Blob, BlobPropertyBag, IdbDatabase, IdbFactory, IdbOpenDbRequest, IdbRequest,
    IdbTransactionMode,
};

thread_local! {
    /// A thread-local cache that avoids opening and closing IndexedDB connections for each request.
    ///
    /// As the [`IdbDatabase`] is not [`Send`] we cannot keep the database as part of the
    /// [`IndexedDbObjectStore`] struct.
    static IDB_CACHE: RefCell<HashMap<String, IdbDatabase>> = RefCell::new(HashMap::new());
}

/// The name of the blob store in IndexedDB.
const BLOB_STORE: &str = "blobs";

/// The name of the metadata store in IndexedDB.
const METADATA_STORE: &str = "metadata";

/// An object store that stores data in an IndexedDB database.
#[derive(Clone, Debug)]
pub struct IndexedDbObjectStore {
    /// The name of the IndexedDB database.
    db_name: String,
}

/// Metadata about a blob stored in IndexedDB.
struct BlobMetadata {
    /// The last modified time of the blob.
    last_modified: DateTime<chrono::Utc>,
    /// The size of the blob in bytes.
    size: u64,
}

impl From<BlobMetadata> for js_sys::Object {
    fn from(value: BlobMetadata) -> Self {
        let obj = js_sys::Object::new();
        js_sys::Reflect::set(
            &obj,
            &JsValue::from_str("lastModified"),
            &JsValue::from_f64(value.last_modified.timestamp_millis() as f64),
        )
        .unwrap();
        js_sys::Reflect::set(
            &obj,
            &JsValue::from_str("size"),
            &JsValue::from_f64(value.size as f64),
        )
        .unwrap();
        obj
    }
}

#[derive(Debug, thiserror::Error)]
#[error("Error parsing metadata from IndexedDB: {0}")]
enum BlobMetadataParsingError {
    #[error("Value was null, undefined, or not an object")]
    NotAnObject,
    #[error("Missing property '{0}'.")]
    MissingProperty(String),
    #[error("Invalid property '{0}'.")]
    InvalidProperty(String),
}

impl TryFrom<JsValue> for BlobMetadata {
    type Error = BlobMetadataParsingError;

    fn try_from(value: JsValue) -> Result<Self, Self::Error> {
        if value.is_null() || value.is_undefined() || !value.is_object() {
            return Err(BlobMetadataParsingError::NotAnObject);
        }

        let size = js_sys::Reflect::get(&value, &JsValue::from_str("size"))
            .map_err(|_| BlobMetadataParsingError::MissingProperty("size".to_string()))?
            .as_f64()
            .ok_or_else(|| {
                BlobMetadataParsingError::InvalidProperty("size".to_string())
            })?;
        let last_modified_millis =
            js_sys::Reflect::get(&value, &JsValue::from_str("lastModified"))
                .map_err(|_| {
                    BlobMetadataParsingError::MissingProperty("lastModified".to_string())
                })?
                .as_f64()
                .ok_or_else(|| {
                    BlobMetadataParsingError::InvalidProperty("lastModified".to_string())
                })?;

        let last_modified = DateTime::from_timestamp_millis(last_modified_millis as i64)
            .unwrap_or_else(chrono::Utc::now);

        Ok(BlobMetadata {
            size: size as u64,
            last_modified,
        })
    }
}

impl IndexedDbObjectStore {
    /// Creates a new [`IndexedDbObjectStore`] with the given name.
    pub fn new(db_name: String) -> Self {
        Self { db_name }
    }
}

impl std::fmt::Display for IndexedDbObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "IndexedDbObjectStore({})", self.db_name)
    }
}

#[async_trait]
impl ObjectStore for IndexedDbObjectStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> OsResult<PutResult> {
        if opts.mode != PutMode::Overwrite
            || !opts.tags.encoded().is_empty()
            || !opts.attributes.is_empty()
        {
            return Err(object_store::Error::NotImplemented {
                implementer: "IndexedDbObjectStore".to_string(),
                operation: "put_opts (with tags, attributes, or non-overwrite mode)"
                    .to_string(),
            });
        }

        let db_name = self.db_name.clone();
        let location_str = location.as_ref().to_string();
        let txn_time = js_sys::Date::now();

        let chunks: Vec<Vec<u8>> = payload.into_iter().map(|b| b.to_vec()).collect();
        let (tx, rx) = oneshot::channel();

        spawn_local(async move {
            let res = idb_put_chunks(&db_name, &location_str, chunks, txn_time).await;
            let _ = tx.send(res.map(|_| PutResult {
                e_tag: None,
                version: None,
            }));
        });

        rx.await
            .map_err(|_| map_js_err("Channel closed unexpectedly"))?
    }

    async fn put_multipart_opts(
        &self,
        _location: &Path,
        _opts: PutMultipartOptions,
    ) -> OsResult<Box<dyn MultipartUpload>> {
        Err(object_store::Error::NotImplemented {
            implementer: "IndexedDbObjectStore".to_string(),
            operation: "put_multipart_opts".to_string(),
        })
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> OsResult<GetResult> {
        if options.if_match.is_some()
            || options.if_none_match.is_some()
            || options.if_modified_since.is_some()
            || options.if_unmodified_since.is_some()
            || options.version.is_some()
        {
            return Err(object_store::Error::NotImplemented {
                implementer: "IndexedDbObjectStore".to_string(),
                operation: "get_opts (with conditions or versioning)".to_string(),
            });
        }

        let db_name = self.db_name.clone();
        let location_str = location.as_ref().to_string();
        let location_path = location.clone();

        let (tx, rx) = oneshot::channel::<OsResult<(Bytes, Range<u64>, BlobMetadata)>>();

        spawn_local(async move {
            let get_blob = async {
                let (blob_metadata, blob) =
                    load_blob_and_metadata(&db_name, &location_str).await?;

                if options.head {
                    return Ok((Bytes::new(), 0..blob_metadata.size, blob_metadata));
                }

                let range = match options.range {
                    Some(r) => r.as_range(blob_metadata.size).map_err(|e| {
                        object_store::Error::Generic {
                            store: "IndexedDB",
                            source: Box::new(std::io::Error::other(format!("{:?}", e))),
                        }
                    })?,
                    None => 0..blob_metadata.size,
                };

                let sliced_blob = blob
                    .slice_with_f64_and_f64(range.start as f64, range.end as f64)
                    .map_err(map_js_err)?;
                let buffer_val = JsFuture::from(sliced_blob.array_buffer())
                    .await
                    .map_err(map_js_err)?;

                let uint8_array = js_sys::Uint8Array::new(&buffer_val);
                let mut vec = vec![0; uint8_array.length() as usize];
                uint8_array.copy_to(&mut vec);

                Ok((Bytes::from(vec), range, blob_metadata))
            };

            let _ = tx.send(get_blob.await);
        });

        let (bytes, range, blob_metadata) =
            rx.await.map_err(|_| map_js_err("Channel closed"))??;

        Ok(GetResult {
            payload: GetResultPayload::Stream(Box::pin(futures::stream::once(
                async move { Ok(bytes) },
            ))),
            meta: ObjectMeta {
                location: location_path,
                last_modified: blob_metadata.last_modified,
                size: blob_metadata.size,
                e_tag: None,
                version: None,
            },
            range,
            attributes: Default::default(),
        })
    }

    fn delete_stream(
        &self,
        _locations: BoxStream<'static, OsResult<Path>>,
    ) -> BoxStream<'static, OsResult<Path>> {
        let stream = async_stream::stream! {
            yield Err(object_store::Error::NotImplemented {
                implementer: "IndexedDbObjectStore".to_string(),
                operation: "delete_stream".to_string()
            })
        };
        Box::pin(stream)
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let db_name = self.db_name.clone();
        let prefix_str = prefix.map(|p| p.as_ref().to_string());
        let stream = async_stream::stream! {
            let entries = idb_list_metadata(db_name, prefix_str).await?;
            for entry in entries {
                yield Ok(entry);
            }
        };
        Box::pin(stream)
    }

    fn list_with_offset(
        &self,
        prefix: Option<&Path>,
        offset: &Path,
    ) -> BoxStream<'static, OsResult<ObjectMeta>> {
        let db_name = self.db_name.clone();
        let prefix_str = prefix.map(|p| p.as_ref().to_string());
        let offset_str = offset.as_ref().to_string();
        let stream = async_stream::stream! {
            let entries = idb_list_metadata(db_name, prefix_str).await?;
            for entry in entries {
                if entry.location.as_ref() > offset_str.as_str() {
                    yield Ok(entry);
                }
            }
        };
        Box::pin(stream)
    }

    async fn list_with_delimiter(&self, _prefix: Option<&Path>) -> OsResult<ListResult> {
        Err(object_store::Error::NotImplemented {
            implementer: "IndexedDbObjectStore".to_string(),
            operation: "list_with_delimiter".to_string(),
        })
    }

    async fn copy_opts(
        &self,
        _from: &Path,
        _to: &Path,
        _options: CopyOptions,
    ) -> OsResult<()> {
        Err(object_store::Error::NotImplemented {
            implementer: "IndexedDbObjectStore".to_string(),
            operation: "copy_opts".to_string(),
        })
    }
}

async fn get_or_open_db(db_name: &str) -> Result<IdbDatabase, object_store::Error> {
    if let Some(db) = IDB_CACHE.with(|cache| cache.borrow().get(db_name).cloned()) {
        return Ok(db);
    }

    let global = js_sys::global();
    let idb_factory: IdbFactory =
        js_sys::Reflect::get(&global, &JsValue::from_str("indexedDB"))
            .map_err(map_js_err)?
            .dyn_into()
            .map_err(|_| map_js_err("IndexedDB is not supported in this environment"))?;

    let open_req: IdbOpenDbRequest = idb_factory.open(db_name).map_err(map_js_err)?;

    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let on_success = Closure::once(move |event: web_sys::Event| {
            let target = event.target().unwrap();
            let req: IdbRequest = target.unchecked_into();
            resolve
                .call1(&JsValue::NULL, &req.result().unwrap_or(JsValue::NULL))
                .unwrap();
        });

        let on_error = Closure::once(move |event: web_sys::Event| {
            reject.call1(&JsValue::NULL, &event).unwrap();
        });

        let on_upgrade = Closure::once(move |event: web_sys::Event| {
            let target = event.target().unwrap();
            let req: IdbOpenDbRequest = target.unchecked_into();
            let db: IdbDatabase = req.result().unwrap().unchecked_into();

            let stores = db.object_store_names();
            if !stores.contains(BLOB_STORE) {
                db.create_object_store(BLOB_STORE).unwrap();
            }
            if !stores.contains(METADATA_STORE) {
                db.create_object_store(METADATA_STORE).unwrap();
            }
        });

        open_req.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
        open_req.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        open_req.set_onupgradeneeded(Some(on_upgrade.as_ref().unchecked_ref()));

        on_success.forget();
        on_error.forget();
        on_upgrade.forget();
    });

    let db_val = JsFuture::from(promise).await.map_err(map_js_err)?;
    let db: IdbDatabase = db_val.unchecked_into();

    IDB_CACHE.with(|cache| {
        cache.borrow_mut().insert(db_name.to_string(), db.clone());
    });

    Ok(db)
}

fn idb_request_to_future(req: IdbRequest) -> JsFuture {
    let promise = js_sys::Promise::new(&mut |resolve, reject| {
        let on_success = Closure::once(move |event: web_sys::Event| {
            let target = event.target().unwrap();
            let req: IdbRequest = target.unchecked_into();
            resolve
                .call1(&JsValue::NULL, &req.result().unwrap_or(JsValue::NULL))
                .unwrap();
        });
        let on_error = Closure::once(move |event: web_sys::Event| {
            reject.call1(&JsValue::NULL, &event).unwrap();
        });
        req.set_onsuccess(Some(on_success.as_ref().unchecked_ref()));
        req.set_onerror(Some(on_error.as_ref().unchecked_ref()));
        on_success.forget();
        on_error.forget();
    });
    JsFuture::from(promise)
}

fn map_js_err(e: impl std::fmt::Debug) -> object_store::Error {
    generic_error(format!("{:?}", e))
}

#[derive(Debug, Clone, thiserror::Error)]
#[error("Error accessing IndexedDB: {0}")]
struct IndexedDbObjectStoreGenericError(String);

fn generic_error(error: String) -> object_store::Error {
    object_store::Error::Generic {
        store: "IndexedDbObjectStore",
        source: Box::new(IndexedDbObjectStoreGenericError(error)),
    }
}

async fn idb_put_chunks(
    db_name: &str,
    location_str: &str,
    chunks: Vec<Vec<u8>>,
    txn_time: f64,
) -> Result<(), object_store::Error> {
    let db = get_or_open_db(db_name).await?;

    let js_chunks = js_sys::Array::new();
    for chunk in chunks {
        let array = js_sys::Uint8Array::new_with_length(chunk.len() as u32);
        array.copy_from(&chunk);
        js_chunks.push(&array);
    }

    let bag = BlobPropertyBag::new();
    bag.set_type("application/octet-stream");
    let blob = Blob::new_with_u8_array_sequence_and_options(&js_chunks, &bag)
        .map_err(map_js_err)?;

    let metadata = js_sys::Object::new();
    js_sys::Reflect::set(
        &metadata,
        &JsValue::from_str("lastModified"),
        &JsValue::from_f64(txn_time),
    )
    .unwrap();
    js_sys::Reflect::set(
        &metadata,
        &JsValue::from_str("size"),
        &JsValue::from_f64(blob.size()),
    )
    .unwrap();

    let stores = js_sys::Array::new();
    stores.push(&JsValue::from_str(BLOB_STORE));
    stores.push(&JsValue::from_str(METADATA_STORE));

    let idb_tx = db
        .transaction_with_str_sequence_and_mode(&stores, IdbTransactionMode::Readwrite)
        .map_err(map_js_err)?;

    let blob_store = idb_tx.object_store(BLOB_STORE).map_err(map_js_err)?;
    let meta_store = idb_tx.object_store(METADATA_STORE).map_err(map_js_err)?;

    let location_key = JsValue::from_str(location_str);
    let blob_req = blob_store
        .put_with_key(&blob, &location_key)
        .map_err(map_js_err)?;
    let meta_req = meta_store
        .put_with_key(&metadata, &location_key)
        .map_err(map_js_err)?;

    // Await both requests to ensure completion
    idb_request_to_future(blob_req).await.map_err(map_js_err)?;
    idb_request_to_future(meta_req).await.map_err(map_js_err)?;

    Ok(())
}

/// Loads the blob and the metadata for a given key, returning an error if the objects cannot be
/// found.
async fn load_blob_and_metadata(
    db_name: &str,
    location_str: &str,
) -> OsResult<(BlobMetadata, Blob)> {
    let db = get_or_open_db(db_name).await?;

    let stores = js_sys::Array::new();
    stores.push(&JsValue::from_str(BLOB_STORE));
    stores.push(&JsValue::from_str(METADATA_STORE));

    let idb_tx = db
        .transaction_with_str_sequence_and_mode(&stores, IdbTransactionMode::Readonly)
        .map_err(map_js_err)?;

    let meta_store = idb_tx.object_store(METADATA_STORE).map_err(map_js_err)?;
    let meta_req = meta_store
        .get(&JsValue::from_str(location_str))
        .map_err(map_js_err)?;

    let metadata_val = idb_request_to_future(meta_req).await.map_err(map_js_err)?;
    if metadata_val.is_null() || metadata_val.is_undefined() {
        return Err(object_store::Error::NotFound {
            path: location_str.to_string(),
            source: "Not found in IndexedDB metadata".into(),
        });
    }

    let metadata = BlobMetadata::try_from(metadata_val)
        .map_err(|_| generic_error("Failed to parse metadata".to_string()))?;

    let blob_store = idb_tx.object_store(BLOB_STORE).map_err(map_js_err)?;
    let blob_req = blob_store
        .get(&JsValue::from_str(location_str))
        .map_err(map_js_err)?;

    let blob_val = idb_request_to_future(blob_req).await.map_err(map_js_err)?;
    if blob_val.is_null() || blob_val.is_undefined() {
        return Err(object_store::Error::NotFound {
            path: location_str.to_string(),
            source: "Not found in IndexedDB blobs".into(),
        });
    }

    let blob: Blob = if blob_val.is_instance_of::<Blob>() {
        blob_val.unchecked_into()
    } else {
        return Err(object_store::Error::Generic {
            store: "IndexedDB",
            source: Box::new(std::io::Error::other("Stored data is not a Blob")),
        });
    };

    if metadata.size != blob.size() as u64 {
        return Err(object_store::Error::Generic {
            store: "IndexedDB",
            source: Box::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Blob size does not match metadata",
            )),
        });
    }

    Ok((metadata, blob))
}

async fn idb_list_metadata(
    db_name: String,
    prefix: Option<String>,
) -> OsResult<Vec<ObjectMeta>> {
    let (tx, rx) = oneshot::channel();

    spawn_local(async move {
        let res = idb_list_metadata_local(&db_name, prefix).await;
        let _ = tx.send(res);
    });

    rx.await.map_err(|_| map_js_err("Channel closed"))?
}

async fn idb_list_metadata_local(
    db_name: &str,
    prefix: Option<String>,
) -> OsResult<Vec<ObjectMeta>> {
    let db = get_or_open_db(db_name).await?;

    let stores = js_sys::Array::new();
    stores.push(&JsValue::from_str(METADATA_STORE));

    let idb_tx = db
        .transaction_with_str_sequence_and_mode(&stores, IdbTransactionMode::Readonly)
        .map_err(map_js_err)?;

    let meta_store = idb_tx.object_store(METADATA_STORE).map_err(map_js_err)?;
    let keys_req = meta_store.get_all_keys().map_err(map_js_err)?;
    let values_req = meta_store.get_all().map_err(map_js_err)?;

    let keys_val = idb_request_to_future(keys_req).await.map_err(map_js_err)?;
    let values_val = idb_request_to_future(values_req)
        .await
        .map_err(map_js_err)?;

    let keys_arr: js_sys::Array = keys_val.unchecked_into();
    let values_arr: js_sys::Array = values_val.unchecked_into();

    let mut results = Vec::new();
    let prefix_str = prefix.unwrap_or_default();

    for i in 0..keys_arr.length() {
        let key_val = keys_arr.get(i);
        let val_val = values_arr.get(i);

        if let Some(key_str) = key_val.as_string() {
            if key_str.starts_with(&prefix_str) {
                if let Ok(metadata) = BlobMetadata::try_from(val_val) {
                    let location = Path::parse(&key_str).map_err(|e| {
                        object_store::Error::Generic {
                            store: "IndexedDB",
                            source: Box::new(e),
                        }
                    })?;
                    results.push(ObjectMeta {
                        location,
                        last_modified: metadata.last_modified,
                        size: metadata.size,
                        e_tag: None,
                        version: None,
                    });
                }
            }
        }
    }

    Ok(results)
}
