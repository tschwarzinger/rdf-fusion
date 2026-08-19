export const DB_NAME = 'rdf_fusion_playground';

const WASM_STORE = 'wasm_versions';
const STORE_DATASETS = 'datasets';
const STORE_BLOBS = 'blobs';
const STORE_METADATA = 'metadata';

const DB_VERSION = 10;

let openDbInstance = null;

export function closeDB() {
    if (openDbInstance) {
        try {
            openDbInstance.close();
        } catch (e) {
            console.warn("Error closing DB:", e);
        }
        openDbInstance = null;
    }
}

export function initDB() {
    if (openDbInstance) {
        return Promise.resolve(openDbInstance);
    }
    return new Promise((resolve, reject) => {
        const req = indexedDB.open(DB_NAME, DB_VERSION);
        req.onupgradeneeded = (e) => {
            const db = e.target.result;
            if (!db.objectStoreNames.contains(WASM_STORE)) {
                db.createObjectStore(WASM_STORE, {keyPath: 'id'});
            }
            if (!db.objectStoreNames.contains(STORE_DATASETS)) {
                db.createObjectStore(STORE_DATASETS, {keyPath: 'id'});
            }
            if (!db.objectStoreNames.contains(STORE_BLOBS)) {
                db.createObjectStore(STORE_BLOBS);
            }
            if (!db.objectStoreNames.contains(STORE_METADATA)) {
                db.createObjectStore(STORE_METADATA);
            }
            // Remove legacy separate stores if present
            if (db.objectStoreNames.contains('custom_datasets')) {
                db.deleteObjectStore('custom_datasets');
            }
            if (db.objectStoreNames.contains('downloaded_datasets')) {
                db.deleteObjectStore('downloaded_datasets');
            }
        };
        req.onsuccess = (e) => {
            openDbInstance = e.target.result;
            openDbInstance.onversionchange = () => {
                closeDB();
            };
            resolve(openDbInstance);
        };
        req.onerror = (e) => reject(e.target.error);
    });
}

// Object store blob operations (blobs & metadata stores)
export async function putBlob(key, blob) {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([STORE_BLOBS, STORE_METADATA], 'readwrite');
        tx.objectStore(STORE_BLOBS).put(blob, key);
        tx.objectStore(STORE_METADATA).put({
            size: blob.size,
            lastModified: Date.now()
        }, key);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}

export async function getBlob(key) {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([STORE_BLOBS], 'readonly');
        const req = tx.objectStore(STORE_BLOBS).get(key);
        req.onsuccess = () => resolve(req.result || null);
        req.onerror = () => reject(req.error);
    });
}

export async function deleteBlob(key) {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([STORE_BLOBS, STORE_METADATA], 'readwrite');
        tx.objectStore(STORE_BLOBS).delete(key);
        tx.objectStore(STORE_METADATA).delete(key);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}

// Unified Dataset store management (datasets store)
export async function saveDataset(dataset) {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([STORE_DATASETS, STORE_BLOBS, STORE_METADATA], 'readwrite');
        const { fileBlob, ...datasetRecord } = dataset;
        tx.objectStore(STORE_DATASETS).put({
            ...datasetRecord,
            is_custom: dataset.is_custom !== undefined ? dataset.is_custom : (dataset.isCustom ?? false),
            size: fileBlob ? fileBlob.size : (dataset.size || undefined),
            timestamp: dataset.timestamp || Date.now()
        });
        if (fileBlob) {
            tx.objectStore(STORE_BLOBS).put(fileBlob, dataset.id);
            tx.objectStore(STORE_METADATA).put({
                size: fileBlob.size,
                lastModified: Date.now()
            }, dataset.id);
        }
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}

export async function getDatasets() {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([STORE_DATASETS], 'readonly');
        const req = tx.objectStore(STORE_DATASETS).getAll();
        req.onsuccess = () => resolve(req.result || []);
        req.onerror = () => reject(req.error);
    });
}

export async function getDataset(id) {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([STORE_DATASETS], 'readonly');
        const req = tx.objectStore(STORE_DATASETS).get(id);
        req.onsuccess = () => resolve(req.result || null);
        req.onerror = () => reject(req.error);
    });
}

export async function deleteDataset(id) {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction([STORE_DATASETS, STORE_BLOBS, STORE_METADATA], 'readwrite');
        tx.objectStore(STORE_DATASETS).delete(id);
        tx.objectStore(STORE_BLOBS).delete(id);
        tx.objectStore(STORE_METADATA).delete(id);
        tx.oncomplete = () => resolve();
        tx.onerror = () => reject(tx.error);
    });
}

// Downloaded / Library Datasets helpers
export async function saveDownloadedDataset(id, fileBlob, name, originalUrl) {
    return saveDataset({
        id,
        name,
        is_custom: false,
        originalUrl,
        url: originalUrl,
        fileBlob,
        timestamp: Date.now()
    });
}

export async function getDownloadedDatasets() {
    const all = await getDatasets();
    return all.filter(d => !d.is_custom);
}

export async function getDownloadedDataset(id) {
    return getDataset(id);
}

export async function deleteDownloadedDataset(id) {
    return deleteDataset(id);
}

// Custom / User-Uploaded Dataset helpers
export async function saveCustomDataset(dataset) {
    return saveDataset({
        ...dataset,
        is_custom: true
    });
}

export async function getCustomDatasets() {
    const all = await getDatasets();
    return all.filter(d => d.is_custom);
}

export async function deleteCustomDataset(id) {
    return deleteDataset(id);
}

// Version management
export async function getLocalVersion(id) {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(WASM_STORE, 'readonly');
        const req = tx.objectStore(WASM_STORE).get(id);
        req.onsuccess = () => resolve(req.result || null);
        req.onerror = () => reject(req.error);
    });
}

export async function storeLocalVersion(id, jsBlob, wasmBlob, customName = null, etag = null) {
    const db = await initDB();
    const size = (jsBlob?.size || 0) + (wasmBlob?.size || 0);
    return new Promise((resolve, reject) => {
        const tx = db.transaction(WASM_STORE, 'readwrite');
        const req = tx.objectStore(WASM_STORE).put({
            id,
            jsBlob,
            wasmBlob,
            size,
            customName,
            etag,
            timestamp: Date.now()
        });
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
    });
}

export async function deleteLocalVersion(id) {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(WASM_STORE, 'readwrite');
        const req = tx.objectStore(WASM_STORE).delete(id);
        req.onsuccess = () => resolve();
        req.onerror = () => reject(req.error);
    });
}

export async function getAllLocalVersions() {
    const db = await initDB();
    return new Promise((resolve, reject) => {
        const tx = db.transaction(WASM_STORE, 'readonly');
        const req = tx.objectStore(WASM_STORE).getAll();
        req.onsuccess = () => resolve(req.result);
        req.onerror = () => reject(req.error);
    });
}

export async function purgeAllDatabaseAndStorage() {
    // 1. Close open DB connection so deletion is not blocked
    closeDB();

    // 2. Clear LocalStorage and SessionStorage
    try {
        localStorage.clear();
        sessionStorage.clear();
    } catch (e) {
        console.warn("Error clearing web storage:", e);
    }

    // 3. Clear all IndexedDB databases
    if (typeof indexedDB !== 'undefined') {
        try {
            if (indexedDB.databases) {
                const dbs = await indexedDB.databases();
                for (const dbInfo of dbs) {
                    if (dbInfo.name) {
                        try {
                            indexedDB.deleteDatabase(dbInfo.name);
                        } catch {
                            // ignore individual delete failure
                        }
                    }
                }
            }
        } catch (e) {
            console.warn("Error listing databases:", e);
        }

        // Explicitly delete primary DB with timeout protection
        await new Promise((resolve) => {
            const timeout = setTimeout(resolve, 500);
            try {
                const req = indexedDB.deleteDatabase(DB_NAME);
                req.onsuccess = () => { clearTimeout(timeout); resolve(); };
                req.onerror = () => { clearTimeout(timeout); resolve(); };
                req.onblocked = () => { clearTimeout(timeout); resolve(); };
            } catch {
                clearTimeout(timeout);
                resolve();
            }
        });
    }
}

