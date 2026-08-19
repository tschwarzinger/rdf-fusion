import { getLocalVersion, storeLocalVersion } from './db.js';
import { createEngineProxy } from './engineProxy.js';
import { jsStore, wasmModule, activeVersionMetadata, reloadStoreTrigger, setStatus } from './store.js';
import { OFFICIAL_VERSIONS } from './data_engine_versions.js';

// engine.js is the single source of truth for activating engine versions.
export { OFFICIAL_VERSIONS };

let activeProxy = null;
let activeJsUrl = null;
let activeWasmUrl = null;

function revokeActiveUrls() {
    if (activeJsUrl) URL.revokeObjectURL(activeJsUrl);
    if (activeWasmUrl) URL.revokeObjectURL(activeWasmUrl);
    activeJsUrl = null;
    activeWasmUrl = null;
}

// Terminate any lingering engine worker when the page is torn down.
window.addEventListener('beforeunload', () => {
    if (activeProxy) {
        activeProxy.terminate();
        activeProxy = null;
    }
    revokeActiveUrls();
});

export async function ensureOfficialVersionDownloaded(versionId) {
    const existing = await getLocalVersion(versionId);
    if (existing) return existing;

    const vInfo = OFFICIAL_VERSIONS.find(v => v.id === versionId);
    if (!vInfo) {
        throw new Error(`Unknown official version: ${versionId}`);
    }

    const [jsRes, wasmRes] = await Promise.all([
        fetch(vInfo.jsUrl, { cache: 'no-cache' }),
        fetch(vInfo.wasmUrl, { cache: 'no-cache' })
    ]);
    if (!jsRes.ok || !wasmRes.ok) {
        throw new Error("Failed to fetch binary resources.");
    }

    const jsBlob = await jsRes.blob();
    const wasmBlob = await wasmRes.blob();
    const validJsBlob = new Blob([jsBlob], { type: 'application/javascript' });
    const etag = wasmRes.headers.get('ETag') || wasmRes.headers.get('etag') || jsRes.headers.get('ETag') || jsRes.headers.get('etag') || null;

    await storeLocalVersion(versionId, validJsBlob, wasmBlob, null, etag);
    return getLocalVersion(versionId);
}

export async function initializeWasm(localData) {
    if (activeProxy) {
        activeProxy.terminate();
        activeProxy = null;
    }
    revokeActiveUrls();

    const jsUrl = URL.createObjectURL(localData.jsBlob);
    const wasmUrl = URL.createObjectURL(localData.wasmBlob);

    const proxy = createEngineProxy({
        onCrash: () => {
            // The engine worker died. We do NOT auto-restart: reset the app so
            // the user can re-select the engine and dataset manually.
            activeProxy = null;
            wasmModule.set(null);
            jsStore.set(null);
            revokeActiveUrls();
            setStatus("The engine crashed. Please re-select the engine and dataset to restart it.", 'fa-triangle-exclamation', 'warning');
        }
    });
    await proxy.init(jsUrl, wasmUrl);
    activeProxy = proxy;
    activeJsUrl = jsUrl;
    activeWasmUrl = wasmUrl;
    if (proxy.runQuery) {
        wasmModule.set(proxy);
        jsStore.set(null);
        reloadStoreTrigger.update(n => n + 1);
    } else {
        wasmModule.set(null);
        jsStore.set(null);
    }
    return proxy;
}

export async function checkForRemoteUpdate(vInfo, localVer) {
    if (!vInfo || !localVer || vInfo.isCustom) return false;
    try {
        const res = await fetch(vInfo.wasmUrl, { method: 'HEAD', cache: 'no-cache' });
        const remoteEtag = res.headers.get('ETag') || res.headers.get('etag') || res.headers.get('Last-Modified');
        return !!(remoteEtag && (!localVer.etag || remoteEtag !== localVer.etag));
    } catch (e) {
        console.error("Failed to check for updates:", e);
        return false;
    }
}

// Activates an engine version: records it as selected, publishes its metadata,
// (re-)initializes the Wasm module, and reports whether a newer build is available.
// `localVersions` lets the caller resolve custom (uploaded) builds by id.
export async function activateVersion(versionId, { localVersion = null, localVersions = [] } = {}) {
    const vInfo = OFFICIAL_VERSIONS.find(v => v.id === versionId);
    localStorage.setItem('lastRdfFusionVersion', versionId);

    const localVer = localVersion ?? (await getLocalVersion(versionId));
    const size = localVer ? (localVer.size ?? (localVer.wasmBlob?.size ? localVer.wasmBlob.size + (localVer.jsBlob?.size || 0) : null)) : null;

    let metadata;
    if (vInfo) {
        metadata = { id: versionId, name: vInfo.name, isCustom: false, size, supportedStorage: vInfo.supportedStorage || [] };
    } else {
        const customVer = localVersions.find(v => v.id === versionId);
        metadata = { id: versionId, name: customVer?.customName || versionId, isCustom: true, size, supportedStorage: [] };
    }
    activeVersionMetadata.set(metadata);

    let hasUpdate = false;
    if (localVer) {
        await initializeWasm(localVer);
        if (vInfo) {
            hasUpdate = await checkForRemoteUpdate(vInfo, localVer);
        }
    }
    return { metadata, hasUpdate };
}
