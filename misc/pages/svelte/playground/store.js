import { writable } from 'svelte/store';

export const jsStore = writable(null);
export const wasmModule = writable(null);
export const activeVersionMetadata = writable(null);
export const activeDatasetMetadata = writable(null);
export const downloadedDatasets = writable([]);
export const customDatasets = writable([]);
export const localVersions = writable([]);
export const isDatasetLoading = writable(false);
export const isEngineInitializing = writable(false); // true while auto-loading engine on startup
export const currentStatus = writable({ text: "Initializing playground...", icon: "fa-circle-notch fa-spin", color: "info" });
export const activeQueryText = writable(`SELECT *
WHERE {
  ?s ?p ?o .
}
LIMIT 10`);
export const globalAlert = writable(null); // { text, icon, color }
export const toasts = writable([]);
export const reloadStoreTrigger = writable(0);
export const expandedStatusSection = writable(null); // 'engine' | 'dataset' | null

export const queryResults = writable(emptyQueryResults());

export function emptyQueryResults() {
    return {
        data: null,
        results: null,
        logicalPlan: "",
        optimizedPlan: "",
        executionPlan: "",
        error: "",
        isExecuting: false,
        totalSeconds: null,
        planningLatencyMs: null,
        planningComputeMs: null
    };
}

export function clearQueryResults() {
    queryResults.set(emptyQueryResults());
}

const defaultEngineSettings = { 
    memoryLimit: 1024,
    metrics: {
        showMetrics: true,
        showStatistics: false
    },
    dataFusion: {
        enableDynamicFilterPushdown: true,
        targetPartitions: 1
    },
    rdfFusion: {
        enableDataCache: true,
        dataCacheBlockSizeKb: 2048,
        dataCacheNumBlocks: 1024
    },
    customConfig: [] // Array of { key: "", value: "" }
};

let storedSettings = null;
try {
    storedSettings = JSON.parse(localStorage.getItem('rdfFusionEngineSettings'));
} catch {
    // ignore parsing errors
}

const initialSettings = { ...defaultEngineSettings };
if (storedSettings) {
    if (storedSettings.memoryLimit !== undefined) initialSettings.memoryLimit = storedSettings.memoryLimit;
    if (storedSettings.metrics) initialSettings.metrics = { ...initialSettings.metrics, ...storedSettings.metrics };
    if (storedSettings.dataFusion) initialSettings.dataFusion = { ...initialSettings.dataFusion, ...storedSettings.dataFusion };
    if (storedSettings.rdfFusion) initialSettings.rdfFusion = { ...initialSettings.rdfFusion, ...storedSettings.rdfFusion };
    if (storedSettings.customConfig) initialSettings.customConfig = storedSettings.customConfig;
}

export const engineSettings = writable(initialSettings);

engineSettings.subscribe(val => {
    localStorage.setItem('rdfFusionEngineSettings', JSON.stringify(val));
});

export function setStatus(text, iconClass, colorClass) {
    currentStatus.set({ text, icon: iconClass, color: colorClass || 'info' });
    if (colorClass === 'danger' || colorClass === 'warning') {
        globalAlert.set({ text, icon: iconClass, color: colorClass });
    } else {
        globalAlert.set(null);
    }
}

export function showError(msg) {
    globalAlert.set({ text: 'Error occurred.', errorDetails: msg, icon: 'fa-triangle-exclamation', color: 'danger' });
}

export function clearAllState() {
    jsStore.set(null);
    activeDatasetMetadata.set(null);
    queryResults.set({
        data: null,
        results: null,
        logicalPlan: "",
        optimizedPlan: "",
        executionPlan: "",
        error: "",
        isExecuting: false,
        totalSeconds: null,
        planningLatencyMs: null,
        planningComputeMs: null
    });
    engineSettings.set({ ...defaultEngineSettings });
    globalAlert.set(null);
    localStorage.removeItem('rdfFusionEngineSettings');
    currentStatus.set({ text: "Playground state reset.", icon: "fa-rotate-left", color: "info" });
    reloadStoreTrigger.update(n => n + 1);
}
