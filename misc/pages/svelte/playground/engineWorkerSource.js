// Source code for the Web Worker that hosts the RDF Fusion Wasm engine.
//
// Embedded as a string and instantiated as a blob-URL module worker (works
// regardless of the site's base URL). All heavy SPARQL + parquet work runs on
// the worker's own thread + event loop; the main thread only sends high-level
// application messages and receives UI-ready results.
//
// Message protocol (application-specific, not a mirror of the Wasm API):
//   - init        { glueUrl, wasmUrl }                    load the engine
//   - setDataset  { dataset }                             load a dataset
//   - runQuery    { query }      -> { results, explanation }
//   - cancelQuery             (acknowledged; interrupts the in-flight query)
//
// Result codes (on { ok:false } replies and the fatal event):
//   - "query_error"  recoverable operation failure; the engine is still healthy
//   - "cancelled"    the in-flight query was aborted by cancelQuery
//   - "engine_fatal" the wasm module aborted (Rust panic); the engine is dead
//
// A Rust panic aborts the whole wasm instance (panic=abort). Rust records the
// real message on `globalThis.__rdfFusionLastPanic` before aborting; the worker
// reads that marker and reports `engine_fatal` so the app does not mistake a
// dead engine for a recoverable query error.
//
// NOTE: keep this free of backticks and "${" so it can be embedded in the
// template literal in engineWorkerSource.js.
export const ENGINE_WORKER_SOURCE = `
let wasm = null;
let activeStore = null;

// RPCs are handled one at a time so concurrent engine operations never overlap
// (each one drives the shared Tokio runtime). Cancellation is handled outside
// the queue (so it can interrupt a running query) and is acknowledged.
let queue = Promise.resolve();

function classify(msg) {
    if (/cancel/i.test(msg || '')) return 'cancelled';
    return 'query_error';
}

function fatal(message) {
    globalThis.__rdfFusionLastPanic = null;
    self.postMessage({ fatal: true, code: 'engine_fatal', error: message || 'The engine aborted unexpectedly.' });
}

// Anything that escapes as an uncaught error here means the wasm instance is
// broken (a trap that did not go through a Rust panic hook) -> fatal.
self.addEventListener('error', function (ev) {
    fatal((ev && ev.message) || 'uncaught worker error');
});

// An unhandled promise rejection in the worker is fatal for the same reason.
self.addEventListener('unhandledrejection', function (ev) {
    fatal((ev && ev.reason && (ev.reason.message || ev.reason)) || 'uncaught promise rejection');
});

self.addEventListener('message', function (ev) {
    const data = ev.data || {};
    const id = data.id;
    const type = data.type;

    if (type === 'cancelQuery') {
        handle(type, data)
            .then(function () {
                self.postMessage({ id: id, ok: true, result: true });
            })
            .catch(function () {
                self.postMessage({ id: id, ok: true, result: true });
            });
        return;
    }

    queue = queue
        .then(function () {
            return handle(type, data);
        })
        .then(function (result) {
            self.postMessage({ id: id, ok: true, result: result });
        })
        .catch(function (err) {
            const msg = (err && err.message) || String(err);
            const panic = globalThis.__rdfFusionLastPanic;
            globalThis.__rdfFusionLastPanic = null;
            if (panic) {
                fatal(panic);
                return;
            }
            self.postMessage({ id: id, ok: false, error: msg, code: classify(msg) });
        });
});

async function handle(type, data) {
    if (type === 'init') {
        const glue = await import(data.glueUrl);
        if (glue.default) {
            await glue.default({ module_or_path: data.wasmUrl });
        }
        wasm = glue;
        return true;
    }
    if (type === 'setDataset') {
        const dataset = data.dataset || {};
        const context = await loadContext(dataset);
        activeStore = new wasm.JsStore(context);
        return true;
    }
    if (type === 'runQuery') {
        if (!activeStore) {
            throw new Error('No dataset loaded');
        }
        return await runQuery(activeStore, data.query);
    }
    if (type === 'ping') {
        return 'pong';
    }
    if (type === 'cancelQuery') {
        if (wasm && wasm.cancelCurrentQuery) {
            wasm.cancelCurrentQuery();
        }
        return true;
    }
    throw new Error('Unknown worker operation: ' + type);
}

async function loadContext(dataset) {
    const config = makeEngineConfig(dataset.settings);
    const encoding = wasm.JsQuadStorageEncoding[dataset.encoding];
    if (dataset.source === 'http' || dataset.url) {
        const createHttp = wasm.create_http_parquet_context || wasm.create_parquet_context_from_url;
        return await createHttp(dataset.url, encoding, config);
    }
    return await wasm.create_parquet_context_from_indexeddb(
        dataset.dbName,
        dataset.key,
        encoding,
        config
    );
}

// All result processing happens here. The query passes the result limit to the
// engine, which only materializes the first MAX_RESULTS_ROWS rows into JS
// objects but still evaluates the full query and reports the total row count.
//
// The reported elapsed time covers query evaluation + conversion of the (first
// 100) results into a JSON object inside this worker. It does NOT include
// copying the data to the UI (postMessage) or rendering, so the wall-clock time
// observed in the UI can be higher than the number reported here.
const MAX_RESULTS_ROWS = 100;

async function runQuery(store, query) {
    const t0 = performance.now();
    const res = await store.query_explain(query, MAX_RESULTS_ROWS);
    const exp = res.explanation;
    const results = res.results;
    const elapsedMs = performance.now() - t0;

    return {
        results,
        elapsedMs,
        explanation: {
            planning_latency_ms: exp.planning_latency_ms,
            planning_compute_ms: exp.planning_compute_ms,
            initial_logical_plan: exp.initial_logical_plan,
            optimized_logical_plan: exp.optimized_logical_plan,
            execution_plan: exp.execution_plan
        }
    };
}

function makeEngineConfig(settings) {
    settings = settings || {};
    const metrics = settings.metrics || {};
    const explain = new wasm.JsExplainConfig(
        metrics.showMetrics !== false,
        metrics.showStatistics === true
    );
    return new wasm.JsEngineConfig(
        settings.memoryLimitMb != null ? settings.memoryLimitMb : 1024,
        explain,
        settings.customConfig || {}
    );
}
`;
