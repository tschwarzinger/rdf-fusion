// Main-thread proxy for the RDF Fusion engine running in a Web Worker (see
// engineWorkerSource.js). Exposes an application-level API (init / setDataset /
// runQuery / cancelQuery) rather than mirroring the Wasm module surface; all
// query processing happens inside the worker.
//
// The proxy does NOT auto-restart a crashed/hung worker: when the worker dies it
// notifies the app (via `onCrash`) so the UI resets and the user can restart the
// engine manually. Before a query it pings the worker so a corpse doesn't hang
// the request — a dead worker makes the query fail fast instead.
//
// Error model (see engineWorkerSource.js): the worker classifies each failure
// with a `code`. Recoverable ops ("query_error", "cancelled") are rejected on
// the live worker; a fatal abort ("engine_fatal") is reported via a `fatal`
// message and takes the whole worker down (markDead -> onCrash). The proxy no
// longer guesses the cause from the message text.
import { ENGINE_WORKER_SOURCE } from './engineWorkerSource.js';

const DEAD_MESSAGE = "The engine has crashed and is no longer available. Please re-select the engine and dataset to restart it.";
const HEARTBEAT_TIMEOUT_MS = 2000;

export function createEngineProxy({ onCrash = null } = {}) {
    let worker = null;
    let nextId = 0;
    const pending = new Map();
    let dead = true;

    let resolveReady;
    let ready;

    // Resolves once the most recent cancellation has been acknowledged by the
    // worker, so a query run right after a cancel is sequenced after the
    // in-flight query is torn down (avoids posting onto a mid-cancel module).
    let cancelSettled = Promise.resolve();

    function spawnWorker() {
        worker = new Worker(
            URL.createObjectURL(new Blob([ENGINE_WORKER_SOURCE], { type: 'text/javascript' })),
            { type: 'module' }
        );
        dead = false;
        let r;
        ready = new Promise(res => { r = res; });
        resolveReady = r;

        worker.onmessage = (ev) => {
            const data = ev.data || {};
            if (data.fatal) { markDead(data.error || DEAD_MESSAGE); return; }
            if (data.id == null) return;
            const handler = pending.get(data.id);
            if (!handler) return;
            pending.delete(data.id);
            if (data.ok) {
                handler.resolve(data.result);
            } else {
                const err = new Error(data.error || 'Unknown worker error');
                err.code = data.code || 'query_error';
                handler.reject(err);
            }
        };

        worker.onerror = (ev) => {
            markDead((ev && ev.message) || 'unexpected worker error');
        };
    }

    function markDead(message) {
        if (dead) return;
        dead = true;
        cancelSettled = Promise.resolve();
        for (const [, handler] of pending.entries()) {
            handler.reject(new Error(message));
        }
        pending.clear();
        if (worker) worker.terminate();
        worker = null;
        if (onCrash) onCrash();
    }

    function post(id, type, payload) {
        if (worker) worker.postMessage({ id, type, ...(payload || {}) });
    }

    function rpcUnready(type, payload) {
        if (dead) return Promise.reject(new Error(DEAD_MESSAGE));
        return new Promise((resolve, reject) => {
            const id = ++nextId;
            pending.set(id, { resolve, reject, op: type });
            post(id, type, payload);
        });
    }

    function rpc(type, payload) {
        if (dead) return Promise.reject(new Error(DEAD_MESSAGE));
        return ready.then(() => {
            if (dead) throw new Error(DEAD_MESSAGE);
            return new Promise((resolve, reject) => {
                const id = ++nextId;
                pending.set(id, { resolve, reject, op: type });
                post(id, type, payload);
            });
        });
    }

    // Pings the worker; resolves true if it answers within the timeout.
    function ping(timeoutMs = HEARTBEAT_TIMEOUT_MS) {
        if (dead || !worker) return Promise.resolve(false);
        const id = ++nextId;
        return new Promise((resolve) => {
            const timer = setTimeout(() => {
                pending.delete(id);
                resolve(false);
            }, timeoutMs);
            pending.set(id, {
                resolve: (value) => { clearTimeout(timer); resolve(value === 'pong'); },
                reject: () => { clearTimeout(timer); resolve(false); },
                op: 'ping'
            });
            post(id, 'ping');
        });
    }

    async function init(glueUrl, wasmUrl) {
        spawnWorker();
        try {
            await rpcUnready('init', { glueUrl, wasmUrl });
            resolveReady();
        } catch (e) {
            markDead(e && e.message ? e.message : String(e));
            throw e;
        }
    }

    function setDataset(dataset) {
        return rpc('setDataset', { dataset });
    }

    // Returns true when a query can run. A dead/hung worker returns false so
    // the query fails fast (the app resets and the user restarts) rather than
    // hanging on a corpse.
    async function ensureHealthy() {
        if (dead) return false;
        const ok = await ping();
        if (ok) return true;
        markDead(DEAD_MESSAGE);
        return false;
    }

    async function runQuery(query) {
        // Do not post onto a worker that may still be tearing down a cancelled
        // query; wait for the cancellation to be acknowledged first.
        await cancelSettled;
        if (dead) throw new Error(DEAD_MESSAGE);
        const healthy = await ensureHealthy();
        if (!healthy || dead) throw new Error(DEAD_MESSAGE);
        return rpc('runQuery', { query });
    }

    // Acknowledged cancellation. Resolves once the worker has applied the abort,
    // so a follow-up query can be sequenced after the in-flight one is torn down.
    function cancelQuery() {
        if (dead) return Promise.resolve();
        const p = rpcUnready('cancelQuery').catch(() => {});
        cancelSettled = p;
        return p;
    }

    return {
        init,
        setDataset,
        runQuery,
        cancelQuery,
        terminate: () => worker && worker.terminate()
    };
}
