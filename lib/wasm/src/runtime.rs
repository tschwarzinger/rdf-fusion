//! Cooperative Tokio runtime bridging for DataFusion on WebAssembly.
//!
//! DataFusion's operators (e.g. `UnionExec`) call [`tokio::task::spawn`] /
//! [`tokio::task::JoinSet`] during execution, which require an active Tokio
//! runtime context (`Handle::current()`). In the browser that is not present:
//! the `#[wasm_bindgen]` async bindings are driven by `wasm_bindgen_futures`
//! (the JavaScript microtask queue), which is not a Tokio runtime — so
//! `tokio::spawn` panics with "there is no reactor running".
//!
//! This module works around that by running the DataFusion future as a task on
//! a single-threaded (`current_thread`) Tokio runtime and *cooperatively
//! driving* that runtime: it advances the Tokio scheduler one turn, then yields
//! control back to the JavaScript event loop so browser-backed I/O (IndexedDB,
//! HTTP fetch) can make progress. `Runtime::block_on` alone would deadlock here
//! because the object stores resolve via JavaScript promises that require the
//! event loop to keep spinning.
//!
//! The driver is environment-agnostic with respect to the JavaScript global: it
//! schedules the wake-up macrotask on either `Window` (main thread) or
//! `WorkerGlobalScope` (web worker), so the same engine can be driven from a
//! dedicated worker to keep the UI thread responsive.
//!
//! A running query can be cancelled from JavaScript via
//! [`cancel_current_query`], which aborts the in-flight Tokio task (used by the
//! playground to turn "Run" into "Cancel").

use std::future::Future;
use std::sync::Mutex;
use std::sync::OnceLock;

use js_sys::Promise;
use tokio::runtime::Runtime;
use tokio::task::{AbortHandle, JoinHandle};
use wasm_bindgen::JsCast;
use wasm_bindgen::JsValue;
use wasm_bindgen::prelude::Closure;
use wasm_bindgen::prelude::wasm_bindgen;
use wasm_bindgen_futures::JsFuture;

static RUNTIME: OnceLock<Runtime> = OnceLock::new();

/// The Tokio task currently being cooperatively driven by this module, if any.
/// Only one query is expected to run at a time (the playground runs a single
/// active query); this lets [`cancel_current_query`] abort it.
static CURRENT_TASK: Mutex<Option<AbortHandle>> = Mutex::new(None);

/// Returns the lazily-constructed single-threaded Tokio runtime used to run
/// DataFusion plans.
fn runtime() -> &'static Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("failed to build the Tokio current-thread runtime")
    })
}

/// Cancels the currently running query, if any. Safe to call at any time; does
/// nothing when no query is in flight.
#[wasm_bindgen]
pub fn cancel_current_query() {
    if let Some(handle) = CURRENT_TASK.lock().unwrap().take() {
        handle.abort();
    }
}

/// Runs `fut` on the Wasm-compatible Tokio runtime, driving it cooperatively
/// while `wasm_bindgen_futures` waits for completion.
pub(crate) async fn run<F, O>(fut: F) -> Result<O, JsValue>
where
    F: Future<Output = O> + Send + 'static,
    O: Send + 'static,
{
    let rt = runtime();
    let task = rt.handle().spawn(fut);
    *CURRENT_TASK.lock().unwrap() = Some(task.abort_handle());
    let result = drive(rt, task).await;
    *CURRENT_TASK.lock().unwrap() = None;
    result
}

/// Advances the single-threaded Tokio runtime, yielding to the browser's event
/// loop whenever no progress can be made synchronously.
async fn drive<O>(rt: &'static Runtime, task: JoinHandle<O>) -> Result<O, JsValue> {
    // If we are already inside a Tokio runtime context, an enclosing drive loop
    // is currently polling the scheduler. Calling `block_on` again would panic
    // with "Cannot start a runtime from within a runtime", so we simply wait for
    // our task (the enclosing driver keeps it running).
    if tokio::runtime::Handle::try_current().is_ok() {
        return finish(task).await;
    }

    loop {
        let finished = task.is_finished();
        if !finished {
            // Run one turn of the Tokio scheduler. `yield_now` returns only
            // after other ready tasks have had a chance to run.
            rt.block_on(async {
                tokio::task::yield_now().await;
            });
        }
        if !task.is_finished() {
            // The spawned task is blocked on I/O (IndexedDB, HTTP fetch) that
            // is resolved by the browser. Give the event loop a chance to run.
            yield_to_event_loop().await?;
        } else {
            break;
        }
    }

    finish(task).await
}

async fn finish<O>(task: JoinHandle<O>) -> Result<O, JsValue> {
    match task.await {
        Ok(output) => Ok(output),
        Err(err) if err.is_cancelled() => Err(JsValue::from_str("Query cancelled")),
        Err(err) => Err(JsValue::from_str(&format!("Tokio task failed: {err}"))),
    }
}

/// Yields control to the JavaScript event loop by scheduling a `setTimeout(0)`
/// (a macrotask), then waiting for it. Using a macrotask (rather than a
/// resolved promise microtask) ensures browser callbacks such as IndexedDB
/// request completions get a chance to run in between Tokio turns.
async fn yield_to_event_loop() -> Result<(), JsValue> {
    let promise = Promise::new(&mut |resolve, reject| {
        let mut resolve = Some(resolve);
        let cb = Closure::once(move || {
            if let Some(resolve) = resolve.take() {
                let _ = resolve.call0(&JsValue::UNDEFINED);
            }
        });
        let handler = cb.as_ref().unchecked_ref::<js_sys::Function>();
        let result = schedule_timeout(handler);
        cb.forget();
        if result.is_err() {
            let _ = reject.call0(&JsValue::UNDEFINED);
        }
    });
    JsFuture::from(promise).await?;
    Ok(())
}

/// Schedules `handler` to run after ~0ms on whichever global scope is active:
/// `Window` on the main thread, `WorkerGlobalScope` (e.g.
/// `DedicatedWorkerGlobalScope`) when running inside a web worker.
fn schedule_timeout(handler: &js_sys::Function) -> Result<(), JsValue> {
    let global = js_sys::global();

    if let Ok(window) = global.clone().dyn_into::<web_sys::Window>() {
        window
            .set_timeout_with_callback_and_timeout_and_arguments_0(handler, 0)
            .map(|_| ())
    } else if let Ok(worker) = global
        .clone()
        .dyn_into::<web_sys::DedicatedWorkerGlobalScope>()
    {
        worker
            .set_timeout_with_callback_and_timeout_and_arguments_0(handler, 0)
            .map(|_| ())
    } else if let Ok(worker) = global.dyn_into::<web_sys::WorkerGlobalScope>() {
        worker
            .set_timeout_with_callback_and_timeout_and_arguments_0(handler, 0)
            .map(|_| ())
    } else {
        Err(JsValue::from_str(
            "no Window or WorkerGlobalScope available",
        ))
    }
}
