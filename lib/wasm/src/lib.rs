//! This crate provides a JavaScript and WebAssembly interface to RDF Fusion.
//!
//! Currently, this is mostly used for powering the RDF Fusion playground. APIs for using RDF Fusion
//! in another application will likely be missing.

pub mod context;
pub mod indexeddb_store;
pub mod results;
pub mod runtime;
pub mod store;

use wasm_bindgen::prelude::*;

#[wasm_bindgen(start)]
pub fn init() {
    console_error_panic_hook::set_once();

    // On top of printing the panic to the console, record the real panic message
    // on the JavaScript global so the worker can distinguish a genuine module
    // abort (fatal) from a recoverable query error. A Rust panic aborts the whole
    // wasm instance (panic = abort), so it is never caught — this marker is the
    // only reliable signal that the engine actually died.
    let default_hook = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let message = info.to_string();
        let global = js_sys::global();
        let _ = js_sys::Reflect::set(
            &global,
            &JsValue::from_str("__rdfFusionLastPanic"),
            &JsValue::from_str(&message),
        );
        default_hook(info);
    }));
}
