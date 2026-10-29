#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo-thumbnail.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! A Parquet-based quad storage for [RDF Fusion](https://docs.rs/rdf-fusion/).

mod bytes_estimator;
mod counting_store;
pub mod dataset;
mod loader;
mod parquet_sink;
mod reader;
mod scan;
mod scan_builder;
mod snapshot;
mod storage;
pub mod switch;
mod writer_properties;

pub use dataset::*;
pub use loader::*;
pub use parquet_sink::*;
pub use reader::*;
pub use scan_builder::*;
pub use snapshot::*;
pub use storage::*;
pub use writer_properties::*;
