#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo-thumbnail.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! A Delta Lake-based quad storage for [RDF Fusion](https://docs.rs/rdf-fusion/).

mod builder;
mod error;
pub mod exec;
pub mod local_object_ids;
mod log;
mod objectids;
mod planner;
mod quad_table;
mod refresh;
mod scan_plan_builder;
mod snapshot;
mod storage;
mod transaction;
mod writer;

pub use builder::{DeltaQuadsStorageBuilder, LoadMode};
pub use storage::DeltaQuadsStorage;
