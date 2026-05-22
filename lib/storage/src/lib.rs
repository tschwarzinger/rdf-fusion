#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! Contains storage layer implementations for [RDF Fusion](https://docs.rs/rdf-fusion/).

pub mod delta;
mod exec;
pub mod index;
pub mod parquet;
pub mod rdf_files;
