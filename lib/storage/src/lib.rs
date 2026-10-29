#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo-thumbnail.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! Contains shared building blocks for the storage layer implementations of [RDF Fusion](https://docs.rs/rdf-fusion/).

pub mod block_cache;
pub mod quad_tables;
pub mod rdf_files;
