#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! This crate defines the execution engine of RDF Fusion.
//!
//! # RDF Fusion Context
//!
//! Similar to DataFusion’s [`SessionContext`](datafusion::prelude::SessionContext),
//! RDF Fusion provides an [`RdfFusionContext`].
//! This context manages the state of the RDF Fusion engine, including the registered RDF term
//! encodings.
//! For more details, see the [`RdfFusionContext`] documentation.
//!
//! # Executing SPARQL Queries
//!
//! DataFusion operates on the concept of a *logical plan*.
//! While DataFusion includes an SQL front-end, it does not provide a SPARQL front-end.
//! To execute SPARQL queries, we convert them into logical plans through the following pipeline:
//!
//! ```text
//! Query String -> SPARQL Algebra (Oxigraph) -> Logical Plan
//! ```
//!
//! The query string is first parsed by Oxigraph’s SPARQL parser, which produces a SPARQL algebra
//! graph pattern. This graph pattern already includes high-level concepts such as `Join`, `Filter`,
//! and `Projection`.
//!
//! We then convert this algebra pattern into a DataFusion logical plan through a rewriting step.
//! The resulting logical plan is executed by DataFusion.

extern crate core;

mod builder;
mod engine;
pub mod input;
pub mod load;
mod planner;
pub mod results;
pub mod sparql;

pub use builder::RdfFusionContextBuilder;
pub use engine::RdfFusionContext;
