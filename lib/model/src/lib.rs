#![doc(test(attr(deny(warnings))))]
#![doc(
    html_favicon_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]
#![doc(
    html_logo_url = "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/misc/logo/logo.png"
)]

//! This crate contains the RDF Fusion model, including types for RDF terms, quads, and RDF graphs.
//! Note that the data representation based on Arrow arrays is *not* part of this crate.
//!
//! Large portions of the available types are re-exported from [Oxigraph](https://github.com/oxigraph/oxigraph).

mod blank_node_mode;
mod error;
pub mod quads;
mod rdf;
mod typed_value;
pub mod vocab;
mod xsd;

pub use blank_node_mode::BlankNodeMatchingMode;
pub use error::*;
pub use rdf::*;
pub use typed_value::*;
pub use xsd::*;

// Re-export some oxrdf types.
pub use oxiri::Iri;
pub use oxrdf::{
    BlankNode, BlankNodeRef, Dataset, Graph, GraphName, GraphNameRef, IriParseError,
    Literal, LiteralRef, NamedNode, NamedNodeRef, NamedOrBlankNode, NamedOrBlankNodeRef,
    Quad, QuadRef, Term, TermParseError, TermRef, Triple, TripleRef, Variable,
    VariableNameParseError, VariableRef, dataset,
};
pub use spargebra::algebra::PropertyPathExpression;
pub use spargebra::term::{GroundTerm, NamedNodePattern, TermPattern, TriplePattern};

use datafusion::arrow::error::ArrowError;

pub type AResult<T> = Result<T, ArrowError>;
pub type DFResult<T> = datafusion::error::Result<T>;
