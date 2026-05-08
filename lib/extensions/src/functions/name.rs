use crate::functions::BuiltinName;
use rdf_fusion_common::NamedNode;
use std::fmt::{Display, Formatter};

/// Identifier for a function. Either it is an RDF Fusion builtin or a custom function.
#[derive(Eq, PartialEq, Debug, Clone, Hash)]
pub enum FunctionName {
    /// An RDF Fusion builtin function.
    Builtin(BuiltinName),
    /// A custom function.
    Custom(NamedNode),
}

impl Display for FunctionName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FunctionName::Builtin(builtin) => builtin.fmt(f),
            FunctionName::Custom(name) => name.fmt(f),
        }
    }
}
