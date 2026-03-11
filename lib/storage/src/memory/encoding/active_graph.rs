use crate::memory::object_id::EncodedObjectId;

/// An encoded version of the active graph.
#[derive(Default, Debug, Clone, PartialEq, Eq, Hash)]
pub enum EncodedActiveGraph {
    #[default]
    DefaultGraph,
    AllGraphs,
    Union(Vec<EncodedObjectId>),
    AnyNamedGraph,
}
