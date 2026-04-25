use std::fmt::{Display, Formatter};

/// Represents the name of a single [TermEncoding](crate::TermEncoding).
///
/// RDF Fusion allows users to define multiple encodings for RDF terms. This allows specializing the
/// Arrow arrays used for holding the results of queries.
///
/// # Order
///
/// The order defined over the [EncodingName] defines how much information they preserve.
/// - [Self::ObjectId] and [Self::PlainTerm] preserve the entire information.
/// - [Self::TypedFamily] preserves the value of the term, but not their lexical form.
/// - [Self::Sortable] can loose information (e.g., precision in numerics)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum EncodingName {
    /// Name of the [ObjectIdEncoding](crate::object_id::ObjectIdEncoding). Represents all terms,
    /// including literals, as a unique identifier.
    ObjectId,
    /// Name of the [PlainTermEncoding](crate::plain_term::PlainTermEncoding). Represents all terms,
    /// including literals, using their lexical value.
    PlainTerm,
    /// Name of the [TypedFamilyEncoding](crate::typed_family::TypedFamilyEncoding).
    ///
    /// Usually, represent literals in their respective value space and allows users to provide
    /// their own typed families.
    TypedFamily,
    /// Name of the [SortableTermEncoding](crate::sortable_term::SortableTermEncoding) which is used
    /// for sorting. We plan to remove this encoding in the future, once we can introduce custom
    /// orderings into the query engine.
    Sortable,
    /// Name of the [StringEncoding](crate::string::StringEncoding). Represents all terms using
    /// their Turtle string representation.
    String,
}

impl Display for EncodingName {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            EncodingName::ObjectId => write!(f, "Object ID"),
            EncodingName::PlainTerm => write!(f, "Plain Term"),
            EncodingName::TypedFamily => write!(f, "Typed Family"),
            EncodingName::Sortable => write!(f, "Sortable"),
            EncodingName::String => write!(f, "String"),
        }
    }
}
