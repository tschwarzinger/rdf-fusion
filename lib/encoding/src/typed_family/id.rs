use std::fmt::{Display, Formatter};

/// An enum representing the built-in and extension type families.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TypedFamilyId {
    /// The null family.
    Null,
    /// The resource family.
    Resource,
    /// The string family.
    String,
    /// The boolean family.
    Boolean,
    /// The numeric family.
    Numeric,
    /// The date-time family.
    DateTime,
    /// The duration family.
    Duration,
    /// The unknown family.
    Unknown,
    /// An extension family.
    Extension(&'static str),
}

impl TypedFamilyId {
    /// Returns the string identifier of the family.
    pub fn as_str(&self) -> &str {
        match self {
            TypedFamilyId::Null => "rdf-fusion.null",
            TypedFamilyId::Resource => "rdf-fusion.resources",
            TypedFamilyId::String => "rdf-fusion.strings",
            TypedFamilyId::Boolean => "rdf-fusion.boolean",
            TypedFamilyId::Numeric => "rdf-fusion.numeric",
            TypedFamilyId::DateTime => "rdf-fusion.date-time",
            TypedFamilyId::Duration => "rdf-fusion.duration",
            TypedFamilyId::Unknown => "rdf-fusion.unknown",
            TypedFamilyId::Extension(id) => id,
        }
    }

    /// Creates a new extension ID from a static string.
    pub const fn extension(id: &'static str) -> Self {
        Self::Extension(id)
    }
}

impl Display for TypedFamilyId {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
