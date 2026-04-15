//! General-purpose types for quad indexes

use rdf_fusion_model::QuadComponent;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// Represents a list of *disjunct* index components.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexComponents([QuadComponent; 4]);

impl IndexComponents {
    /// Returns a reference to the inner array.
    pub fn inner(&self) -> &[QuadComponent; 4] {
        &self.0
    }
}

impl Display for IndexComponents {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for component in self.0.iter() {
            write!(f, "{component}")?;
        }
        Ok(())
    }
}

impl IndexComponents {
    /// A GSPO index.
    pub const GSPO: IndexComponents = IndexComponents([
        QuadComponent::GraphName,
        QuadComponent::Subject,
        QuadComponent::Predicate,
        QuadComponent::Object,
    ]);

    /// A GPOS index.
    pub const GPOS: IndexComponents = IndexComponents([
        QuadComponent::GraphName,
        QuadComponent::Predicate,
        QuadComponent::Object,
        QuadComponent::Subject,
    ]);

    /// A GPSO index.
    pub const GOSP: IndexComponents = IndexComponents([
        QuadComponent::GraphName,
        QuadComponent::Object,
        QuadComponent::Subject,
        QuadComponent::Predicate,
    ]);

    /// Tries to create a new [IndexConfiguration].
    ///
    /// Returns an error if an [QuadComponent] appears more than once.
    pub fn try_new(
        components: [QuadComponent; 4],
    ) -> Result<Self, IndexComponentsCreationError> {
        let distinct = components.iter().collect::<HashSet<_>>();
        if distinct.len() != components.len() {
            return Err(IndexComponentsCreationError);
        }

        Ok(IndexComponents(components))
    }
}

#[derive(Debug, Error)]
#[error("Duplicate indexed component given.")]
pub struct IndexComponentsCreationError;

#[cfg(test)]
mod tests {
    use crate::index::{IndexComponents, QuadComponent};

    #[test]
    fn index_configuration_accepts_unique_components() {
        let ok = IndexComponents::try_new([
            QuadComponent::GraphName,
            QuadComponent::Subject,
            QuadComponent::Predicate,
            QuadComponent::Object,
        ]);
        assert!(ok.is_ok());
    }

    #[test]
    fn index_configuration_rejects_duplicate_components() {
        let err = IndexComponents::try_new([
            QuadComponent::GraphName,
            QuadComponent::Subject,
            QuadComponent::Subject,
            QuadComponent::Object,
        ]);
        assert!(err.is_err());
    }
}
