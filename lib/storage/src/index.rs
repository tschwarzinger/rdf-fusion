//! General-purpose types for quad indexes

use rdf_fusion_common::QuadComponent;
use std::collections::HashSet;
use std::fmt::{Display, Formatter};
use thiserror::Error;

/// Represents a list of *disjunct* index components.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct IndexComponents([QuadComponent; 4]);

impl Display for IndexComponents {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for component in self.0.iter() {
            write!(f, "{component}")?;
        }
        Ok(())
    }
}

// Allows concisely generating all 24 constants
macro_rules! define_indexes {
    ($($name:ident => [$a:ident, $b:ident, $c:ident, $d:ident]),* $(,)?) => {
        $(
            #[doc = concat!("A ", stringify!($name), " index.")]
            pub const $name: IndexComponents = IndexComponents([
                QuadComponent::$a,
                QuadComponent::$b,
                QuadComponent::$c,
                QuadComponent::$d,
            ]);
        )*

        /// Returns a list of all 24 valid [`IndexComponents`] permutations.
        pub const fn list_all() -> &'static [IndexComponents; 24] {
            &[$(Self::$name),*]
        }
    };
}

impl IndexComponents {
    define_indexes! {
        // Subject First
        SPOG => [Subject, Predicate, Object, GraphName],
        SPGO => [Subject, Predicate, GraphName, Object],
        SOPG => [Subject, Object, Predicate, GraphName],
        SOGP => [Subject, Object, GraphName, Predicate],
        SGPO => [Subject, GraphName, Predicate, Object],
        SGOP => [Subject, GraphName, Object, Predicate],

        // Predicate First
        PSOG => [Predicate, Subject, Object, GraphName],
        PSGO => [Predicate, Subject, GraphName, Object],
        POSG => [Predicate, Object, Subject, GraphName],
        POGS => [Predicate, Object, GraphName, Subject],
        PGSO => [Predicate, GraphName, Subject, Object],
        PGOS => [Predicate, GraphName, Object, Subject],

        // Object First
        OSPG => [Object, Subject, Predicate, GraphName],
        OSGP => [Object, Subject, GraphName, Predicate],
        OPSG => [Object, Predicate, Subject, GraphName],
        OPGS => [Object, Predicate, GraphName, Subject],
        OGSP => [Object, GraphName, Subject, Predicate],
        OGPS => [Object, GraphName, Predicate, Subject],

        // GraphName First
        GSPO => [GraphName, Subject, Predicate, Object],
        GSOP => [GraphName, Subject, Object, Predicate],
        GPSO => [GraphName, Predicate, Subject, Object],
        GPOS => [GraphName, Predicate, Object, Subject],
        GOSP => [GraphName, Object, Subject, Predicate],
        GOPS => [GraphName, Object, Predicate, Subject],
    }

    /// Returns a reference to the inner array.
    pub const fn inner(&self) -> &[QuadComponent; 4] {
        &self.0
    }

    /// Tries to create a new [IndexComponents].
    ///
    /// Returns an error if a [QuadComponent] appears more than once.
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
