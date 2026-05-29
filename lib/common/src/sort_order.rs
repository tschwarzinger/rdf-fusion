use crate::QuadComponent;
use std::collections::BTreeSet;
use std::fmt::{Display, Formatter};
use thiserror::Error;
/// The sort order for dumping a store.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RdfSortOrder {
    /// Standard lexicographical sort as defined by SPARQL.
    SparqlOrder(Vec<QuadComponent>),
    /// Use the native order (i.e., DataFusion's order) of the respective encoding.
    NativeOrder(Vec<QuadComponent>),
    /// Z-Order clustering.
    ZOrder(Vec<QuadComponent>),
}

/// A version of [`RdfSortOrder`] that reflects the name of the sort order (but currently also carries the components).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RdfSortOrderName {
    /// See [`RdfSortOrder::SparqlOrder`]
    SparqlOrder(Vec<QuadComponent>),
    /// See [`RdfSortOrder::NativeOrder`]
    NativeOrder(Vec<QuadComponent>),
    /// See [`RdfSortOrder::ZOrder`]
    ZOrder(Vec<QuadComponent>),
}

impl RdfSortOrderName {
    /// Returns the components of the sort order name.
    pub fn components(&self) -> &[QuadComponent] {
        match self {
            RdfSortOrderName::SparqlOrder(c) => c,
            RdfSortOrderName::NativeOrder(c) => c,
            RdfSortOrderName::ZOrder(c) => c,
        }
    }
}

#[derive(Debug, Error)]
#[error("Duplicate components in sort order: {0:?}")]
pub struct RdfSortOrderValidationError(Vec<QuadComponent>);

impl RdfSortOrder {
    /// Returns the name of the sort order.
    pub fn name(&self) -> RdfSortOrderName {
        match self {
            RdfSortOrder::SparqlOrder(c) => RdfSortOrderName::SparqlOrder(c.clone()),
            RdfSortOrder::NativeOrder(c) => RdfSortOrderName::NativeOrder(c.clone()),
            RdfSortOrder::ZOrder(c) => RdfSortOrderName::ZOrder(c.clone()),
        }
    }

    /// Validates that all components in the sort order are unique.
    pub fn validate(&self) -> Result<(), RdfSortOrderValidationError> {
        let components = match self {
            RdfSortOrder::SparqlOrder(c) => c,
            RdfSortOrder::NativeOrder(c) => c,
            RdfSortOrder::ZOrder(c) => c,
        };

        let mut seen = std::collections::HashSet::new();
        let mut duplicates = BTreeSet::new();
        for component in components {
            if !seen.insert(component) {
                duplicates.insert(component);
            }
        }

        if !duplicates.is_empty() {
            return Err(RdfSortOrderValidationError(
                duplicates.into_iter().cloned().collect(),
            ));
        }

        Ok(())
    }

    /// Returns the components of the sort order.
    pub fn components(&self) -> &[QuadComponent] {
        match self {
            RdfSortOrder::SparqlOrder(c) => c,
            RdfSortOrder::NativeOrder(c) => c,
            RdfSortOrder::ZOrder(c) => c,
        }
    }
}

impl Display for RdfSortOrder {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            RdfSortOrder::SparqlOrder(_) => {}
            RdfSortOrder::NativeOrder(_) => f.write_str("Native(")?,
            RdfSortOrder::ZOrder(_) => f.write_str("ZOrder(")?,
        };

        let components = self.components();
        for c in components {
            write!(f, "{c}")?;
        }

        if matches!(self, RdfSortOrder::NativeOrder(_) | RdfSortOrder::ZOrder(_)) {
            f.write_str(")")?;
        }

        Ok(())
    }
}

#[derive(Debug, Error)]
#[error("Error while parsing RDF sort order: {0}")]
pub enum RdfSortOrderParsingError {
    #[error("No RDF quad components were given.")]
    EmptyComponents,
    #[error("An unknown RDF quad component was found: '{0}'")]
    UnknownComponent(char),
    #[error(transparent)]
    Validation(#[from] RdfSortOrderValidationError),
}

impl std::str::FromStr for RdfSortOrder {
    type Err = RdfSortOrderParsingError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let upper = s.trim().to_uppercase();

        let result = if upper.starts_with("ZORDER(") && upper.ends_with(')') {
            let inner = &upper[7..upper.len() - 1];
            let components = parse_components(inner)?;
            RdfSortOrder::ZOrder(components)
        } else if upper.starts_with("NATIVE(") && upper.ends_with(')') {
            let inner = &upper[7..upper.len() - 1];
            let components = parse_components(inner)?;
            RdfSortOrder::NativeOrder(components)
        } else {
            let components = parse_components(s)?;
            RdfSortOrder::SparqlOrder(components)
        };

        result.validate()?;

        Ok(result)
    }
}

/// Parses the components of a sort order (e.g., SPO)
fn parse_components(inner: &str) -> Result<Vec<QuadComponent>, RdfSortOrderParsingError> {
    let mut components = Vec::new();
    for c in inner.chars() {
        if c == ',' || c.is_whitespace() {
            continue;
        }
        let comp = QuadComponent::from_char(c)
            .ok_or(RdfSortOrderParsingError::UnknownComponent(c))?;
        components.push(comp);
    }

    if components.is_empty() {
        return Err(RdfSortOrderParsingError::EmptyComponents);
    }

    Ok(components)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::QuadComponent;
    use std::str::FromStr;

    #[test]
    fn test_rdf_sort_order_parse_regular() {
        assert_eq!(
            RdfSortOrder::from_str("NATIVE(S)").unwrap(),
            RdfSortOrder::NativeOrder(vec![QuadComponent::Subject])
        );
    }

    #[test]
    fn test_rdf_sort_order_parse_zorder() {
        assert_eq!(
            RdfSortOrder::from_str("ZORDER(GSPO)").unwrap(),
            RdfSortOrder::ZOrder(vec![
                QuadComponent::GraphName,
                QuadComponent::Subject,
                QuadComponent::Predicate,
                QuadComponent::Object
            ])
        );
    }

    #[test]
    fn test_rdf_sort_order_parse_native() {
        assert_eq!(
            RdfSortOrder::from_str("NATIVE(S)").unwrap(),
            RdfSortOrder::NativeOrder(vec![QuadComponent::Subject])
        );
    }

    #[test]
    fn test_rdf_sort_order_validation() {
        assert!(RdfSortOrder::from_str("SPO").unwrap().validate().is_ok());
    }

    #[test]
    fn test_rdf_sort_order_validation_error() {
        assert_eq!(
            RdfSortOrder::from_str("SPOOS").unwrap_err().to_string(),
            "Duplicate components in sort order: [Subject, Object]"
        );
    }
}
