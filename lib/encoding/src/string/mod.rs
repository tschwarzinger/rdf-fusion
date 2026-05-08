mod array;
mod encoding;
mod scalar;

pub use array::*;
use datafusion::common::{DataFusionError, exec_err};
pub use encoding::*;
use rdf_fusion_common::vocab::xsd;
use rdf_fusion_common::{BlankNode, DFResult, Literal, NamedNode, Term};
pub use scalar::*;

pub(crate) fn parse_turtle_term(s: &str) -> DFResult<Term> {
    if s.is_empty() {
        return exec_err!("Empty string is not a valid RDF term");
    }

    if s.starts_with('<') && s.ends_with('>') {
        let iri = &s[1..s.len() - 1];
        let nn = NamedNode::new(iri)
            .map_err(|e| DataFusionError::Execution(format!("Invalid IRI '{s}': {e}")))?;
        return Ok(Term::NamedNode(nn));
    }

    if let Some(stripped) = s.strip_prefix("_:") {
        let bn = BlankNode::new(stripped).map_err(|e| {
            DataFusionError::Execution(format!("Invalid BlankNode '{s}': {e}"))
        })?;
        return Ok(Term::BlankNode(bn));
    }

    if s.starts_with('"') {
        let last_quote = s.rfind('"').ok_or_else(|| {
            DataFusionError::Execution(format!("Invalid literal format: {s}"))
        })?;
        let value = &s[1..last_quote];
        // Basic unescaping (simplified for Turtle requirements in prompt)
        let unescaped_value = unescape_turtle(value)?;

        let suffix = &s[last_quote + 1..];
        if suffix.is_empty() {
            return Ok(Term::Literal(Literal::new_simple_literal(unescaped_value)));
        } else if let Some(stripped) = suffix.strip_prefix('@') {
            let lit = Literal::new_language_tagged_literal(unescaped_value, stripped)
                .map_err(|e| {
                    DataFusionError::Execution(format!(
                        "Invalid language tag in '{s}': {e}"
                    ))
                })?;
            return Ok(Term::Literal(lit));
        } else if let Some(datatype_str) = suffix.strip_prefix("^^") {
            let datatype_iri =
                if datatype_str.starts_with('<') && datatype_str.ends_with('>') {
                    &datatype_str[1..datatype_str.len() - 1]
                } else if datatype_str == "xsd::integer" {
                    xsd::INTEGER.as_str()
                } else if datatype_str == "xsd::string" {
                    xsd::STRING.as_str()
                } else {
                    return exec_err!("Unsupported datatype format: {datatype_str}");
                };

            let datatype = NamedNode::new(datatype_iri).map_err(|e| {
                DataFusionError::Execution(format!(
                    "Invalid datatype IRI '{datatype_str}': {e}"
                ))
            })?;
            return Ok(Term::Literal(Literal::new_typed_literal(
                unescaped_value,
                datatype,
            )));
        }
    }

    exec_err!("Failed to parse Turtle term: {s}")
}

fn unescape_turtle(s: &str) -> DFResult<String> {
    let mut result = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('t') => result.push('\t'),
                Some('b') => result.push('\u{0008}'),
                Some('n') => result.push('\n'),
                Some('r') => result.push('\r'),
                Some('f') => result.push('\u{000c}'),
                Some('"') => result.push('"'),
                Some('\'') => result.push('\''),
                Some('\\') => result.push('\\'),
                Some('u') => {
                    let hex: String = chars.by_ref().take(4).collect();
                    let code = u32::from_str_radix(&hex, 16).map_err(|e| {
                        DataFusionError::Execution(format!("Invalid \\u escape: {e}"))
                    })?;
                    let c = std::char::from_u32(code).ok_or_else(|| {
                        DataFusionError::Execution("Invalid Unicode escape".to_string())
                    })?;
                    result.push(c);
                }
                Some('U') => {
                    let hex: String = chars.by_ref().take(8).collect();
                    let code = u32::from_str_radix(&hex, 16).map_err(|e| {
                        DataFusionError::Execution(format!("Invalid \\U escape: {e}"))
                    })?;
                    let c = std::char::from_u32(code).ok_or_else(|| {
                        DataFusionError::Execution("Invalid Unicode escape".to_string())
                    })?;
                    result.push(c);
                }
                Some(other) => result.push(other),
                None => return exec_err!("Trailing backslash in literal"),
            }
        } else {
            result.push(c);
        }
    }
    Ok(result)
}
