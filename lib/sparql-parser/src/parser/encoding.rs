//! These functions are taken from oxigraph.
//!
//! See <https://github.com/oxigraph/oxigraph/blob/db207116c1a3ad1e3b80498a21d4508dfaf83361/lib/spargebra/src/algebra_builder.rs>

use crate::SparqlParseError;
use std::borrow::Cow;

pub fn unescape_iriref(mut input: &str) -> Result<Cow<'_, str>, SparqlParseError> {
    let mut output = None;
    while let Some((before, after)) = input.split_once('\\') {
        let output: &mut String = output.get_or_insert_default();
        output.push_str(before);
        let mut after = after.chars();
        let (escape, after) = match after.next() {
            Some('u') => read_hex_char::<4>(after.as_str())?,
            Some('U') => read_hex_char::<8>(after.as_str())?,
            Some(c) => {
                unreachable!(
                    "IRIs are only allowed to contain escape sequences \\uXXXX and \\UXXXXXXXX, found \\{c}"
                );
            }
            None => {
                unreachable!("IRIs are not allowed to end with a '\\'");
            }
        };
        output.push(escape);
        input = after;
    }
    Ok(if let Some(mut output) = output {
        output.push_str(input);
        output.into()
    } else {
        input.into()
    })
}

pub fn unescape_local_name(mut input: &str) -> (Cow<'_, str>, bool) {
    let mut output = None;
    let mut might_be_invalid_iri = false;
    while let Some((before, after)) = input.split_once('\\') {
        let output: &mut String = output.get_or_insert_default();
        output.push_str(before);
        let mut chars = after.chars();
        let Some(escape) = chars.next() else {
            unreachable!("PNAME_LOCAL is not allowed to end with a '\\'");
        };
        output.push(escape);
        if matches!(escape, '/' | '?' | '#' | '@' | '%') {
            might_be_invalid_iri = true;
        }
        input = chars.as_str();
    }
    (
        if let Some(mut output) = output {
            output.push_str(input);
            output.into()
        } else {
            input.into()
        },
        might_be_invalid_iri,
    )
}

pub fn unescape_string(mut input: &str) -> Result<Cow<'_, str>, SparqlParseError> {
    let mut output = None;
    while let Some((before, after)) = input.split_once('\\') {
        let output: &mut String = output.get_or_insert_default();
        output.push_str(before);
        let mut after = after.chars();
        let (escape, after) = match after.next() {
            Some('t') => ('\u{0009}', after.as_str()),
            Some('b') => ('\u{0008}', after.as_str()),
            Some('n') => ('\u{000A}', after.as_str()),
            Some('r') => ('\u{000D}', after.as_str()),
            Some('f') => ('\u{000C}', after.as_str()),
            Some('"') => ('\u{0022}', after.as_str()),
            Some('\'') => ('\u{0027}', after.as_str()),
            Some('\\') => ('\u{005C}', after.as_str()),
            Some('u') => read_hex_char::<4>(after.as_str())?,
            Some('U') => read_hex_char::<8>(after.as_str())?,
            Some(c) => {
                unreachable!("{c} is not an allowed escaping in strings");
            }
            None => {
                unreachable!("strings are not allowed to end with a '\\'");
            }
        };
        output.push(escape);
        input = after;
    }

    Ok(if let Some(mut output) = output {
        output.push_str(input);
        output.into()
    } else {
        input.into()
    })
}

fn read_hex_char<const SIZE: usize>(
    input: &str,
) -> Result<(char, &str), SparqlParseError> {
    let escape = input.get(..SIZE).ok_or_else(|| {
        SparqlParseError::new_without_span(
            "\\u escape sequence must contain 4 characters",
        )
    })?;
    let char = u32::from_str_radix(escape, 16).map_err(|_| {
        SparqlParseError::new_without_span(
            "\\u escape sequence must be followed by hexadecimal digits",
        )
    })?;
    let char = char::from_u32(char).ok_or_else(|| {
        SparqlParseError::new_without_span(format!(
            "{char:#X} is not a valid unicode codepoint (surrogates are not supported)"
        ))
    })?;
    Ok((char, &input[SIZE..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unescape_local_name() {
        assert_eq!(unescape_local_name(r"c\:d\?").0, "c:d?");
        assert_eq!(unescape_local_name(r"c\~z\.").0, "c~z.");
        assert_eq!(unescape_local_name(r"b%3D").0, "b%3D");
    }
}
