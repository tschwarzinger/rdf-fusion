use crate::plain_term::PlainTermEncoding;
use crate::plain_term::decoders::DefaultPlainTermDecoder;
use crate::string::{STRING_ENCODING, StringEncodingArray};
use crate::{EncodingDatum, EncodingScalar, TermDecoder, TermEncoder};
use datafusion::arrow::error::ArrowError;
use rdf_fusion_common::AResult;

/// Changes the encoding from the [`PlainTermEncoding`] to the
/// [`StringEncoding`](rdf_fusion_encoding::string::StringEncoding).
pub fn with_string_encoding_from_plain_term(
    datum: &EncodingDatum<PlainTermEncoding>,
) -> AResult<StringEncodingArray> {
    match datum {
        EncodingDatum::Array(array) => {
            let terms = DefaultPlainTermDecoder::decode_terms(array);
            STRING_ENCODING
                .encode_terms(terms)
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
        }
        EncodingDatum::Scalar(scalar) => {
            let term = DefaultPlainTermDecoder::decode_term(scalar);
            STRING_ENCODING
                .encode_term(term)
                .map(|s| s.to_array(1).unwrap())
                .map_err(|e| ArrowError::ExternalError(Box::new(e)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plain_term::{PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder};
    use crate::{EncodingArray, TermEncoding};
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use insta::assert_snapshot;
    use rdf_fusion_common::{Literal, NamedNode, Term};

    #[test]
    fn test_with_string_encoding_from_plain_term_array() {
        let mut builder = PlainTermArrayElementBuilder::with_capacity(3);
        builder.append_term(
            Term::NamedNode(NamedNode::new_unchecked("https://my.org/1")).as_ref(),
        );
        builder
            .append_term(Term::Literal(Literal::new_simple_literal("literal")).as_ref());
        builder.append_term(
            Term::Literal(Literal::new_typed_literal(
                "42",
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            ))
            .as_ref(),
        );
        builder.append_null();
        let array = builder.finish();

        let encoding_array = PLAIN_TERM_ENCODING
            .try_new_array(array.into_array_ref())
            .unwrap();
        let datum = EncodingDatum::Array(encoding_array);

        let result = with_string_encoding_from_plain_term(&datum).unwrap();
        let printed =
            pretty_format_columns("result", &[result.into_array_ref()]).unwrap();
        assert_snapshot!(printed, @r###"
        +--------------------------------------------------+
        | result                                           |
        +--------------------------------------------------+
        | <https://my.org/1>                               |
        | "literal"                                        |
        | "42"^^<http://www.w3.org/2001/XMLSchema#integer> |
        |                                                  |
        +--------------------------------------------------+
        "###);
    }
}
