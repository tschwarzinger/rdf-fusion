use crate::EncodingDatum;
use crate::plain_term::{PlainTermArray, PlainTermArrayElementBuilder};
use crate::string::StringEncoding;
use datafusion::arrow::array::{Array, AsArray};
use rdf_fusion_model::AResult;

/// Changes the encoding from the [`StringEncoding`] to the
/// [`PlainTermEncoding`](rdf_fusion_encoding::string::StringEncoding).
pub fn with_plain_term_encoding_from_string(
    datum: &EncodingDatum<StringEncoding>,
) -> AResult<PlainTermArray> {
    let array = datum.to_array(1);
    let array = array.inner().as_string::<i32>();
    let mut builder = PlainTermArrayElementBuilder::with_capacity(array.len());
    for i in 0..array.len() {
        if array.is_null(i) {
            builder.append_null();
        } else {
            let s = array.value(i);
            let term = crate::string::parse_turtle_term(s)?;
            builder.append_term(term.as_ref());
        }
    }
    Ok(builder.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::string::STRING_ENCODING;
    use crate::{EncodingArray, TermEncoding};
    use datafusion::arrow::array::StringArray;
    use datafusion::arrow::util::pretty::pretty_format_columns;
    use insta::assert_snapshot;
    use std::sync::Arc;

    #[test]
    fn test_with_plain_term_encoding_from_string() {
        let array = Arc::new(StringArray::from(vec![
            Some("<https://my.org/1>"),
            Some("\"literal\""),
            Some("\"42\"^^xsd::integer"),
            None,
        ]));
        let encoding_array = STRING_ENCODING.try_new_array(array).unwrap();
        let datum = EncodingDatum::Array(encoding_array);

        let result = with_plain_term_encoding_from_string(&datum).unwrap();
        let printed =
            pretty_format_columns("result", &[result.into_array_ref()]).unwrap();
        assert_snapshot!(printed, @r###"
        +----------------------------------------------------------------------------------------------------+
        | result                                                                                             |
        +----------------------------------------------------------------------------------------------------+
        | {term_type: 0, value: https://my.org/1, data_type: , language_tag: }                               |
        | {term_type: 2, value: literal, data_type: http://www.w3.org/2001/XMLSchema#string, language_tag: } |
        | {term_type: 2, value: 42, data_type: http://www.w3.org/2001/XMLSchema#integer, language_tag: }     |
        |                                                                                                    |
        +----------------------------------------------------------------------------------------------------+
        "###);
    }
}
