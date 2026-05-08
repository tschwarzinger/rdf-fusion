use crate::encoding::{EncodingArray, TermDecoder};
use crate::plain_term::PlainTermEncoding;
use crate::plain_term::encoding::PlainTermType;
use crate::{EncodingScalar, TermEncoding};
use datafusion::arrow::array::{
    Array, AsArray, GenericStringArray, PrimitiveArray, StructArray,
};
use datafusion::arrow::datatypes::Int8Type;
use datafusion::common::ScalarValue;
use rdf_fusion_common::{
    BlankNodeRef, LiteralRef, NamedNodeRef, TermRef, ThinError, ThinResult,
};

#[derive(Debug)]
pub struct DefaultPlainTermDecoder;

/// Extracts a sequence of term references from the given array.
impl TermDecoder<PlainTermEncoding> for DefaultPlainTermDecoder {
    type Term<'data> = TermRef<'data>;

    fn decode_terms(
        array: &<PlainTermEncoding as TermEncoding>::Array,
    ) -> impl Iterator<Item = ThinResult<Self::Term<'_>>> {
        let array = array.inner().as_struct();

        let term_type = array.column(0).as_primitive::<Int8Type>();

        let value = array.column(1).as_string::<i32>();
        let datatype = array.column(2).as_string::<i32>();
        let language = array.column(3).as_string::<i32>();

        (0..array.len())
            .map(|idx| extract_term(array, term_type, value, datatype, language, idx))
    }

    fn decode_term(
        scalar: &<PlainTermEncoding as TermEncoding>::Scalar,
    ) -> ThinResult<Self::Term<'_>> {
        let ScalarValue::Struct(array) = scalar.scalar_value() else {
            panic!("Unexpected encoding. Should be ensured by the wrapping type.");
        };

        let term_type = array.column(0).as_primitive::<Int8Type>();
        let value = array.column(1).as_string::<i32>();
        let datatype = array.column(2).as_string::<i32>();
        let language = array.column(3).as_string::<i32>();

        extract_term(array, term_type, value, datatype, language, 0)
    }
}

fn extract_term<'data>(
    array: &'data StructArray,
    term_type: &'data PrimitiveArray<Int8Type>,
    value: &'data GenericStringArray<i32>,
    datatype: &'data GenericStringArray<i32>,
    language: &'data GenericStringArray<i32>,
    idx: usize,
) -> ThinResult<TermRef<'data>> {
    array
        .is_valid(idx)
        .then(|| {
            let term_type = PlainTermType::try_from(term_type.value(idx)).expect(
                "Unexpected term type encoding. Should be ensured by the wrapping type.",
            );
            decode_term(value, datatype, language, idx, term_type)
        })
        .ok_or(ThinError::ExpectedError)
}

fn decode_term<'data>(
    value: &'data GenericStringArray<i32>,
    datatype: &'data GenericStringArray<i32>,
    language: &'data GenericStringArray<i32>,
    idx: usize,
    term_type: PlainTermType,
) -> TermRef<'data> {
    match term_type {
        PlainTermType::NamedNode => {
            TermRef::NamedNode(NamedNodeRef::new_unchecked(value.value(idx)))
        }
        PlainTermType::BlankNode => {
            TermRef::BlankNode(BlankNodeRef::new_unchecked(value.value(idx)))
        }
        PlainTermType::Literal => {
            if language.is_valid(idx) {
                TermRef::Literal(LiteralRef::new_language_tagged_literal_unchecked(
                    value.value(idx),
                    language.value(idx),
                ))
            } else {
                TermRef::Literal(LiteralRef::new_typed_literal(
                    value.value(idx),
                    NamedNodeRef::new_unchecked(datatype.value(idx)),
                ))
            }
        }
    }
}
