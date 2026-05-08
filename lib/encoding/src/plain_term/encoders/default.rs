use crate::encoding::TermEncoder;
use crate::plain_term::{PlainTermArrayElementBuilder, PlainTermEncoding};
use crate::{EncodingArray, TermEncoding};
use rdf_fusion_common::DFResult;
use rdf_fusion_common::{TermRef, ThinResult};

#[derive(Debug, Default)]
pub struct DefaultPlainTermEncoder;

impl TermEncoder<PlainTermEncoding> for DefaultPlainTermEncoder {
    type Term<'data> = TermRef<'data>;

    fn encode_terms<'data>(
        &self,
        terms: impl IntoIterator<Item = ThinResult<Self::Term<'data>>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Array> {
        let mut value_builder = PlainTermArrayElementBuilder::default();
        for value in terms {
            match value {
                Ok(TermRef::NamedNode(value)) => value_builder.append_named_node(value),
                Ok(TermRef::BlankNode(value)) => value_builder.append_blank_node(value),
                Ok(TermRef::Literal(value)) => value_builder.append_literal(value),
                Err(_) => value_builder.append_null(),
            }
        }
        Ok(value_builder.finish())
    }

    fn encode_term(
        &self,
        term: ThinResult<Self::Term<'_>>,
    ) -> DFResult<<PlainTermEncoding as TermEncoding>::Scalar> {
        self.encode_terms([term])?.try_as_scalar(0)
    }
}
