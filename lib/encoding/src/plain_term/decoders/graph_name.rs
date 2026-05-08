use crate::TermEncoding;
use crate::encoding::TermDecoder;
use crate::plain_term::PlainTermEncoding;
use crate::plain_term::decoders::DefaultPlainTermDecoder;
use rdf_fusion_common::{GraphNameRef, TermRef, ThinResult};

#[derive(Debug)]
pub struct GraphNameRefPlainTermDecoder;

/// Extracts a sequence of term references from the given array.
impl TermDecoder<PlainTermEncoding> for GraphNameRefPlainTermDecoder {
    type Term<'data> = GraphNameRef<'data>;

    fn decode_terms(
        array: &<PlainTermEncoding as TermEncoding>::Array,
    ) -> impl Iterator<Item = ThinResult<Self::Term<'_>>> {
        DefaultPlainTermDecoder::decode_terms(array).map(map_term_ref_to_graph_name_ref)
    }

    fn decode_term(
        scalar: &<PlainTermEncoding as TermEncoding>::Scalar,
    ) -> ThinResult<Self::Term<'_>> {
        let term = DefaultPlainTermDecoder::decode_term(scalar);
        map_term_ref_to_graph_name_ref(term)
    }
}

fn map_term_ref_to_graph_name_ref(
    term: ThinResult<TermRef<'_>>,
) -> ThinResult<GraphNameRef<'_>> {
    match term {
        Ok(TermRef::NamedNode(nn)) => Ok(GraphNameRef::NamedNode(nn)),
        Ok(TermRef::BlankNode(bnode)) => Ok(GraphNameRef::BlankNode(bnode)),
        Ok(TermRef::Literal(_)) => panic!("Literal when extracting graph name"),
        Err(_) => Ok(GraphNameRef::DefaultGraph),
    }
}
