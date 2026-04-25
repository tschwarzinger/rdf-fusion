use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion_encoding::compute::with_string_encoding_from_plain_term;
use rdf_fusion_encoding::plain_term::{
    PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder,
};
use rdf_fusion_encoding::{EncodingArray, EncodingDatum, TermEncoding};
use rdf_fusion_model::{Literal, NamedNode, Term};
use std::hint::black_box;

fn bench_with_string_encoding_from_plain_term(c: &mut Criterion) {
    let num_terms = 10_000;
    let mut builder = PlainTermArrayElementBuilder::with_capacity(num_terms);

    for i in 0..num_terms {
        let term = match i % 3 {
            0 => Term::NamedNode(NamedNode::new_unchecked(format!("https://my.org/{i}"))),
            1 => Term::Literal(Literal::new_simple_literal(format!("string_{i}"))),
            _ => Term::Literal(Literal::new_typed_literal(
                format!("{i}"),
                NamedNode::new_unchecked("http://www.w3.org/2001/XMLSchema#integer"),
            )),
        };
        builder.append_term(term.as_ref());
    }

    let plain_term_array = builder.finish();
    let encoding_array = PLAIN_TERM_ENCODING
        .try_new_array(plain_term_array.into_array_ref())
        .unwrap();
    let datum = EncodingDatum::Array(encoding_array);

    let mut group = c.benchmark_group("Compute");
    group.bench_function("with_string_encoding_from_plain_term_10k", |b| {
        b.iter(|| {
            let result = with_string_encoding_from_plain_term(black_box(&datum)).unwrap();
            black_box(result);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_with_string_encoding_from_plain_term);
criterion_main!(benches);
