use criterion::{Criterion, criterion_group, criterion_main};
use datafusion::arrow::array::StringArray;
use rdf_fusion_encoding::compute::with_plain_term_encoding_from_string;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::{EncodingDatum, TermEncoding};
use std::hint::black_box;
use std::sync::Arc;

fn bench_with_plain_term_encoding_from_string(c: &mut Criterion) {
    let num_terms = 10_000;
    let mut terms = Vec::with_capacity(num_terms);
    for i in 0..num_terms {
        let term = match i % 3 {
            0 => format!("<https://my.org/{i}>"),
            1 => format!("\"string_{i}\""),
            _ => format!("\"{i}\"^^xsd::integer"),
        };
        terms.push(Some(term));
    }

    let array = Arc::new(StringArray::from(terms));
    let encoding_array = STRING_ENCODING.try_new_array(array).unwrap();
    let datum = EncodingDatum::Array(encoding_array);

    let mut group = c.benchmark_group("Compute");
    group.bench_function("with_plain_term_encoding_from_string_10k", |b| {
        b.iter(|| {
            let result = with_plain_term_encoding_from_string(black_box(&datum)).unwrap();
            black_box(result);
        })
    });
    group.finish();
}

criterion_group!(benches, bench_with_plain_term_encoding_from_string);
criterion_main!(benches);
