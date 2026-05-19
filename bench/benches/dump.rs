mod utils;

use crate::utils::{ENCODINGS_TO_BENCHMARK, setup_benchmark_env};
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::common::RdfFormat;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

fn bench_dump_formats(c: &mut Criterion) {
    let encoding = ENCODINGS_TO_BENCHMARK[0];
    let benchmarking_context =
        RdfFusionBenchContext::new_for_criterion(PathBuf::from("./data"), encoding, 1)
            .build();
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(NumProducts::N10_000, None).unwrap();

    let (runtime, _benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);

    let temp_dir = std::env::temp_dir().join("rdf-fusion-bench-dump");
    std::fs::create_dir_all(&temp_dir).unwrap();

    for format_name in ["ttl", "nq", "parquet"] {
        c.bench_function(&format!("Dump Store ({format_name})"), |b| {
            b.to_async(&runtime).iter(|| async {
                use rdf_fusion::store::DumpOptions;
                let output_path = temp_dir.join(format!("output.{format_name}"));
                let output_url = format!("file://{}", output_path.to_str().unwrap());

                let format = match format_name {
                    "parquet" => RdfFormat::Parquet,
                    "ttl" => RdfFormat::Turtle,
                    "nq" => RdfFormat::NQuads,
                    _ => unreachable!(),
                };

                store
                    .dump(output_url, format, DumpOptions::default())
                    .await
                    .unwrap();
            });
        });
    }
}

criterion_group!(
    name = bench_dump;
    config = Criterion::default().sample_size(10);
    targets = bench_dump_formats
);
criterion_main!(bench_dump);
