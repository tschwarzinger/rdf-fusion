mod utils;

use crate::utils::setup_benchmark_env;
use criterion::{Criterion, criterion_group, criterion_main};
use rdf_fusion::common::RdfDumpFormat;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::store::RdfDumpOptions;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use std::path::PathBuf;

fn bench_dump_formats(c: &mut Criterion) {
    let benchmarking_context = RdfFusionBenchContext::new_for_criterion(
        PathBuf::from("./data"),
        QuadStorageEncodingName::ObjectId,
        1,
    )
    .build();
    let name = rdf_fusion_bench::benchmarks::BenchmarkName::BsbmExplore {
        num_products: NumProducts::N10_000,
        max_query_count: None,
    };
    let context = benchmarking_context.create_benchmark_context(name).unwrap();
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(&context, NumProducts::N10_000, None)
            .unwrap();

    let (runtime, _benchmark_context, store) =
        setup_benchmark_env(&benchmarking_context, &benchmark);

    let temp_dir = std::env::temp_dir().join("rdf-fusion-bench-dump");
    std::fs::create_dir_all(&temp_dir).unwrap();

    for format in RdfDumpFormat::LIST_ALL {
        let format_name = format.to_string();
        let file_ext = format.file_extension();

        c.bench_function(&format!("Dump Store ({format_name})"), |b| {
            b.to_async(&runtime).iter(|| async {
                let output_path = temp_dir.join(format!("output.{file_ext}"));
                let output_url = format!("file://{}", output_path.to_str().unwrap());

                store
                    .dump(output_url, *format, RdfDumpOptions::default())
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
