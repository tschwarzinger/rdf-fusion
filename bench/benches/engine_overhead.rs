//! Measures the query-engine overhead of RDF Fusion compared to [`spareval`]
//! (Oxigraph's query engine) **on the same storage layer** for small queries (BSBM Explore).
//!
//! # Setup
//!
//! [`MemoryQuadStorage`] keeps its quads in an [`oxrdf::Dataset`] and [`spareval`] can evaluate
//! queries directly on a `&Dataset`. Hence, the BSBM `dataset.nt` is loaded once into a
//! [`Dataset`] on which [`spareval`] runs directly, while RDF Fusion runs on a
//! [`MemoryQuadStorage`] that is filled with the same quads. Differences in the measurements are
//! therefore caused by the query engines and not by the storage layer.
//!
//! # Queries
//!
//! - One instance with a non-empty result of each BSBM Explore query (see
//!   [`BsbmUseCase::queries`]).
//! - Instances of BSBM Explore queries with an empty result, either because a triple pattern has
//!   no match or because the join of the triple patterns is empty (see
//!   [`BsbmUseCase::empty_queries`]).
//! - Synthetic "floor" queries:
//!   - `ASK {}` measures the fixed per-query cost.
//!   - `SELECT ?p ?o { <s> ?p ?o }` is a point lookup for the first subject of the data file.
//!   - `SELECT (COUNT(*) AS ?c) { ?s ?p ?o }` measures the per-row cost.
//!
//! # Parquet
//!
//! In addition, RDF Fusion is measured on a [`ParquetQuadStorage`] (a Parquet dataset in an
//! in-memory object store, string encoding) that contains the same quads. The files are kept in
//! memory to match spareval and the memory storage, which do no I/O. As the storage layers differ,
//! these measurements are not directly comparable with spareval. However, they show the overhead
//! of a storage layer that requires decoding.
//!
//! The quads are written in two sort orders (`GSPO` and `GPOS`), each as a separate quad table. The
//! storage chooses the quad table for each triple pattern, such that patterns with a bound subject
//! or predicate can skip most of the data. Without a sort order, every pattern scans all quads.
//!
//! The Parquet dataset is written with RDF Fusion's default Parquet writer settings:
//!   - `rdf-fusion-parquet-default/full`: default Parquet writer settings.
//!
//! [`ParquetQuadStorage`]: rdf_fusion::storage::parquet::ParquetQuadStorage
//!
//! # Zero Copy
//!
//! Currently, the setup favors spareval as it can do zero-copy pointers into the storage. For the
//! in-memory storage we currently have to allocate new Arrow arrays during scanning.
//!
//! [`BsbmUseCase::queries`]: rdf_fusion_bench::benchmarks::bsbm::BsbmUseCase::queries
//! [`BsbmUseCase::empty_queries`]: rdf_fusion_bench::benchmarks::bsbm::BsbmUseCase::empty_queries

mod utils;

use crate::utils::verbose::{is_verbose, print_query_details};
use criterion::{Criterion, criterion_group, criterion_main};
use futures::StreamExt;
use oxrdf::{Dataset, NamedOrBlankNode, Quad, QuadRef};
use oxrdfio::{RdfFormat, RdfParser};
use rdf_fusion::common::RdfSortOrder;
use rdf_fusion::common::config::RdfFusionOptions;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::results::QueryResults;
use rdf_fusion::execution::sparql::QueryOptions;
use rdf_fusion::options::query_options_to_parser;
use rdf_fusion::sparql_parser::parse_query;
use rdf_fusion::storage::memory::MemoryQuadStorage;
use rdf_fusion::store::Store;
use rdf_fusion_bench::benchmarks::bsbm::{BsbmBenchmark, ExploreUseCase, NumProducts};
use rdf_fusion_bench::benchmarks::{Benchmark, BenchmarkName};
use rdf_fusion_bench::environment::RdfFusionBenchContext;
use rdf_fusion_bench::{BenchQuadStorageTypeArg, QuadStorageLocationArg};
use spareval::QueryEvaluator;
use spargebra::{Query, SparqlParser};
use std::fs::File;
use std::io::BufReader;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};

/// The outcome of a query, used for comparing the results of both engines.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Boolean(bool),
    Solutions(usize),
    Graph(usize),
}

fn bench_engine_overhead(c: &mut Criterion) {
    // The empty queries are only pre-selected for this dataset size.
    let num_products = NumProducts::N1_000;
    let runtime = Builder::new_current_thread().enable_all().build().unwrap();

    let bench_context = RdfFusionBenchContext::new_for_criterion(
        PathBuf::from("./data"),
        QuadStorageEncodingName::PlainTerm,
        1,
    )
    .build();
    let name = BenchmarkName::BsbmExplore {
        num_products,
        max_query_count: None,
    };
    let context = bench_context.create_benchmark_context(name).unwrap();
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(&context, num_products, None).unwrap();

    let (dataset, first_subject) = load_dataset(&context.data_dir());
    let store = create_store(&runtime, &dataset);
    let parquet_store = create_parquet_store(&runtime, num_products, name);
    let verbose = is_verbose();

    let mut queries = benchmark.queries(&context).unwrap();
    let empty_queries = benchmark.empty_queries(&context).unwrap();
    let empty_query_names = empty_queries
        .iter()
        .map(|(name, _)| name.clone())
        .collect::<Vec<_>>();
    queries.extend(empty_queries);
    queries.push(("ASK".to_owned(), "ASK {}".to_owned()));
    queries.push((
        "Point Lookup".to_owned(),
        format!("SELECT ?p ?o {{ {first_subject} ?p ?o }}"),
    ));
    queries.push((
        "Count All".to_owned(),
        "SELECT (COUNT(*) AS ?c) { ?s ?p ?o }".to_owned(),
    ));

    let evaluator = QueryEvaluator::new();
    let query_options = QueryOptions::default();
    let parser_options = query_options_to_parser(&query_options).build();

    for (query_name, query_text) in queries {
        if verbose {
            runtime
                .block_on(print_query_details(
                    &store,
                    QueryOptions::default(),
                    &query_name,
                    &query_text,
                ))
                .unwrap();
        }

        let spareval_query = SparqlParser::new().parse_query(&query_text).unwrap();
        let spareval_outcome = run_spareval(&evaluator, &dataset, &spareval_query);
        let rdf_fusion_outcome = runtime.block_on(run_rdf_fusion(&store, &query_text));
        check_outcomes(
            &query_name,
            &spareval_query,
            spareval_outcome,
            rdf_fusion_outcome,
        );
        let parquet_outcome =
            runtime.block_on(run_rdf_fusion(&parquet_store, &query_text));
        check_outcomes(
            &query_name,
            &spareval_query,
            spareval_outcome,
            parquet_outcome,
        );
        if empty_query_names.contains(&query_name) {
            assert_eq!(
                spareval_outcome,
                Outcome::Solutions(0),
                "{query_name} is expected to have an empty result"
            );
        }

        let mut group =
            c.benchmark_group(format!("Engine Overhead {num_products} - {query_name}"));

        group.bench_function("spareval/parse", |b| {
            b.iter(|| SparqlParser::new().parse_query(&query_text).unwrap());
        });
        group.bench_function("spareval/plan", |b| {
            b.iter(|| {
                let query = SparqlParser::new().parse_query(&query_text).unwrap();
                let results = evaluator.prepare(&query).execute(&dataset).unwrap();
                drop(results);
            });
        });
        group.bench_function("spareval/full", |b| {
            b.iter(|| {
                let query = SparqlParser::new().parse_query(&query_text).unwrap();
                run_spareval(&evaluator, &dataset, &query)
            });
        });

        group.bench_function("rdf-fusion/parse", |b| {
            b.iter(|| {
                parse_query(&store.context().create_view(), &query_text, &parser_options)
                    .unwrap()
            });
        });
        group.bench_function("rdf-fusion/plan", |b| {
            b.to_async(&runtime).iter(|| async {
                let results = store
                    .query_opt(&query_text, QueryOptions::default())
                    .await
                    .unwrap();
                drop(results);
            });
        });
        group.bench_function("rdf-fusion/full", |b| {
            b.to_async(&runtime)
                .iter(|| async { run_rdf_fusion(&store, &query_text).await });
        });
        group.bench_function("rdf-fusion-parquet-default/full", |b| {
            b.to_async(&runtime)
                .iter(|| async { run_rdf_fusion(&parquet_store, &query_text).await });
        });

        group.finish();
    }
}

criterion_group!(
    name = engine_overhead;
    config = Criterion::default()
        .warm_up_time(Duration::from_secs(1))
        .measurement_time(Duration::from_secs(3));
    targets = bench_engine_overhead
);
criterion_main!(engine_overhead);

/// Loads `dataset.nt` into a [`Dataset`] and returns it with the first subject of the file.
fn load_dataset(data_dir: &Path) -> (Dataset, NamedOrBlankNode) {
    let path = data_dir.join("dataset.nt");
    let file = File::open(&path).unwrap_or_else(|err| {
        panic!(
            "Failed to open {}: {err}. Have you prepared the data (`just bench \
             prepare-benches-tests`) and are you running from the `bench` directory?",
            path.display()
        )
    });

    let mut dataset = Dataset::new();
    let mut first_subject = None;
    for quad in
        RdfParser::from_format(RdfFormat::NTriples).for_reader(BufReader::new(file))
    {
        let quad: Quad = quad.unwrap();
        first_subject.get_or_insert_with(|| quad.subject.clone());
        dataset.insert(&quad);
    }

    (dataset, first_subject.expect("Dataset must not be empty"))
}

/// Creates an RDF Fusion [`Store`] on a [`MemoryQuadStorage`] that contains the quads of
/// `dataset`.
fn create_store(runtime: &Runtime, dataset: &Dataset) -> Store {
    let store = Store::with_storage(Arc::new(MemoryQuadStorage::new()));
    runtime
        .block_on(store.extend(dataset.iter().map(QuadRef::into_owned)))
        .unwrap();
    store
}

/// The sort orders of the Parquet dataset. Each sort order is written as a separate quad table.
///
/// On BSBM, `GSPO` + `GPOS` is 39% faster (geomean) than an unsorted file. `GPOS` alone does not
/// help point lookups and an additional `GOSP` barely helps.
const PARQUET_SORT_ORDERS: [&str; 2] = ["NATIVE(GSPO)", "NATIVE(GPOS)"];

/// Creates an RDF Fusion [`Store`] on a Parquet dataset (string encoding, [`PARQUET_SORT_ORDERS`])
/// that contains the BSBM dataset. The files are written with the default Parquet writer settings.
fn create_parquet_store(
    runtime: &Runtime,
    num_products: NumProducts,
    name: BenchmarkName,
) -> Store {
    let bench_context = RdfFusionBenchContext::new_for_criterion(
        PathBuf::from("./data"),
        QuadStorageEncodingName::String,
        1,
    )
    .with_storage_type(BenchQuadStorageTypeArg::Parquet)
    .with_storage_location(QuadStorageLocationArg::InMemory)
    .with_configure_engine(|mut config| {
        config
            .options_mut()
            .extensions
            .get_mut::<RdfFusionOptions>()
            .expect("RDF Fusion options are registered")
            .storage
            .parquet
            .sort_orders = PARQUET_SORT_ORDERS
            .iter()
            .map(|sort_order| sort_order.parse::<RdfSortOrder>().unwrap())
            .collect();
        config
    })
    .build();
    let context = bench_context.create_benchmark_context(name).unwrap();
    let benchmark =
        BsbmBenchmark::<ExploreUseCase>::try_new(&context, num_products, None).unwrap();
    runtime
        .block_on(benchmark.prepare_store(&context, false))
        .unwrap()
}

/// Executes a query with spareval and drains all results.
fn run_spareval(evaluator: &QueryEvaluator, dataset: &Dataset, query: &Query) -> Outcome {
    match evaluator.prepare(query).execute(dataset).unwrap() {
        spareval::QueryResults::Boolean(value) => Outcome::Boolean(value),
        spareval::QueryResults::Solutions(solutions) => {
            let mut count = 0;
            for solution in solutions {
                solution.unwrap();
                count += 1;
            }
            Outcome::Solutions(count)
        }
        spareval::QueryResults::Graph(triples) => {
            let mut count = 0;
            for triple in triples {
                triple.unwrap();
                count += 1;
            }
            Outcome::Graph(count)
        }
    }
}

/// Executes a query with RDF Fusion and drains all results as solutions/triples.
async fn run_rdf_fusion(store: &Store, query: &str) -> Outcome {
    match store
        .query_opt(query, QueryOptions::default())
        .await
        .unwrap()
    {
        QueryResults::Boolean(value) => Outcome::Boolean(value),
        QueryResults::Solutions(mut solutions) => {
            let mut count = 0;
            while let Some(solution) = solutions.next().await {
                solution.unwrap();
                count += 1;
            }
            Outcome::Solutions(count)
        }
        QueryResults::Graph(mut triples) => {
            let mut count = 0;
            while let Some(triple) = triples.next().await {
                triple.unwrap();
                count += 1;
            }
            Outcome::Graph(count)
        }
    }
}

/// Aborts if the engines disagree on the result of a query (except for `DESCRIBE`).
fn check_outcomes(
    query_name: &str,
    query: &Query,
    spareval: Outcome,
    rdf_fusion: Outcome,
) {
    if spareval == rdf_fusion {
        return;
    }

    if matches!(query, Query::Describe { .. }) {
        println!(
            "WARNING: {query_name} (DESCRIBE) results differ (implementation-defined): \
             spareval={spareval:?}, RDF Fusion={rdf_fusion:?}"
        );
        return;
    }

    panic!(
        "{query_name}: Result mismatch between spareval ({spareval:?}) and RDF Fusion \
         ({rdf_fusion:?})"
    );
}
