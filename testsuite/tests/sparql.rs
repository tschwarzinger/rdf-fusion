#![cfg(test)]

use anyhow::Result;
use rdf_fusion::common::RdfInput;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::storage::delta::DeltaQuadsStorageBuilder;
use rdf_fusion::storage::parquet::{ParquetQuadStorage, RdfParquetLoader};
use rdf_fusion::store::Store;
use rdf_fusion_testsuite::w3c::files::W3CTestRuntime;
use rdf_fusion_testsuite::w3c::utils::W3CTestUtils;
use rdf_fusion_testsuite::w3c::{StoreFactory, W3CSparqlTestSuiteBuilder};
use std::sync::Arc;

#[tokio::test]
async fn sparql10_w3c_query_syntax_testsuite() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-syntax.ttl",
    )
        .await?
        .ignore_test(
            // Tokenizer
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql3/manifest#syn-bad-26",
        )
        .build()
        .await?
        .run()
        .await
        .assert_success();

    Ok(())
}

/// Contains tests that are not yet supported by RDF Fusion
const UNSUPPORTED_SPARQL10_TESTS: [&str; 14] = [
    // Testing equality of illegal literals ("xyz"^^<http://www.w3.org/2001/XMLSchema#integer>)
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-07",
    //Simple literal vs xsd:string. We apply RDF 1.1
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-08",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-10",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-11",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#open-eq-12",
    // We use XSD 1.1 equality on dates
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/open-world/manifest#date-2",
    // We choose to simplify first the nested group patterns in OPTIONAL
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/optional-filter/manifest#dawg-optional-filter-005-not-simplified",
    // This test relies on naive iteration on the input file
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/reduced/manifest#reduced-1",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/reduced/manifest#reduced-2",
    // GRAPH ?g with an empty or unknown graph pattern should range over the named graphs
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/graph/manifest#graph-empty",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/graph/manifest#graph-not-exist",
    // Nested GRAPH should be evaluated against all named graphs, not only the outer one
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/graph/manifest#graph-nested-2",
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/graph/manifest#graph-nested-3",
    // The variable bound by GRAPH must not affect the inner OPTIONAL
    "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/graph/manifest#graph-optional",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_delta_plain_term() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(plain_term_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_delta_object_id() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_delta_plain_term_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(plain_term_store_factory())
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_delta_object_id_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_delta_string() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(string_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql10_query_evaluation_delta_string_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(string_store_factory())
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn w3c_sparql10_query_evaluation_parquet_string() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(parquet_store_factory(QuadStorageEncodingName::String))
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn w3c_sparql10_query_evaluation_parquet_plain_term() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest-evaluation.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL10_TESTS)
    .with_store_factory(parquet_store_factory(QuadStorageEncodingName::PlainTerm))
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

/// Contains tests that are not yet supported by RDF Fusion
const UNSUPPORTED_SPARQL11_TESTS: [&str; 25] = [
    // GROUP_CONCAT must always return xsd:string, even if all inputs share a language tag
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-groupconcat-04",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-groupconcat-06",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-groupconcat-distinct",
    // COUNT(DISTINCT *) with GROUP BY
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-count-rows-distinct",
    // Patterns without triple patterns (VALUES, subqueries, implicit grouping) inside GRAPH ?g must be
    // evaluated once per named graph of the dataset
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/aggregates/manifest#agg-empty-group-count-graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#graph",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#graph-no-var",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#graph-from-named-1",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#graph-from-named-2",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#graph-from-named-3",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/subquery/manifest#subquery15",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/subquery/manifest#subquery16",
    // The variable bound by GRAPH must not affect MINUS disjointness
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/negation/manifest#graph-minus",
    // Post-query VALUES may bind variables that are not grouped
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#values-construct-group-by",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#values-group-by-projection",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#values-group-by-select-expression",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/bindings/manifest#values-group-order-by",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-query/manifest#test_group_by_values_scope_01",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/syntax-query/manifest#test_group_by_values_scope_02",
    // COALESCE() without arguments
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#coalesce-empty",
    // IRI()/URI() on an IRI
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/functions/manifest#iri02",
    // * and ? with a fixed start/end must not check whether the term exists in the graph
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#zero_or_more_set_end",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#zero_or_more_set_start",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#zero_or_one_set_end",
    "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/property-path/manifest#zero_or_one_set_start",
];

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_delta_plain_term() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL11_TESTS)
    .with_store_factory(plain_term_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_delta_plain_term_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL11_TESTS)
    .with_store_factory(plain_term_store_factory())
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_delta_object_id() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL11_TESTS)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_delta_object_id_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL11_TESTS)
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_delta_string() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL11_TESTS)
    .with_store_factory(string_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_delta_string_with_optimize() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL11_TESTS)
    .with_store_factory(string_store_factory())
    .with_optimize_after_load(true)
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_query_evaluation_parquet_string() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL11_TESTS)
    .with_store_factory(parquet_store_factory(QuadStorageEncodingName::String))
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test]
async fn w3c_sparql11_query_evaluation_parquet_plain_term() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
    )
    .await?
    .ignore_tests(UNSUPPORTED_SPARQL11_TESTS)
    .with_store_factory(parquet_store_factory(QuadStorageEncodingName::PlainTerm))
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_json_evaluation() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/json-res/manifest.ttl",
    )
    .await?
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_tsv_evaluation() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/csv-tsv-res/manifest.ttl",
    )
        .await?
        .ignore_tests([
            // We do not run CSVResultFormatTest tests yet
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv01",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv02",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv03",
            "http://www.w3.org/2009/sparql/docs/tests/data-sparql11/csv-tsv-res/manifest#csv04",
        ])
        .build()
        .await?
        .run()
        .await
        .assert_success();

    Ok(())
}

/// Creates the [`Store`] using the plain term encoding that is used for the plain term encoding
/// tests.
fn plain_term_store_factory() -> StoreFactory {
    Arc::new(|config| {
        Box::pin(async move {
            let memory_store = Arc::new(object_store::memory::InMemory::new());
            config.runtime_env.register_object_store(
                &url::Url::parse("memory://").unwrap(),
                Arc::clone(&memory_store) as Arc<dyn object_store::ObjectStore>,
            );

            let log_store = deltalake::logstore::logstore_with(
                Arc::clone(&memory_store) as Arc<dyn object_store::ObjectStore>,
                &url::Url::parse("memory:///").unwrap(),
                deltalake::logstore::StorageConfig::default().with_io_runtime(
                    deltalake::logstore::IORuntime::RT(tokio::runtime::Handle::current()),
                ),
            )
            .unwrap();

            let delta_storage = DeltaQuadsStorageBuilder::new()
                .with_encoding(QuadStorageEncodingName::PlainTerm)
                .with_log_store(log_store)
                .build()
                .await
                .unwrap();

            let context = RdfFusionContextBuilder::new(Arc::new(delta_storage))
                .with_runtime_env(Some(Arc::clone(&config.runtime_env)))
                .with_single_partition_session_config()
                .build()
                .unwrap();
            let store = Store::new(context);

            let utils = W3CTestUtils::new(W3CTestRuntime::new(config.runtime_env));
            for (name, source) in config.default_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }
            for (name, source) in config.named_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }

            Ok(store)
        })
    })
}

/// Creates the [`Store`] using the plain term encoding that is used for the plain term encoding
/// tests.
fn string_store_factory() -> StoreFactory {
    Arc::new(|config| {
        Box::pin(async move {
            let memory_store = Arc::new(object_store::memory::InMemory::new());
            config.runtime_env.register_object_store(
                &url::Url::parse("memory://").unwrap(),
                Arc::clone(&memory_store) as Arc<dyn object_store::ObjectStore>,
            );

            let log_store = deltalake::logstore::logstore_with(
                Arc::clone(&memory_store) as Arc<dyn object_store::ObjectStore>,
                &url::Url::parse("memory:///").unwrap(),
                deltalake::logstore::StorageConfig::default().with_io_runtime(
                    deltalake::logstore::IORuntime::RT(tokio::runtime::Handle::current()),
                ),
            )
            .unwrap();

            let delta_storage = DeltaQuadsStorageBuilder::new()
                .with_encoding(QuadStorageEncodingName::String)
                .with_log_store(log_store)
                .build()
                .await
                .unwrap();

            let context = RdfFusionContextBuilder::new(Arc::new(delta_storage))
                .with_runtime_env(Some(Arc::clone(&config.runtime_env)))
                .with_single_partition_session_config()
                .build()
                .unwrap();
            let store = Store::new(context);

            let utils = W3CTestUtils::new(W3CTestRuntime::new(config.runtime_env));
            for (name, source) in config.default_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }
            for (name, source) in config.named_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }

            Ok(store)
        })
    })
}

/// Creates the [`Store`] using the data dump storage.
fn parquet_store_factory(encoding: QuadStorageEncodingName) -> StoreFactory {
    Arc::new(move |config| {
        Box::pin(async move {
            let output_url = url::Url::parse("memory:///dataset.parquet").unwrap();

            let delta_storage =
                Arc::new(DeltaQuadsStorageBuilder::new().build().await.unwrap());
            let context = RdfFusionContextBuilder::new(delta_storage)
                .with_runtime_env(Some(Arc::clone(&config.runtime_env)))
                .with_single_partition_session_config()
                .build()
                .unwrap();

            let loader = RdfParquetLoader::try_new(
                context.session_context().clone(),
                context.create_view(),
                encoding,
                Vec::new(),
            )
            .unwrap();
            let mut inputs = Vec::new();
            for (name, source) in config.default_graphs {
                inputs.push(RdfInput::new_with_format(source.url, name, source.format));
            }
            for (name, source) in config.named_graphs {
                inputs.push(RdfInput::new_with_format(source.url, name, source.format));
            }

            loader.load_many(inputs, output_url.clone()).await.unwrap();

            let state = context.session_context().state();
            let storage = ParquetQuadStorage::builder(output_url)
                .with_encoding(encoding)
                .build(&state)
                .await
                .unwrap();

            let context = RdfFusionContextBuilder::new(Arc::new(storage))
                .with_runtime_env(Some(config.runtime_env))
                .with_single_partition_session_config()
                .build()
                .unwrap();

            Ok(Store::new(context))
        })
    })
}
