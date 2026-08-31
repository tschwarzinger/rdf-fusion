use crate::sanitize_snapshot_name;
use anyhow::{Context, Error};
use datafusion::arrow::datatypes::DataType;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDF, ScalarUDFImpl, Signature, Volatility,
};
use insta::{Settings, assert_snapshot};
use rdf_fusion::common::Iri;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion::encoding::{RdfFusionEncodings, TermEncoding};
use rdf_fusion::extensions::functions::FunctionName;
use rdf_fusion::functions::scalar::SparqlUDFTypeSignatureBuilder;
use rdf_fusion_common::DateTime;
use rdf_fusion_sparql_parser::{ParserConfig, SparqlParser};
use rdf_fusion_testsuite::store_factories::parquet_store_factory;
use rdf_fusion_testsuite::test::{Test, TestOutcome};
use rdf_fusion_testsuite::w3c::files::{TEST_RUNTIME_ENV, W3CTestRuntime};
use rdf_fusion_testsuite::w3c::{
    StoreConfig, StoreFactory, W3CSparqlTestSuiteBuilder, manifest,
};
use std::sync::Arc;

#[tokio::test]
async fn w3c_sparql10_query_parser_positive() -> anyhow::Result<()> {
    run_positive_parser_tests(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest.ttl",
        "sparql1.0",
    )
    .await?
}

#[tokio::test]
async fn w3c_sparql10_query_parser_negative() -> anyhow::Result<()> {
    run_negative_parser_tests(
        "https://w3c.github.io/rdf-tests/sparql/sparql10/manifest.ttl",
        "sparql1.0-negative",
        &[
            "http://www.w3.org/2001/sw/DataAccess/tests/data-r2/syntax-sparql3/manifest#syn-bad-26",
        ],
    )
    .await?
}

#[tokio::test]
async fn w3c_sparql11_query_parser_positive() -> anyhow::Result<()> {
    run_positive_parser_tests(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
        "sparql1.1",
    )
    .await?
}

#[tokio::test]
async fn w3c_sparql11_query_parser_negative() -> anyhow::Result<()> {
    run_negative_parser_tests(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-query.ttl",
        "sparql1.1-negative",
        &[],
    )
    .await?
}

#[tokio::test]
async fn w3c_sparql_update_parser_positive() -> anyhow::Result<()> {
    run_positive_parser_tests(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
        "update",
    )
    .await?
}

#[tokio::test]
async fn w3c_sparql_update_parser_negative() -> anyhow::Result<()> {
    run_negative_parser_tests(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
        "update-negative",
        &[],
    )
    .await?
}

#[tokio::test]
async fn rdf_fusion_sparql() -> anyhow::Result<()> {
    run_positive_parser_tests(
        "https://codeberg.org/tschwarzinger/rdf-fusion/raw/branch/main/testsuite/rdf-fusion-tests/sparql/manifest.ttl",
        "rdf-fusion-sparql",
    )
    .await?
}

async fn run_positive_parser_tests(
    manifest_url: &'static str,
    snapshot_path: &'static str,
) -> Result<Result<(), Error>, Error> {
    W3CSparqlTestSuiteBuilder::load_manifest(manifest_url)
        .await?
        .with_store_factory(parquet_store_factory(QuadStorageEncodingName::PlainTerm))
        .with_test_factory(Arc::new(move |test, store_factory| {
            let test = test.clone();
            let store_factory = Arc::clone(store_factory);
            Box::pin(async move {
                parser_test_factory(snapshot_path, &test, &store_factory, false).await
            })
        }))
        .ignore_tests_predicate(|test| test.kind.as_str().contains("Negative"))
        .build()
        .await?
        .run()
        .await
        .assert_success();

    Ok(Ok(()))
}

async fn run_negative_parser_tests(
    manifest_url: &'static str,
    snapshot_path: &'static str,
    additional_ignored_tests: &'static [&'static str],
) -> Result<Result<(), Error>, Error> {
    W3CSparqlTestSuiteBuilder::load_manifest(manifest_url)
        .await?
        .with_store_factory(parquet_store_factory(QuadStorageEncodingName::PlainTerm))
        .with_test_factory(Arc::new(move |test, store_factory| {
            let test = test.clone();
            let store_factory = Arc::clone(store_factory);
            Box::pin(async move {
                parser_test_factory(snapshot_path, &test, &store_factory, true).await
            })
        }))
        .ignore_tests_predicate(|test| !test.kind.as_str().contains("Negative"))
        .ignore_tests(additional_ignored_tests.iter().copied())
        .build()
        .await?
        .run()
        .await
        .assert_success();

    Ok(Ok(()))
}

async fn parser_test_factory(
    snapshot_path: &str,
    test: &manifest::Test,
    store_factory: &StoreFactory,
    expect_error: bool,
) -> anyhow::Result<Box<dyn Test>> {
    let runtime = W3CTestRuntime::new(TEST_RUNTIME_ENV.clone());
    let store = store_factory(StoreConfig {
        runtime_env: runtime.fresh_env(),
        default_graphs: vec![],
        named_graphs: vec![],
    })
    .await?;

    let context_view = store.context().create_view();
    for iri in [
        "http://example.org/ns#myFunc",
        "http://example.org/ns#func",
        "http://example.org/ns#func2",
        "http://example.org/name",
        "http://example/function",
    ] {
        let udf = ScalarUDF::new_from_impl(PlaceholderUdf::new(
            iri,
            context_view.encodings().clone(),
        ));
        context_view.functions().register_udf(udf);
    }

    let parser = SparqlParser::new(context_view);

    Ok(Box::new(PlannerSnapshotTest {
        snapshot_path: snapshot_path.to_string(),
        test_data: test.clone(),
        expect_error,
        parser,
        runtime,
    }))
}

pub struct PlannerSnapshotTest {
    snapshot_path: String,
    test_data: manifest::Test,
    expect_error: bool,
    parser: SparqlParser,
    runtime: W3CTestRuntime,
}

#[async_trait::async_trait]
impl Test for PlannerSnapshotTest {
    fn id(&self) -> &str {
        self.test_data.id.as_str()
    }

    fn name(&self) -> Option<&str> {
        self.test_data.name.as_deref()
    }

    async fn run(&self) -> anyhow::Result<TestOutcome> {
        self.execute().await?;
        Ok(TestOutcome::Success)
    }
}

impl PlannerSnapshotTest {
    async fn execute(&self) -> anyhow::Result<()> {
        let query_file = self
            .test_data
            .query
            .as_deref()
            .or(self.test_data.action.as_deref())
            .or(self.test_data.update.as_deref())
            .context("No action found")?;
        let query_str = self.runtime.read_file_to_string(query_file).await?;

        let is_update = self.test_data.kind.as_str().contains("Update");

        let mut settings = Settings::clone_current();
        settings.set_prepend_module_to_snapshot(false);
        settings.set_snapshot_path(&self.snapshot_path);
        settings.add_filter(r"_:[0-9a-f]{16,}", "_:<guid>");
        settings.add_filter(r"BlankNode\(BlankNode\(Anonymous \{[^}]*\}\)\)", "_:<guid>");
        settings.add_filter(r"[0-9a-f]{16,}__type", "<guid>__type");
        // DESCRIBE synthesizes random variables named after an opaque hash (also referenced with a
        // leading `?` in the rendered plan, and as a Debug `Variable { name: "..." }`).
        settings.add_filter(r"\?[0-9a-f]{16,}", "?<guid>");
        settings.add_filter("name: \"[0-9a-f]{16,}\"", "name: \"<guid>\"");
        // Opaque column identifiers appear as standalone names, i.e. they are delimited by
        // whitespace, `=`, `(`, `[` on the left and whitespace, `)`, `]`, `,` on the right. Anchoring
        // to these delimiters makes the match much more restrictive: in particular it no longer
        // matches numeric sub-second fractions of date-time literals (which are bounded by `.`/`-`).
        settings.add_filter(r"([\s\[(=])([0-9a-f]{16,})([\s\]\),])", "$1<guid>$3");
        settings.bind(|| {
            let test_name = if let Some(name) = self.name() {
                if name.ends_with(".rq") || name.ends_with(".ru") {
                    let parent = self
                        .id()
                        .split("/manifest#")
                        .next()
                        .and_then(|p| p.rsplit('/').next())
                        .unwrap_or("");
                    if !parent.is_empty() {
                        sanitize_snapshot_name(&format!("{parent}_{name}"))
                    } else {
                        sanitize_snapshot_name(name)
                    }
                } else {
                    sanitize_snapshot_name(name)
                }
            } else {
                sanitize_snapshot_name(self.id())
            };
            let parser_config = ParserConfig::builder()
                .with_base_iri(Some(Iri::parse(query_file.to_string()).unwrap()))
                .with_now(DateTime::MIN)
                .build();

            if self.expect_error {
                let error_string = if is_update {
                    self.parser
                        .parse_update(&query_str, &parser_config)
                        .map(|_| ())
                        .expect_err(&format!(
                            "Expected an update error but succeeded for: {query_str}",
                        ))
                        .to_string()
                } else {
                    self.parser
                        .parse_query(&query_str, &parser_config)
                        .map(|_| ())
                        .expect_err(&format!(
                            "Expected a query error but succeeded for: {query_str}",
                        ))
                        .to_string()
                };
                assert_snapshot!(test_name, error_string);
            } else {
                let snapshot = if is_update {
                    let result = self
                        .parser
                        .parse_update(&query_str, &parser_config)
                        .map_err(|err| err.to_string());
                    match result {
                        Ok(update) => update.display_list_operations().to_string(),
                        Err(err) => format!("Error: {err}\n"),
                    }
                } else {
                    let result = self
                        .parser
                        .parse_query(&query_str, &parser_config)
                        .map_err(|err| err.to_string());
                    match result {
                        Ok(query) => format!(
                            "Variant: {:?}\n{}",
                            query.variant(),
                            query.logical_plan().display_indent()
                        ),
                        Err(err) => format!("Error: {err}\n"),
                    }
                };
                assert_snapshot!(test_name, snapshot);
            }
        });

        Ok(())
    }
}

/// A placeholder udf for the functions used in the tests that are not available in RDF Fusion.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct PlaceholderUdf {
    name: String,
    signature: Signature,
}

impl PlaceholderUdf {
    pub fn new(iri: &str, encodings: RdfFusionEncodings) -> Self {
        let type_signature = SparqlUDFTypeSignatureBuilder::new()
            .with_supported_encoding(encodings.typed_family().as_ref())
            .with_variadic_arity()
            .build();
        Self {
            name: FunctionName::Custom(rdf_fusion_common::NamedNode::new_unchecked(iri))
                .to_string(),
            signature: Signature::new(type_signature, Volatility::Immutable),
        }
    }
}

impl ScalarUDFImpl for PlaceholderUdf {
    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(
        &self,
        _arg_types: &[DataType],
    ) -> rdf_fusion_common::DFResult<DataType> {
        Ok(PLAIN_TERM_ENCODING.data_type().clone())
    }

    fn invoke_with_args(
        &self,
        _args: ScalarFunctionArgs,
    ) -> rdf_fusion_common::DFResult<ColumnarValue> {
        unimplemented!()
    }
}
