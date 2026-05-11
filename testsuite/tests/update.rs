#![cfg(test)]

use anyhow::Result;
use rdf_fusion::encoding::QuadStorageEncodingName;
use rdf_fusion::execution::RdfFusionContextBuilder;
use rdf_fusion::storage::delta::DeltaQuadStorageBuilder;
use rdf_fusion::store::Store;
use rdf_fusion_testsuite::w3c::files::W3CTestRuntime;
use rdf_fusion_testsuite::w3c::utils::W3CTestUtils;
use rdf_fusion_testsuite::w3c::{StoreFactory, W3CSparqlTestSuiteBuilder};
use std::sync::Arc;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_update_testsuite_plain_term() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
    )
    .await?
    .with_store_factory(plain_term_store_factory())
    .build()
    .await?
    .run()
    .await
    .assert_success();

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn w3c_sparql11_update_testsuite_object_id() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
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
async fn w3c_sparql11_update_testsuite_string() -> Result<()> {
    W3CSparqlTestSuiteBuilder::load_manifest(
        "https://w3c.github.io/rdf-tests/sparql/sparql11/manifest-sparql11-update.ttl",
    )
    .await?
    .with_store_factory(string_store_factory())
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
    Arc::new(|store_config| {
        Box::pin(async move {
            let delta_storage = DeltaQuadStorageBuilder::new()
                .with_encoding(QuadStorageEncodingName::PlainTerm)
                .build()
                .await?;

            let context = RdfFusionContextBuilder::new(Arc::new(delta_storage))
                .with_runtime_env(Some(store_config.runtime_env))
                .with_single_partition_session_config()
                .build()?;
            let store = Store::new(context);

            let utils = W3CTestUtils::new(W3CTestRuntime::new(Arc::clone(
                &store.context().runtime_env,
            )));
            for (name, source) in store_config.default_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }
            for (name, source) in store_config.named_graphs {
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
    Arc::new(|store_config| {
        Box::pin(async move {
            let delta_storage = DeltaQuadStorageBuilder::new()
                .with_encoding(QuadStorageEncodingName::String)
                .build()
                .await?;

            let context = RdfFusionContextBuilder::new(Arc::new(delta_storage))
                .with_runtime_env(Some(store_config.runtime_env))
                .with_single_partition_session_config()
                .build()?;
            let store = Store::new(context);

            let utils = W3CTestUtils::new(W3CTestRuntime::new(Arc::clone(
                &store.context().runtime_env,
            )));
            for (name, source) in store_config.default_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }
            for (name, source) in store_config.named_graphs {
                utils
                    .load_to_store_from_source(&source, &store, name)
                    .await?;
            }

            Ok(store)
        })
    })
}
