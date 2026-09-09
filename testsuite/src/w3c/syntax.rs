use crate::test::{Test, TestOutcome};
use crate::w3c::custom_functions::register_custom_functions;
use crate::w3c::files::W3CTestRuntime;
use crate::w3c::{StoreConfig, StoreFactory};
use anyhow::{Context, ensure};
use rdf_fusion::common::Iri;
use rdf_fusion_common::DateTime;
use rdf_fusion_sparql_parser::ast::pretty_printable;
use rdf_fusion_sparql_parser::parser::SparqlParser;
use rdf_fusion_sparql_parser::{ParserOptions, parse_query, parse_update};
use std::sync::Arc;

pub struct W3CSparqlSyntaxTest {
    pub id: String,
    pub name: Option<String>,
    pub action_file: String,
    pub is_positive: bool,
    pub is_update: bool,
    pub store_factory: StoreFactory,
    pub runtime: W3CTestRuntime,
}

#[async_trait::async_trait]
impl Test for W3CSparqlSyntaxTest {
    fn id(&self) -> &str {
        &self.id
    }

    fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    async fn run(&self) -> anyhow::Result<TestOutcome> {
        let content = self.runtime.read_file_to_string(&self.action_file).await?;
        let store = (self.store_factory)(StoreConfig {
            runtime_env: self.runtime.fresh_env(),
            default_graphs: vec![],
            named_graphs: vec![],
        })
        .await?;
        let context_view = store.context().create_view();
        register_custom_functions(&context_view);
        let parser_config = ParserOptions::builder()
            .with_base_iri(Iri::parse(self.action_file.clone()).ok())
            .with_now(DateTime::MIN)
            .build();

        let result = if self.is_positive {
            if self.is_update {
                let update =
                    SparqlParser::new(&content, Arc::clone(context_view.functions()))
                        .parse_update()
                        .context("Not able to parse positive update syntax test")?;
                let pretty_str = pretty_printable(&update).to_string();
                parse_update(&context_view, &pretty_str, &parser_config)
                    .map(|_| ())
                    .with_context(|| format!("Failure to deserialize \"{pretty_str}\""))
            } else {
                let query =
                    SparqlParser::new(&content, Arc::clone(context_view.functions()))
                        .parse_query()
                        .context("Not able to parse positive syntax test")?;
                let pretty_str = pretty_printable(&query).to_string();
                parse_query(&context_view, &pretty_str, &parser_config)
                    .map(|_| ())
                    .with_context(|| format!("Failure to deserialize \"{pretty_str}\""))
            }
        } else {
            let res = if self.is_update {
                parse_update(&context_view, &content, &parser_config).map(|_| ())
            } else {
                parse_query(&context_view, &content, &parser_config).map(|_| ())
            };
            ensure!(
                res.is_err(),
                "Negative syntax test {} parsed even if it should not.",
                self.id
            );
            Ok(())
        };

        Ok(match result {
            Ok(_) => TestOutcome::Success,
            Err(e) => TestOutcome::Failed(e),
        })
    }
}
