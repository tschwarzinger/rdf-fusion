use rdf_fusion_common::DateTime;
use rdf_fusion_execution::sparql::{QueryOptions, UpdateOptions};
use rdf_fusion_sparql_parser::ParserOptionsBuilder;

pub fn query_options_to_parser(query_options: &QueryOptions) -> ParserOptionsBuilder {
    ParserOptionsBuilder::default()
        .with_base_iri(query_options.base_iri.clone())
        .with_default_dataset(Some(query_options.dataset.as_query_dataset()))
        .with_output_encoding_name(query_options.output_encoding_name)
        .with_now(query_options.now.unwrap_or_else(DateTime::now))
}

pub fn update_options_to_parser(query_options: &UpdateOptions) -> ParserOptionsBuilder {
    ParserOptionsBuilder::default()
        .with_default_dataset(Some(query_options.dataset.as_query_dataset()))
        .with_now(query_options.now.unwrap_or_else(DateTime::now))
}
