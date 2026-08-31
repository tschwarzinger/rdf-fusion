use crate::{ParserConfig, SparqlParseError};
use rdf_fusion_common::sparql::{RdfFusionQuery, RdfFusionUpdate};
use rdf_fusion_execution::sparql::error::QueryEvaluationError;
use rdf_fusion_execution::sparql::{DatasetOptions, plan_query, plan_update};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_logical::RdfFusionLogicalPlanBuilderContext;

/// Parses SPARQL queries and updates and rewrites them into [`RdfFusionQuery`]s and
/// [`RdfFusionUpdate`]s.
pub struct SparqlParser {
    context: RdfFusionContextView,
}

impl SparqlParser {
    /// Creates a new [`SparqlParser`].
    pub fn new(context: RdfFusionContextView) -> Self {
        Self { context }
    }

    /// Parses the given SPARQL query and rewrites it into an [`RdfFusionQuery`].
    pub fn parse_query(
        &self,
        query: &str,
        config: &ParserConfig,
    ) -> Result<RdfFusionQuery, SparqlParseError> {
        let parsed = self.parse_with_base_iri(config)?.parse_query(query)?;
        let builder_context =
            RdfFusionLogicalPlanBuilderContext::new(self.context.clone());
        let query = plan_query(
            builder_context,
            parsed,
            None,
            &DatasetOptions::default(),
            config.now(),
        )
        .map_err(plan_creation_error)?;
        Ok(query)
    }

    /// Parses the given SPARQL update and rewrites it into an [`RdfFusionUpdate`].
    pub fn parse_update(
        &self,
        update: &str,
        config: &ParserConfig,
    ) -> Result<RdfFusionUpdate, SparqlParseError> {
        let parsed = self.parse_with_base_iri(config)?.parse_update(update)?;
        let builder_context =
            RdfFusionLogicalPlanBuilderContext::new(self.context.clone());
        let update = plan_update(
            builder_context,
            parsed,
            None,
            &DatasetOptions::default(),
            config.now(),
        )
        .map_err(plan_creation_error)?;
        Ok(update)
    }

    fn parse_with_base_iri(
        &self,
        config: &ParserConfig,
    ) -> Result<rdf_fusion_common::sparql::SparqlParser, SparqlParseError> {
        let mut parser = rdf_fusion_common::sparql::SparqlParser::new();
        if let Some(base_iri) = config.default_base_iri() {
            parser = parser.with_base_iri(base_iri.as_str())?;
        }
        Ok(parser)
    }
}

/// Wraps an error returned by the planner into a [`SparqlParseError::PlanCreation`].
fn plan_creation_error(error: QueryEvaluationError) -> SparqlParseError {
    SparqlParseError::PlanCreation(datafusion::error::DataFusionError::External(
        Box::new(error),
    ))
}
