use anyhow::Context;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::SessionConfig;
use rdf_fusion::encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
use rdf_fusion::execution::RdfFusionContext;
use rdf_fusion::io::{RdfFormat, RdfParser};
use rdf_fusion::logical::{ActiveGraph, RdfFusionLogicalPlanBuilderContext};
use rdf_fusion::model::{
    NamedNode, NamedNodePattern, TermPattern, TriplePattern, Variable,
};
use rdf_fusion::storage::memory::{MemObjectIdMapping, MemQuadStorage};
use std::sync::Arc;

/// This example shows how to use RDF Fusion's query builder for programmatically creating SPARQL
/// queries.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Create a new in-memory instance
    let config = SessionConfig::default();
    let storage = create_storage(&config);
    let engine =
        RdfFusionContext::new(config, RuntimeEnvBuilder::default().build_arc()?, storage);

    // Load data file into storage
    let file = std::fs::File::open("./examples/data/spiderman.ttl")
        .context("Could not find spiderman.ttl")?;
    let reader = RdfParser::from_format(RdfFormat::Turtle);
    let quads = reader
        .rename_blank_nodes()
        .for_reader(file)
        .collect::<Result<Vec<_>, _>>()?;
    engine.storage().extend(quads).await?;
    assert_eq!(engine.len().await?, 7);

    // Build pattern
    let builder = RdfFusionLogicalPlanBuilderContext::new(engine.create_view());
    let pattern = TriplePattern {
        subject: TermPattern::NamedNode(NamedNode::new("http://example.org/#spiderman")?),
        predicate: NamedNodePattern::NamedNode(NamedNode::new(
            "http://www.perceive.net/schemas/relationship/enemyOf",
        )?),
        object: TermPattern::Variable(Variable::new("enemy")?),
    };
    let plan = builder.create_pattern(ActiveGraph::DefaultGraph, None, pattern);

    // Build expression to compute the string representation
    let expr = plan
        .expr_builder_root()
        .variable(Variable::new("enemy")?.as_ref())?
        .str()?
        .build()?;

    // Get string representation of ?enemy, bind it to ?enemy_string, and project.
    let final_plan = plan
        .extend(Variable::new("enemy_string")?, expr)?
        .project(&[Variable::new("enemy_string")?])?
        .build()?;

    // Execute Logical Plan
    let result = engine
        .session_context()
        .execute_logical_plan(final_plan)
        .await?;
    result.show().await?;

    Ok(())
}

/// Creates a new in-memory storage.
fn create_storage(config: &SessionConfig) -> Arc<MemQuadStorage> {
    let mapping = Arc::new(MemObjectIdMapping::default());
    let encoding = Arc::new(ObjectIdEncoding::new(
        Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
    ));
    Arc::new(
        MemQuadStorage::try_new(encoding, config.batch_size())
            .expect("MemObjectIdMapping has 4-byte wide object ids"),
    )
}
