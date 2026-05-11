use anyhow::Context;
use rdf_fusion::io::RdfFormat;
use rdf_fusion::logical::{ActiveGraph, RdfFusionLogicalPlanBuilderContext};
use rdf_fusion::model::{
    NamedNode, NamedNodePattern, TermPattern, TriplePattern, Variable,
};
use rdf_fusion::storage::rdf_files::RdfParserOptions;
use rdf_fusion::store::Store;

/// This example shows how to use RDF Fusion's query builder for programmatically creating SPARQL
/// queries.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    // Create a new in-memory instance
    let store = Store::new_in_memory().await;
    let engine = store.context().clone();

    // Load data file into storage
    let file = tokio::fs::File::open("./examples/data/spiderman.ttl")
        .await
        .context("Could not find spiderman.ttl")?;
    store
        .load_from_reader(file, RdfParserOptions::with_format(RdfFormat::Turtle))
        .await?;
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
