use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{HashSet, exec_datafusion_err};
use datafusion::datasource::TableProvider;
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::execution::SessionState;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::{LogicalPlan, LogicalPlanBuilder, UserDefinedLogicalNode};
use datafusion::optimizer::OptimizerRule;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};
use datafusion::prelude::SessionConfig;
use rdf_fusion::api::RdfFusionContextView;
use rdf_fusion::api::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use rdf_fusion::common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion::common::{GraphName, NamedNode, Quad, StorageError, TermPattern};
use rdf_fusion::encoding::object_id::ObjectIdMapping;
use rdf_fusion::encoding::plain_term::{PlainTermArrayElementBuilder, PlainTermEncoding};
use rdf_fusion::encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion::encoding::{EncodingArray, QuadStorageEncoding};
use rdf_fusion::execution::RdfFusionContext;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::logical::RdfFusionLogicalPlanBuilderContext;
use rdf_fusion::logical::patterns::PatternLoweringRule;
use rdf_fusion::logical::quad_pattern::QuadPatternNode;
use rdf_fusion::store::Store;
use std::sync::Arc;

/// This example shows how to use a custom storage layer for RDF Fusion.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let vec = HashSet::from([
        Quad::new(
            NamedNode::new("http://example.org/#spiderman")?,
            NamedNode::new("http://www.perceive.net/schemas/relationship/enemyOf")?,
            NamedNode::new("http://example.org/#green-goblin")?,
            GraphName::DefaultGraph,
        ),
        Quad::new(
            NamedNode::new("http://example.org/#spiderman")?,
            NamedNode::new("http://www.w3.org/1999/02/22-rdf-syntax-ns#type")?,
            NamedNode::new("http://xmlns.com/foaf/0.1/Person")?,
            GraphName::DefaultGraph,
        ),
    ]);

    let context = RdfFusionContext::new(
        SessionConfig::default(),
        RuntimeEnvBuilder::new().build_arc()?,
        Arc::new(VecQuadStorage(Arc::new(vec))),
        Arc::new(TypedFamilyEncoding::default()),
    );
    let store = Store::new(context);

    // Run SPARQL query.
    let query = "
    BASE <http://example.org/>
    PREFIX foaf: <http://xmlns.com/foaf/0.1/>

    SELECT ?person
    WHERE {
        ?person a foaf:Person .
    }
    ";
    let result = store.query(query).await?;

    // Serialize result
    let mut result_buffer = Vec::new();
    result
        .write(&mut result_buffer, QueryResultsFormat::Csv)
        .await?;
    let result = String::from_utf8(result_buffer)?;

    // Print results.
    println!("Persons:");
    print!("{result}");

    Ok(())
}

/// This is the custom storage layer that we use for this example.
///
/// The database is a simple set of quads that cannot be changed after creating the storage (for
/// the sake of simplicity).
#[derive(Clone)]
struct VecQuadStorage(Arc<HashSet<Quad>>);

#[async_trait]
impl QuadStorage for VecQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        // We use the plain term encoding for the quads.
        QuadStorageEncoding::PlainTerm
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        // We do not have an object ID mapping.
        None
    }

    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        Ok(Arc::new(VecQuadStorageSnapshot {
            quads: Arc::clone(&self.0),
        }))
    }

    async fn begin_transaction(
        &self,
        _session: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError> {
        Err(StorageError::Other(Box::new(exec_datafusion_err!(
            "Transactions are not supported for the VecQuadStorage."
        ))))
    }

    async fn optimize(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }
}

/// A custom planner that plans the quad pattern nodes based on a given [`MemTable`]. We assume that
/// the table has the following schema: (graph, subject, predicate, object).
///
/// Evaluating a pattern will be done in three steps:
/// 1. Create a new logical plan that scans the entire quads table
/// 2. Apply a pattern node
/// 3. PLan the new logical plan and return the result.
///
/// Usually, implementation will tightly couple these three steps to improve performance.
struct VecQuadStoragePlanner(RdfFusionContextView, Arc<MemTable>);

#[async_trait]
impl ExtensionPlanner for VecQuadStoragePlanner {
    async fn plan_extension(
        &self,
        planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        _physical_inputs: &[Arc<dyn ExecutionPlan>],
        session_state: &SessionState,
    ) -> datafusion::common::Result<Option<Arc<dyn ExecutionPlan>>> {
        // Only plan quad pattern nodes.
        let Some(node) = node.as_any().downcast_ref::<QuadPatternNode>() else {
            return Ok(None);
        };

        // 1. Full Scan
        let scan = LogicalPlanBuilder::scan(
            "quads",
            Arc::new(DefaultTableSource::new(
                Arc::clone(&self.1) as Arc<dyn TableProvider>
            )),
            None,
        )?;

        // 2. Apply the pattern
        let builder_context = RdfFusionLogicalPlanBuilderContext::new(self.0.clone());
        let quad_pattern = node.quad_pattern();
        let pattern = builder_context
            .create(Arc::new(scan.build()?))
            .pattern(vec![
                quad_pattern
                    .graph_variable
                    .clone()
                    .map(|v| TermPattern::Variable(v.into())),
                Some(quad_pattern.triple_pattern.subject.clone()),
                Some(quad_pattern.triple_pattern.predicate.clone().into()),
                Some(quad_pattern.triple_pattern.object.clone()),
            ])?
            .build()?;

        // 2.2 Lower pattern (Implementing the pattern is not trivial, therefore, we use existing
        // machinery).
        let pattern_rewriting_rule = PatternLoweringRule::new(self.0.clone());
        let pattern = pattern_rewriting_rule.rewrite(pattern, session_state)?.data;

        // 3. Plan new logical plan
        planner
            .create_physical_plan(&pattern, session_state)
            .await
            .map(Some)
    }
}

/// Represents a snapshot of the [`VecQuadStorage`].
struct VecQuadStorageSnapshot {
    /// A copy of the original quad set.
    quads: Arc<HashSet<Quad>>,
}

impl VecQuadStorageSnapshot {
    /// Creates a [MemTable] for the set. This is a struct from DataFusion that simply emits
    /// references to record batches.
    pub fn create_mem_table(&self) -> MemTable {
        let num_quads = self.quads.len();
        let mut graph_name = PlainTermArrayElementBuilder::with_capacity(num_quads);
        let mut subject = PlainTermArrayElementBuilder::with_capacity(num_quads);
        let mut predicate = PlainTermArrayElementBuilder::with_capacity(num_quads);
        let mut object = PlainTermArrayElementBuilder::with_capacity(num_quads);

        for quad in self.quads.iter() {
            match &quad.graph_name {
                GraphName::NamedNode(node) => {
                    graph_name.append_term(node.as_ref().into())
                }
                GraphName::BlankNode(node) => {
                    graph_name.append_term(node.as_ref().into())
                }
                GraphName::DefaultGraph => graph_name.append_null(),
            }
            subject.append_term(quad.subject.as_ref().into());
            predicate.append_term(quad.predicate.as_ref().into());
            object.append_term(quad.object.as_ref().into());
        }

        let graph_name = graph_name.finish();
        let subject = subject.finish();
        let predicate = predicate.finish();
        let object = object.finish();

        let schema = SchemaRef::new(Schema::new(vec![
            Field::new(COL_GRAPH, PlainTermEncoding::data_type(), true),
            Field::new(COL_SUBJECT, PlainTermEncoding::data_type(), false),
            Field::new(COL_PREDICATE, PlainTermEncoding::data_type(), false),
            Field::new(COL_OBJECT, PlainTermEncoding::data_type(), false),
        ]));

        let record_batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                graph_name.into_array_ref(),
                subject.into_array_ref(),
                predicate.into_array_ref(),
                object.into_array_ref(),
            ],
        )
        .expect("Schema and length always match");

        MemTable::try_new(Arc::clone(&schema), vec![vec![record_batch]])
            .expect("Schemas always match")
    }
}

#[async_trait]
impl QuadStorageSnapshot for VecQuadStorageSnapshot {
    async fn planners(
        &self,
        context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        let mem_table = self.create_mem_table();
        vec![Arc::new(VecQuadStoragePlanner(
            context.clone(),
            Arc::new(mem_table),
        ))]
    }

    async fn named_graphs(
        &self,
        _state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        Err(StorageError::Other(Box::new(exec_datafusion_err!(
            "Obtaining named graphs is not supported for the VecQuadStorage."
        ))))
    }

    async fn len(&self, _state: &SessionState) -> Result<usize, StorageError> {
        Ok(self.quads.len())
    }
}
