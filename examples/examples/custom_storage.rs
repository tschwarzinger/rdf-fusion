use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{Field, Schema, SchemaRef};
use datafusion::common::{HashSet, exec_datafusion_err};
use datafusion::datasource::TableProvider;
use datafusion::datasource::{DefaultTableSource, MemTable};
use datafusion::execution::SessionState;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::logical_expr::LogicalPlanBuilder;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use rdf_fusion::common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion::common::{GraphName, NamedNode, Quad, QuadPattern, StorageError};
use rdf_fusion::encoding::object_id::ObjectIdDictionary;
use rdf_fusion::encoding::plain_term::{PlainTermArrayElementBuilder, PlainTermEncoding};
use rdf_fusion::encoding::typed_family::TypedFamilyEncoding;
use rdf_fusion::encoding::{EncodingArray, QuadStorageEncoding};
use rdf_fusion::execution::RdfFusionContext;
use rdf_fusion::execution::results::QueryResultsFormat;
use rdf_fusion::extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use rdf_fusion::logical::quad_pattern::compute_quad_pattern_filters;
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

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdDictionary>> {
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
            object.append_term(quad.object.as_ref());
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
    async fn scan_quad_pattern(
        &self,
        pattern: &QuadPattern,
        projection: Option<Vec<usize>>,
        session_state: &SessionState,
    ) -> Result<Arc<dyn ExecutionPlan>, StorageError> {
        // 1. Full scan of the quads table.
        let mem_table = self.create_mem_table();
        let scan = LogicalPlanBuilder::scan(
            "quads",
            Arc::new(DefaultTableSource::new(
                Arc::new(mem_table) as Arc<dyn TableProvider>
            )),
            None,
        )?;

        // 2. Apply the pattern as filters over the scan.
        let filters =
            compute_quad_pattern_filters(pattern, &QuadStorageEncoding::PlainTerm)
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
        let mut builder = scan;
        if !filters.is_empty() {
            let filter = filters
                .into_iter()
                .reduce(|acc, expr| acc.and(expr))
                .expect("filters is not empty");
            builder = builder.filter(filter)?;
        }

        // 3. Project the matched quads to the pattern's output columns.
        let full_projections: Vec<_> = pattern
            .compute_projection()
            .into_iter()
            .map(|(expr, name)| expr.alias(name))
            .collect();
        let selected = match &projection {
            Some(indices) => indices
                .iter()
                .map(|&idx| full_projections[idx].clone())
                .collect::<Vec<_>>(),
            None => full_projections,
        };
        let plan = builder.project(selected)?.build()?;

        // 4. Plan the resulting logical plan through DataFusion.
        session_state
            .create_physical_plan(&plan)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))
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
