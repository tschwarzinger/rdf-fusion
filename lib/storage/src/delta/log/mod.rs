mod changeset;
mod changeset_eager;
mod compute_log_changes;

pub(crate) use changeset::*;
pub(crate) use changeset_eager::*;
pub(crate) use compute_log_changes::*;

use crate::delta::error::DeltaQuadStorageError;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::Session;
use datafusion::execution::SessionState;
use datafusion::optimizer::OptimizerConfig;
use datafusion::physical_expr::LexOrdering;
use datafusion::physical_expr::PhysicalSortExpr;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_optimizer::sanity_checker::SanityCheckPlan;
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{ExecutionPlan, ExecutionPlanProperties, execute_stream};
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::operations::create::CreateBuilder;
use deltalake::{DataType as DeltaDataType, DeltaTable, StructField, TableProperty};
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use std::fmt::{Debug, Display, Formatter};
use std::sync::Arc;
use tokio::sync::RwLock;

/// The column of the added column
pub(crate) const COL_OPERATION: &str = "operation";

/// The column of the delta commit version
pub(crate) const COL_COMMIT_VERSION: &str = "_commit_version";

/// A syntactically valid range of [`DeltaStorageLog`] version numbers, guaranteeing that the ending
/// version is not before the starting version.
///
/// This does not guarantee that the versions exist in the underlying table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct DeltaStorageLogVersionRange(u64, u64);

impl DeltaStorageLogVersionRange {
    /// Creates a new [`DeltaStorageLogVersionRange`] if the ending version is greater than the
    /// starting version. Returns [`None`] otherwise.
    pub fn try_new(starting_version: u64, ending_version: u64) -> Option<Self> {
        if ending_version <= starting_version {
            return None;
        }
        Some(Self(starting_version, ending_version))
    }

    /// Creates a new [`DeltaStorageLogVersionRange`] without checking the invariants.
    pub fn new_unchecked(starting_version: u64, ending_version: u64) -> Self {
        Self(starting_version, ending_version)
    }

    /// Returns the starting version of the range.
    pub fn starting_version(&self) -> u64 {
        self.0
    }

    /// Returns the ending version of the range.
    pub fn ending_version(&self) -> u64 {
        self.1
    }
}

impl Display for DeltaStorageLogVersionRange {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}..{}", self.0, self.1)
    }
}

/// Represents a deletion or addition operation in the log.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum DeltaStorageLogOperation {
    /// Drops a single graph.
    ///
    /// The graph column either is null (DROP the default graph) or set (DROP a named graph). The
    /// other columns are null.
    DropGraph,
    /// Clears all quads from a single graph.
    ///
    /// The graph column either is null (CLEAR the default graph) or set (CLEAR a named graph). The
    /// other columns are null.
    ClearGraph,
    /// Adds a graph to the database.
    ///
    /// The graph column should be non-null.
    CreateGraph,
    /// Removes a quad from the database.
    ///
    /// All columns should be non-null except for the graph column if the quad is in the default
    /// graph.
    RemoveQuad,
    /// Adds a quad to the database.
    ///
    /// All columns should be non-null except for the graph column if the quad is in the default
    /// graph.
    AddQuad,
}

impl DeltaStorageLogOperation {
    /// Return the encoded value of the operation. This is stored in the delta table.
    ///
    /// This uses an `i8` (as opposed to an `u8`) because of Delta's byte type.
    pub const fn as_stored(self) -> i8 {
        match self {
            // Graph Operations
            DeltaStorageLogOperation::DropGraph => 10,
            DeltaStorageLogOperation::ClearGraph => 11,
            DeltaStorageLogOperation::CreateGraph => 12,
            // Quad Operations
            DeltaStorageLogOperation::RemoveQuad => 20,
            DeltaStorageLogOperation::AddQuad => 21,
        }
    }

    /// Returns the operation from its stored value. Returns [`None`] if the value is not a valid
    /// operation.
    pub fn from_stored(value: i8) -> Option<Self> {
        match value {
            10 => Some(DeltaStorageLogOperation::DropGraph),
            11 => Some(DeltaStorageLogOperation::ClearGraph),
            12 => Some(DeltaStorageLogOperation::CreateGraph),
            20 => Some(DeltaStorageLogOperation::RemoveQuad),
            21 => Some(DeltaStorageLogOperation::AddQuad),
            _ => None,
        }
    }

    /// Returns true if the operation is a graph-level operation.
    pub fn is_graph_operation(self) -> bool {
        matches!(
            self,
            DeltaStorageLogOperation::DropGraph
                | DeltaStorageLogOperation::CreateGraph
                | DeltaStorageLogOperation::ClearGraph
        )
    }
}

/// Implements an operations log based on a delta table. The log is append-only.
///
/// This is the entry point for new transactions on the RDF store. Each operation simply inserts the
/// updates to the log. A single version in the log table therefore represents a single transaction.
///
/// The log has the following columns:
/// | operation | graph      | subject    | predicate    | object    |
/// |-----------|------------|------------|--------------|-----------|
/// | 21        |            | <subject1> | <predicate1> | <object1> |
/// | 21        | <my-graph> | <subject1> | <predicate1> | <object1> |
///
/// For further information on the supported operations and the constraints applied to the other
/// columns, see the [`DeltaStorageLogOperation`] enum.
///
/// # Handling Duplicates
///
/// The log *does not special-case* inserting duplicates or removing quads does not actually exist
/// in the database. The log simply appends another transaction that contains the duplicated insert
/// (or remove respectively). When interpreting the log, the other system parts must be aware of
/// this circumstance and adjust their alogorithms accordingly. For example, considering the index
/// updater, it might be that the log contains the removal of a quad that is not in the index, as
/// the quad has never existed in the database. Instead of returning an error, the index updater
/// should simply ignore the remove operation.
///
/// # Further Implicatations
///
/// As a result of this approach, operations that mutate the database usually do not know how many
/// quads (or graphs) are affected by the operation. For methods on the [`QuadStorage`] trait that
/// can return this information (e.g., [`QuadStorage::extend`]), this storage implementation must
/// return [`None`].
///
/// [`QuadStorage`]: rdf_fusion_extensions::storage::QuadStorage
pub struct DeltaStorageLog {
    /// The underlying delta table.
    table: Arc<RwLock<DeltaTable>>,
    /// The schema of the delta table.
    table_schema: SchemaRef,
}

impl Debug for DeltaStorageLog {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DeltaStorageLog")
            .field("table", &self.table)
            .field("schema", &self.table_schema)
            .finish()
    }
}

impl DeltaStorageLog {
    /// Tries to create a new [`DeltaStorageLog`] ensuring that the given encoding exists.
    pub async fn try_new_at_location(
        quad_storage_encoding: QuadStorageEncoding,
        location: &str,
    ) -> Result<Self, DeltaQuadStorageError> {
        let data_type = quad_storage_encoding.term_type().clone();
        let delta_data_type =
            DeltaDataType::try_from_arrow(&data_type).map_err(|_| {
                DeltaQuadStorageError::UnsupportedArrowType(data_type.clone())
            })?;

        let delta_columns = vec![
            StructField::new(COL_OPERATION, DeltaDataType::BYTE, false),
            StructField::new(COL_GRAPH, delta_data_type.clone(), true),
            StructField::new(COL_SUBJECT, delta_data_type.clone(), true),
            StructField::new(COL_PREDICATE, delta_data_type.clone(), true),
            StructField::new(COL_OBJECT, delta_data_type, true),
        ];

        let table = CreateBuilder::new()
            .with_location(location)
            .with_columns(delta_columns)
            .with_configuration_property(TableProperty::AppendOnly, Some("true"))
            .with_configuration_property(
                TableProperty::EnableChangeDataFeed,
                Some("true"),
            )
            .await?;

        let schema = Arc::new(Schema::new(vec![
            Field::new(COL_OPERATION, DataType::Int8, false),
            Field::new(COL_GRAPH, data_type.clone(), true),
            Field::new(COL_SUBJECT, data_type.clone(), true),
            Field::new(COL_PREDICATE, data_type.clone(), true),
            Field::new(COL_OBJECT, data_type, true),
        ]));
        Ok(Self {
            table: Arc::new(RwLock::new(table)),
            table_schema: schema,
        })
    }

    /// Returns the schema of the delta table.
    pub fn schema(&self) -> &SchemaRef {
        &self.table_schema
    }

    /// Returns the current version of the delta table.
    pub async fn version(&self) -> u64 {
        self.table
            .read()
            .await
            .version()
            .expect("There should always be a commit in the loaded table")
    }

    /// Returns the underlying delta table.
    pub fn table(&self) -> &Arc<RwLock<DeltaTable>> {
        &self.table
    }

    /// Computes the difference between two versions of the log.
    pub async fn compute_changeset(
        &self,
        state: &SessionState,
        version_range: DeltaStorageLogVersionRange,
    ) -> Result<DeltaQuadStorageLogChangesetRef, DeltaQuadStorageError> {
        let table = self.table.read().await.clone();
        let cdf_scan = query_cdf(state, version_range, table).await?;
        let current_plan = Self::create_changeset_plan(state, cdf_scan)?;

        let stream = execute_stream(current_plan, state.task_ctx())?;
        return Ok(Arc::new(
            EagerDeltaQuadStorageChangeset::partition_operations(
                state,
                version_range,
                stream,
            )
            .await?,
        ));

        /// Returns a query plan that sorts the changes based on the transaction version and the
        /// operations (inserts precede deletes). This allows iterating once over the result of this
        /// query plan to build the delta.
        ///
        /// This function ensures that only a single partition is used for the output. This is
        /// currently required by [`ComputeLogChangesetExec`].
        pub async fn query_cdf(
            session: &dyn Session,
            version_range: DeltaStorageLogVersionRange,
            table: DeltaTable,
        ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
            let cdf_scan = table
                .scan_cdf()
                .with_starting_version(version_range.starting_version())
                .with_ending_version(version_range.ending_version())
                .build(session, None)
                .await?;

            let cdf_scan = if cdf_scan.output_partitioning().partition_count() == 0 {
                Arc::new(EmptyExec::new(cdf_scan.schema())) as Arc<dyn ExecutionPlan>
            } else if cdf_scan.output_partitioning().partition_count() > 1 {
                Arc::new(CoalescePartitionsExec::new(cdf_scan)) as Arc<dyn ExecutionPlan>
            } else {
                cdf_scan
            };

            let sort_exprs = vec![PhysicalSortExpr {
                expr: datafusion::physical_expr::expressions::col(
                    COL_COMMIT_VERSION,
                    cdf_scan.schema().as_ref(),
                )
                .expect("Corruption error"),
                options: SortOptions::default().asc(),
            }];

            Ok(Arc::new(SortExec::new(
                LexOrdering::new(sort_exprs).expect("Valid sort expressions"),
                cdf_scan,
            )))
        }
    }

    /// Creates the [`ComputeLogChangesetExec`] plan and runs some optimizations.
    fn create_changeset_plan(
        state: &SessionState,
        cdf_scan: Arc<dyn ExecutionPlan>,
    ) -> Result<Arc<dyn ExecutionPlan>, DeltaQuadStorageError> {
        let last_change_per_quad =
            ComputeLogChangesetExec::try_new(cdf_scan).expect("Valid CDF");

        let rules =
            vec![Arc::new(SanityCheckPlan::new()) as Arc<dyn PhysicalOptimizerRule>];

        let mut current_plan = Arc::new(last_change_per_quad) as Arc<dyn ExecutionPlan>;
        for rule in rules {
            current_plan = rule
                .optimize(current_plan, state.options().as_ref())
                .unwrap();
        }
        Ok(current_plan)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::DeltaQuadStorage;
    use crate::delta::log::changeset::DeltaQuadStorageLogChangeset;
    use datafusion::arrow::array::{NullArray, RecordBatch};
    use datafusion::arrow::datatypes::{Field, Schema};
    use datafusion::dataframe::DataFrame;
    use datafusion::physical_plan::collect;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use deltalake::arrow::util::pretty::pretty_format_batches;
    use deltalake::delta_datafusion::DeltaTableProvider;
    use insta::assert_snapshot;
    use rdf_fusion_encoding::plain_term::{
        PLAIN_TERM_ENCODING, PlainTermArrayElementBuilder,
    };
    use rdf_fusion_encoding::typed_family::TypedFamilyEncoding;
    use rdf_fusion_encoding::{EncodingArray, QuadStorageEncodingName, TermEncoding};
    use rdf_fusion_extensions::storage::QuadStorage;
    use rdf_fusion_model::NamedNodeRef;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_append_quads_plain_term() {
        let session = create_session();
        let storage = create_storage().await;
        let transaction = storage.begin_transaction(&session.state()).await.unwrap();

        let a_quads = create_plain_term_quads_with_postfix(&session, "A");
        let b_quads = create_plain_term_quads_with_postfix(&session, "B");

        transaction.insert(a_quads).await.unwrap();
        transaction.insert(b_quads).await.unwrap();
        transaction.commit().await.unwrap();

        let result = collect_table_snapshot(storage.log()).await;
        assert_snapshot!(result, @"
        +-----------+-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        | operation | graph | subject                                                               | predicate                                                             | object                                                                |
        +-----------+-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        | 21        |       | {term_type: 0, value: https://my.com/sA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/pA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/oA, data_type: , language_tag: } |
        | 21        |       | {term_type: 0, value: https://my.com/sB, data_type: , language_tag: } | {term_type: 0, value: https://my.com/pB, data_type: , language_tag: } | {term_type: 0, value: https://my.com/oB, data_type: , language_tag: } |
        +-----------+-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        ");
    }

    #[tokio::test]
    async fn test_append_quads_with_wrong_schema() {
        let session = create_session();
        let storage = create_storage().await;
        let transaction = storage.begin_transaction(&session.state()).await.unwrap();

        let quads = create_data_frame_with_wrong_schema(&session);
        let error = transaction.insert(quads).await.unwrap_err();

        assert_eq!(
            error.to_string(),
            "The given stream has an invalid schema. Found schema: Field { \"SomeCol\": nullable Null }"
        );

        /// Creates a new [`DataFrame`] with a wrong schema.
        fn create_data_frame_with_wrong_schema(
            session_context: &SessionContext,
        ) -> DataFrame {
            let schema = Arc::new(Schema::new(vec![Field::new(
                "SomeCol",
                DataType::Null,
                true,
            )]));
            let batch = RecordBatch::try_new(
                Arc::clone(&schema),
                vec![Arc::new(NullArray::new(1))],
            )
            .unwrap();

            session_context.read_batch(batch).unwrap()
        }
    }

    #[tokio::test]
    async fn test_remove_quads_plain_term() -> Result<(), DeltaQuadStorageError> {
        let session = create_session();
        let storage = create_storage().await;
        let transaction = storage.begin_transaction(&session.state()).await.unwrap();

        let quads_to_remove = create_plain_term_quads_with_postfix(&session, "A");
        transaction.remove(quads_to_remove).await?;
        transaction.commit().await?;

        let result = collect_table_snapshot(storage.log()).await;
        assert_snapshot!(result, @"
        +-----------+-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        | operation | graph | subject                                                               | predicate                                                             | object                                                                |
        +-----------+-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        | 20        |       | {term_type: 0, value: https://my.com/sA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/pA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/oA, data_type: , language_tag: } |
        +-----------+-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_compute_changeset_with_add_changes() -> Result<(), DeltaQuadStorageError>
    {
        let session = create_session();
        let storage = create_storage().await;
        let transaction = storage.begin_transaction(&session.state()).await.unwrap();

        let a_quads = create_plain_term_quads_with_postfix(&session, "A");
        let b_quads = create_plain_term_quads_with_postfix(&session, "B");

        transaction.insert(a_quads).await?;
        transaction.insert(b_quads).await?;
        transaction.commit().await?;

        let range = DeltaStorageLogVersionRange(0, 2);
        let result = storage
            .log()
            .compute_changeset(&session.state(), range)
            .await?;

        assert_snapshot!(
            print_added_quads(&session.state(), result.as_ref()).await,
            @"
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        | graph | subject                                                               | predicate                                                             | object                                                                |
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        |       | {term_type: 0, value: https://my.com/sA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/pA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/oA, data_type: , language_tag: } |
        |       | {term_type: 0, value: https://my.com/sB, data_type: , language_tag: } | {term_type: 0, value: https://my.com/pB, data_type: , language_tag: } | {term_type: 0, value: https://my.com/oB, data_type: , language_tag: } |
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_compute_changeset_with_duplicate_add()
    -> Result<(), DeltaQuadStorageError> {
        let session = create_session();
        let storage = create_storage().await;
        let transaction = storage.begin_transaction(&session.state()).await.unwrap();

        let a_quads = create_plain_term_quads_with_postfix(&session, "A");

        transaction.insert(a_quads.clone()).await?;
        transaction.insert(a_quads).await?;
        transaction.commit().await?;

        let range = DeltaStorageLogVersionRange(0, 2);
        let result = storage
            .log()
            .compute_changeset(&session.state(), range)
            .await?;

        assert_snapshot!(
            print_added_quads(&session.state(), result.as_ref()).await,
            @"
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        | graph | subject                                                               | predicate                                                             | object                                                                |
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        |       | {term_type: 0, value: https://my.com/sA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/pA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/oA, data_type: , language_tag: } |
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        ");
        Ok(())
    }

    #[tokio::test]
    async fn test_compute_changeset_with_add_and_then_remove()
    -> Result<(), DeltaQuadStorageError> {
        let session = create_session();
        let storage = create_storage().await;
        let transaction = storage
            .begin_transaction(&session.state())
            .await
            .map_err(|e| DeltaQuadStorageError::from(e.to_string()))?;

        let a_quads = create_plain_term_quads_with_postfix(&session, "A");

        transaction.insert(a_quads.clone()).await?;
        transaction.remove(a_quads).await?;
        transaction.commit().await?;

        let range = DeltaStorageLogVersionRange(0, 2);
        let result = storage
            .log()
            .compute_changeset(&session.state(), range)
            .await?;

        assert_snapshot!(
            print_removed_quads(&session.state(), result.as_ref()).await,
            @"
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        | graph | subject                                                               | predicate                                                             | object                                                                |
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        |       | {term_type: 0, value: https://my.com/sA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/pA, data_type: , language_tag: } | {term_type: 0, value: https://my.com/oA, data_type: , language_tag: } |
        +-------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+-----------------------------------------------------------------------+
        ");
        Ok(())
    }

    async fn print_removed_quads(
        state: &SessionState,
        changeset: &dyn DeltaQuadStorageLogChangeset,
    ) -> String {
        let plan = changeset
            .removed_quads(state)
            .await
            .expect("Failed to obtain removals")
            .expect("Removals are empty");
        let batches = collect(plan, state.task_ctx()).await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }

    /// Creates a new session context for the test
    fn create_session() -> SessionContext {
        let options = SessionConfig::default().with_target_partitions(1);
        SessionContext::new_with_config(options)
    }

    /// Helper: Create the Delta Storage
    async fn create_storage() -> DeltaQuadStorage {
        DeltaQuadStorage::new_in_memory(
            QuadStorageEncodingName::PlainTerm,
            vec![],
            Arc::new(TypedFamilyEncoding::default()),
        )
        .await
    }

    /// Generate a mocked stream of Quads with a postfix to make the quad unique
    fn create_plain_term_quads_with_postfix(
        session_context: &SessionContext,
        postfix: &str,
    ) -> DataFrame {
        let data_type = PLAIN_TERM_ENCODING.data_type().clone();
        let schema = Arc::new(Schema::new(vec![
            Field::new(COL_GRAPH, data_type.clone(), true),
            Field::new(COL_SUBJECT, data_type.clone(), true),
            Field::new(COL_PREDICATE, data_type.clone(), true),
            Field::new(COL_OBJECT, data_type, true),
        ]));
        // Initialize builders for each column
        let mut graph_builder = PlainTermArrayElementBuilder::new();
        let mut subject_builder = PlainTermArrayElementBuilder::new();
        let mut predicate_builder = PlainTermArrayElementBuilder::new();
        let mut object_builder = PlainTermArrayElementBuilder::new();

        graph_builder.append_null();
        subject_builder.append_named_node(NamedNodeRef::new_unchecked(&format!(
            "https://my.com/s{postfix}"
        )));
        predicate_builder.append_named_node(NamedNodeRef::new_unchecked(&format!(
            "https://my.com/p{postfix}"
        )));
        object_builder.append_named_node(NamedNodeRef::new_unchecked(&format!(
            "https://my.com/o{postfix}"
        )));

        // Finish builders into Arrow Arrays
        let graph_array = graph_builder.finish().into_array_ref();
        let subject_array = subject_builder.finish().into_array_ref();
        let predicate_array = predicate_builder.finish().into_array_ref();
        let object_array = object_builder.finish().into_array_ref();

        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(graph_array),
                Arc::new(subject_array),
                Arc::new(predicate_array),
                Arc::new(object_array),
            ],
        )
        .unwrap();

        session_context.read_batch(batch).unwrap()
    }

    /// Helper: Read the delta table and return a formatted snapshot string
    async fn collect_table_snapshot(log: &DeltaStorageLog) -> String {
        let ctx = SessionContext::new();

        // Lock the table properly to read its state
        let table_lock = log.table.read().await;

        let provider = DeltaTableProvider::try_new(
            table_lock.snapshot().unwrap().snapshot().clone(),
            table_lock.log_store(),
            Default::default(),
        )
        .unwrap();

        let df = ctx.read_table(Arc::new(provider)).unwrap();
        let results = df.collect().await.unwrap();

        pretty_format_batches(&results).unwrap().to_string()
    }

    async fn print_added_quads(
        state: &SessionState,
        changeset: &dyn DeltaQuadStorageLogChangeset,
    ) -> String {
        let plan = changeset
            .added_quads(state)
            .await
            .expect("Failed to obtain additions")
            .expect("Additions are empty");
        let batches = collect(plan, state.task_ctx()).await.unwrap();
        pretty_format_batches(&batches).unwrap().to_string()
    }
}
