use crate::delta::error::DeltaQuadStorageError;
use crate::delta::index::DeltaStorageQuadIndex;
use crate::delta::log::{
    DeltaStorageLog, DeltaStorageLogOperation, DeltaStorageLogVersionRange,
};
use crate::delta::objectids::DeltaObjectIdMapping;
use crate::delta::planner::DeltaQuadStoragePlanner;
use crate::delta::scan_plan_builder::DeltaQuadStorageScanPlanBuilder;
use crate::index::IndexComponents;
use async_trait::async_trait;
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::error::DataFusionError;
use datafusion::execution::{SessionState, TaskContext};
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::{ExecutionPlan, execute_stream};
use datafusion::physical_planner::ExtensionPlanner;
use datafusion::prelude::DataFrame;
use futures::StreamExt;
use rdf_fusion_encoding::object_id::{ObjectIdEncoding, ObjectIdMapping};
use rdf_fusion_encoding::plain_term::PLAIN_TERM_ENCODING;
use rdf_fusion_encoding::string::STRING_ENCODING;
use rdf_fusion_encoding::typed_family::TypedFamilyEncodingRef;
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName, TermEncoding};
use rdf_fusion_extensions::RdfFusionContextView;
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_logical::encoding::change::ChangeEncodingNode;
use rdf_fusion_logical::quad_pattern::QuadPattern;
use rdf_fusion_model::sparql::Update;
use rdf_fusion_model::{
    CorruptionError, DFResult, GraphNameRef, NamedOrBlankNode, NamedOrBlankNodeRef,
    StorageError, TermRef,
};
use std::sync::Arc;

/// A quad storage that uses Delta Lake tables for storing quads.
#[derive(Clone)]
pub struct DeltaQuadStorage {
    /// The log that records the changes made to the storage
    log: Arc<DeltaStorageLog>,
    /// The encodings used for storing quads
    storage_encoding: QuadStorageEncoding,
    /// The indexes of the storage
    indexes: Vec<Arc<DeltaStorageQuadIndex>>,
    /// The object id mapping used for encoding object ids, if necessary.
    object_id_mapping: Option<Arc<DeltaObjectIdMapping>>,
}

impl DeltaQuadStorage {
    /// Creates a new [`DeltaQuadStorage`] at the given `base_location`.
    pub async fn new_at_location(
        encoding: QuadStorageEncodingName,
        index_configurations: Vec<IndexComponents>,
        base_location: &str,
        typed_family_encoding: TypedFamilyEncodingRef,
    ) -> Result<Self, DeltaQuadStorageError> {
        let (object_id_mapping, storage_encoding) = match encoding {
            QuadStorageEncodingName::PlainTerm => (None, QuadStorageEncoding::PlainTerm),
            QuadStorageEncodingName::ObjectId => {
                let mapping = Arc::new(
                    DeltaObjectIdMapping::try_new_at_location(
                        &format!("{base_location}/object_ids",),
                        typed_family_encoding,
                    )
                    .await?,
                );
                let encoding = ObjectIdEncoding::new(
                    Arc::clone(&mapping) as Arc<dyn ObjectIdMapping>
                );

                (
                    Some(mapping),
                    QuadStorageEncoding::ObjectId(Arc::new(encoding)),
                )
            }
            QuadStorageEncodingName::String => (None, QuadStorageEncoding::String),
        };

        let log = DeltaStorageLog::try_new_at_location(
            storage_encoding.clone(),
            &format!("{base_location}/log"),
        )
        .await
        .expect("TODO");

        let mut indexes = Vec::new();
        for index in index_configurations {
            let new_index = DeltaStorageQuadIndex::try_new(
                storage_encoding.clone(),
                &format!("{base_location}/{index}"),
                index,
            )
            .await
            .unwrap();
            indexes.push(Arc::new(new_index));
        }

        Ok(Self {
            log: Arc::new(log),
            storage_encoding,
            indexes,
            object_id_mapping,
        })
    }

    /// Creates a new [`DeltaQuadStorage`] with default settings (ObjectId encoding) at the given `base_location`.
    pub async fn new_default_at_location(
        index_configurations: Vec<IndexComponents>,
        base_location: &str,
        typed_family_encoding: TypedFamilyEncodingRef,
    ) -> Result<Self, DeltaQuadStorageError> {
        Self::new_at_location(
            QuadStorageEncodingName::ObjectId,
            index_configurations,
            base_location,
            typed_family_encoding,
        )
        .await
    }

    /// Creates a new [`DeltaQuadStorage`] in memory.
    pub async fn new_in_memory(
        encoding: QuadStorageEncodingName,
        index_configurations: Vec<IndexComponents>,
        typed_family_encoding: TypedFamilyEncodingRef,
    ) -> Self {
        Self::new_at_location(
            encoding,
            index_configurations,
            "memory://",
            typed_family_encoding,
        )
        .await
        .expect("In Memory should always initialize successfully")
    }

    /// Creates a new [`DeltaQuadStorage`] in memory with default settings (ObjectId encoding).
    pub async fn new_default_in_memory(
        index_configurations: Vec<IndexComponents>,
        typed_family_encoding: TypedFamilyEncodingRef,
    ) -> Self {
        Self::new_in_memory(
            QuadStorageEncodingName::ObjectId,
            index_configurations,
            typed_family_encoding,
        )
        .await
    }

    /// Returns the log that records the changes made to the storage.
    pub fn log(&self) -> &Arc<DeltaStorageLog> {
        &self.log
    }

    /// Returns the indexes of the storage.
    pub fn indexes(&self) -> &[Arc<DeltaStorageQuadIndex>] {
        &self.indexes
    }

    /// Returns the encodings used by this storage.
    pub fn storage_encoding(&self) -> &QuadStorageEncoding {
        &self.storage_encoding
    }

    /// Returns the object id mapping used by this storage, if any.
    pub fn delta_object_id_mapping(&self) -> Option<Arc<DeltaObjectIdMapping>> {
        self.object_id_mapping.clone()
    }

    /// Encodes the quads using object ids, if necessary.
    async fn encode_quads_if_necessary(
        &self,
        quads: DataFrame,
    ) -> Result<DataFrame, StorageError> {
        let quads_schema = quads.schema();

        // If schema matches, no encoding is necessary
        if quads_schema == self.storage_encoding.quad_schema().as_ref() {
            return Ok(quads);
        };

        let (state, logical_plan) = quads.into_parts();

        let encoded =
            ChangeEncodingNode::try_new(logical_plan, self.storage_encoding.clone())
                .expect("Data Types checked");
        Ok(DataFrame::new(
            state,
            LogicalPlan::Extension(Extension {
                node: Arc::new(encoded),
            }),
        ))
    }
}

#[async_trait]
impl QuadStorage for DeltaQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        self.storage_encoding.clone()
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdMapping>> {
        self.storage_encoding
            .object_id_encoding()
            .map(|enc| Arc::clone(enc.mapping()))
    }

    async fn planners(
        &self,
        _context: &RdfFusionContextView,
    ) -> Vec<Arc<dyn ExtensionPlanner + Send + Sync>> {
        vec![Arc::new(DeltaQuadStoragePlanner::new(Arc::new(
            self.clone(),
        )))]
    }

    async fn insert(&self, quads: DataFrame) -> Result<Option<usize>, StorageError> {
        let quads = self.encode_quads_if_necessary(quads).await?;
        let (state, logical_plan) = quads.into_parts();
        let quads = DataFrame::new(state.clone(), logical_plan);

        self.log
            .transaction(&state)
            .append_quads(quads)
            .expect("TODO: Error handling")
            .execute()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn remove(&self, quads: DataFrame) -> Result<Option<bool>, StorageError> {
        let quads = self.encode_quads_if_necessary(quads).await?;
        let (state, logical_plan) = quads.into_parts();
        let quads = DataFrame::new(state.clone(), logical_plan);

        self.log
            .transaction(&state)
            .remove_quads(quads)
            .expect("TODO: Error handling")
            .execute()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn insert_named_graph<'a>(
        &self,
        state: &SessionState,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<Option<bool>, StorageError> {
        let graph_batch = self
            .storage_encoding
            .encode_term_scalar(graph_name.into())?;
        self.log
            .transaction(state)
            .append_graph_operation(graph_batch, DeltaStorageLogOperation::CreateGraph)
            .expect("TODO: Error handling")
            .execute()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(None)
    }

    async fn named_graphs(
        &self,
        state: &SessionState,
    ) -> Result<Vec<NamedOrBlankNode>, StorageError> {
        let current_version = self.log.version().await;
        let range = DeltaStorageLogVersionRange::new_unchecked(0, current_version);
        let changeset = self
            .log
            .compute_changeset(state, range)
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        let Some(named_graphs) = changeset.added_named_graphs(state).await? else {
            return Ok(vec![]);
        };

        let mut result = Vec::new();
        let mut stream = execute_stream(named_graphs, state.task_ctx())?;
        while let Some(record_batch) = stream.next().await {
            let record_batch = record_batch?;
            let column = &record_batch.columns()[0];
            let plain_term_array = match &self.storage_encoding {
                QuadStorageEncoding::PlainTerm => {
                    PLAIN_TERM_ENCODING.try_new_array(Arc::clone(column))?
                }
                QuadStorageEncoding::ObjectId(encoding) => {
                    encoding.mapping().decode_array(column)?
                }
                QuadStorageEncoding::String => STRING_ENCODING
                    .try_new_array(Arc::clone(column))?
                    .as_plain_term_array()
                    .map_err(|err| DataFusionError::ArrowError(Box::new(err), None))?,
            };

            let new_named_nodes = plain_term_array
                .iter()
                .map(|term| match term.as_term() {
                    Ok(term) => match term {
                        TermRef::NamedNode(named_node) => {
                            Ok(named_node.to_owned().into())
                        }
                        TermRef::BlankNode(blank_node) => {
                            Ok(blank_node.to_owned().into())
                        }
                        TermRef::Literal(_) => Err(StorageError::Corruption(
                            CorruptionError::new("Named graphs contained null"),
                        )),
                    },
                    Err(_) => Err(StorageError::Corruption(CorruptionError::new(
                        "Named graphs contained null",
                    ))),
                })
                .collect::<Result<Vec<NamedOrBlankNode>, StorageError>>()?;
            result.extend(new_named_nodes);
        }

        Ok(result)
    }

    async fn contains_named_graph<'a>(
        &self,
        state: &SessionState,
        graph_name: NamedOrBlankNodeRef<'a>,
    ) -> Result<bool, StorageError> {
        let graphs = self.named_graphs(state).await?;
        Ok(graphs.iter().any(|g| g.as_ref() == graph_name))
    }

    async fn clear(&self, state: &SessionState) -> Result<(), StorageError> {
        self.log
            .transaction(state)
            .append_general_operation(DeltaStorageLogOperation::ClearDatabase)
            .expect("TODO: Error handling")
            .execute()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(())
    }

    async fn clear_graph<'a>(
        &self,
        state: &SessionState,
        graph_name: GraphNameRef<'a>,
    ) -> Result<(), StorageError> {
        let scalar_value = match graph_name {
            GraphNameRef::NamedNode(nn) => {
                self.storage_encoding.encode_term_scalar(nn.into())?
            }
            GraphNameRef::BlankNode(bn) => {
                self.storage_encoding.encode_term_scalar(bn.into())?
            }
            GraphNameRef::DefaultGraph => {
                let g_type = self.log.schema().field(1).data_type();
                ScalarValue::try_new_null(g_type)?
            }
        };

        self.log
            .transaction(state)
            .append_graph_operation(scalar_value, DeltaStorageLogOperation::ClearGraph)
            .expect("TODO: Error handling")
            .execute()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;

        Ok(())
    }

    async fn drop_named_graph(
        &self,
        state: &SessionState,
        graph_name: NamedOrBlankNodeRef<'_>,
    ) -> Result<Option<bool>, StorageError> {
        let graph_batch = self
            .storage_encoding
            .encode_term_scalar(graph_name.into())?;
        self.log
            .transaction(state)
            .append_graph_operation(graph_batch, DeltaStorageLogOperation::DropGraph)
            .expect("TODO: Error handling")
            .execute()
            .await
            .map_err(|e| StorageError::Other(Box::new(e)))?;
        Ok(Some(true))
    }

    async fn len(&self, state: &SessionState) -> Result<usize, StorageError> {
        let scan_planning_result = DeltaQuadStorageScanPlanBuilder::new(
            state.clone(),
            QuadPattern::for_all_quads(),
            self.encoding(),
        )
        .with_best_index(self.indexes())
        .await?
        .with_changeset_for_log(self.log(), None)
        .await?
        .build()
        .await?;

        let physical_plan = scan_planning_result.scan;
        let count = count_rows(physical_plan, state.task_ctx()).await?;

        return Ok(count);

        /// Attempts to look up the count in the statistics, falling back to an Arrow stream count
        /// if necessary.
        async fn count_rows(
            plan: Arc<dyn ExecutionPlan>,
            task_ctx: Arc<TaskContext>,
        ) -> DFResult<usize> {
            // Fast Path: Check if DataFusion knows the exact answer from metadata
            let stats = plan.partition_statistics(None)?;
            if let Precision::Exact(exact_count) = stats.num_rows {
                return Ok(exact_count);
            }

            let mut total_count = 0;
            let partition_count =
                plan.properties().output_partitioning().partition_count();

            for partition in 0..partition_count {
                let mut stream = plan.execute(partition, Arc::clone(&task_ctx))?;

                while let Some(batch_result) = stream.next().await {
                    let batch = batch_result?;
                    total_count += batch.num_rows();
                }
            }

            Ok(total_count)
        }
    }

    async fn optimize(&self, state: &SessionState) -> Result<(), StorageError> {
        if self.indexes.is_empty() {
            return Ok(());
        }

        let any_index = &self.indexes()[0];
        let snapshot = any_index.snapshot().await?;
        let current_index_version = snapshot.log_transaction_version();
        let current_log_version = self.log.version().await;

        if current_log_version < current_index_version {
            return Err(DeltaQuadStorageError::VersionError(format!(
                "Index is already at version {current_index_version}. Cannot downgrade to version {current_log_version}.",
            )).into());
        }

        if current_log_version == current_index_version {
            return Ok(());
        }

        let version_range = DeltaStorageLogVersionRange::new_unchecked(
            current_index_version,
            current_log_version,
        );
        let changeset = self.log.compute_changeset(state, version_range).await?;

        for index in &self.indexes {
            index
                .update(state, Arc::clone(&changeset))
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
        }

        Ok(())
    }

    async fn validate(&self, state: &SessionState) -> Result<(), StorageError> {
        // TODO: Validate the log

        for index in &self.indexes {
            index
                .validate(state)
                .await
                .map_err(|e| StorageError::Other(Box::new(e)))?;
        }

        Ok(())
    }

    async fn execute_update(
        &self,
        _state: &SessionState,
        _update: &Update,
    ) -> Result<(), StorageError> {
        unimplemented!("Storage layer does not yet support updates")
    }
}
