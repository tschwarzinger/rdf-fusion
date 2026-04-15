mod snapshot;
mod update;
mod validation;

use crate::delta::error::DeltaQuadStorageError;
use crate::delta::index::update::DeltaStorageQuadIndexUpdater;
use crate::delta::index::validation::validate_index;
use crate::delta::log::changeset::DeltaStorageLogChangesetRef;
use crate::index::IndexComponents;
use datafusion::execution::SessionState;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{Add, Transaction};
use deltalake::operations::create::CreateBuilder;
use deltalake::parquet::file::metadata::SortingColumn;
use deltalake::parquet::file::properties::EnabledStatistics;
use deltalake::parquet::schema::types::ColumnPath;
use deltalake::{DataType as DeltaDataType, DeltaTable, StructField};
use futures::TryStreamExt;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_logical::ActiveGraph;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{
    BlankNodeMatchingMode, NamedNodePattern, QuadComponent, TermPattern, TriplePattern,
};
pub use snapshot::DeltaStorageQuadIndexSnapshot;
use std::sync::Arc;
use tokio::sync::RwLock;

/// The state of the index table.
///
/// As we only support a single writer for now, we can cache the active files in memory.
struct IndexTableState {
    /// The underlying delta table.
    table: DeltaTable,
    /// The active files in the index.
    active_files: Arc<Vec<Add>>,
    /// The log transaction version of the index.
    log_transaction_version: u64,
}

impl IndexTableState {
    /// Creates a new [`IndexTableState`].
    fn new(
        table: DeltaTable,
        active_files: Arc<Vec<Add>>,
        log_transaction_version: u64,
    ) -> Self {
        Self {
            table,
            active_files,
            log_transaction_version,
        }
    }
}

/// Represents a mutable index for the Delta storage.
///
/// An index is a Delta table that stores a full snapshot of the quads at a specific log version.
pub struct DeltaStorageQuadIndex {
    /// The encodings used for storing quads
    storage_encoding: QuadStorageEncoding,
    /// The underlying delta table (guarded for concurrent mutable updates).
    table: Arc<RwLock<IndexTableState>>,
    /// The components of the index.
    components: IndexComponents,
}

impl DeltaStorageQuadIndex {
    /// The application id used to store the log version in delta transactions.
    const APP_ID: &'static str = "rdf_fusion.index_updater";

    /// Tries to create a new [`DeltaStorageQuadIndex`].
    pub async fn try_new(
        storage_encoding: QuadStorageEncoding,
        location: &str,
        components: IndexComponents,
    ) -> Result<Self, DeltaQuadStorageError> {
        let data_type = storage_encoding.term_type().clone();
        let delta_data_type = DeltaDataType::try_from_arrow(&data_type)
            .map_err(|_| DeltaQuadStorageError::UnsupportedArrowType(data_type))?;

        let delta_columns = vec![
            StructField::new(COL_GRAPH, delta_data_type.clone(), true),
            StructField::new(COL_SUBJECT, delta_data_type.clone(), false),
            StructField::new(COL_PREDICATE, delta_data_type.clone(), false),
            StructField::new(COL_OBJECT, delta_data_type, false),
        ];

        let sync_txn = Transaction {
            app_id: Self::APP_ID.to_string(),
            version: 0,
            last_updated: None,
        };
        let commit_props =
            CommitProperties::default().with_application_transaction(sync_txn);

        let table = CreateBuilder::new()
            .with_location(location)
            .with_columns(delta_columns)
            .with_commit_properties(commit_props)
            .with_table_name(format!("Index_{components}"))
            .await?;

        let index = Self {
            storage_encoding,
            table: Arc::new(RwLock::new(IndexTableState::new(
                table,
                Arc::new(vec![]),
                0,
            ))),
            components,
        };

        Ok(index)
    }

    /// Returns a reference to the used [`IndexComponents`].
    pub fn components(&self) -> IndexComponents {
        self.components
    }

    /// Takes a point-in-time snapshot of the index.
    ///
    /// Use this snapshot to read the log version and query the data without risking race conditions
    /// from concurrent updates. We assume that the files of the snapshot will not be deleted by
    /// another process (vacuuming).
    pub async fn snapshot(
        &self,
    ) -> Result<Arc<DeltaStorageQuadIndexSnapshot>, DeltaQuadStorageError> {
        let guard = self.table.read().await;
        Ok(Arc::new(DeltaStorageQuadIndexSnapshot::new(
            self.storage_encoding.clone(),
            guard.table.snapshot()?.snapshot().clone(),
            guard.table.log_store(),
            Arc::clone(&guard.active_files),
            self.components,
            guard.log_transaction_version,
        )))
    }

    /// Computes the scan score for this index given the active graph and the pattern.
    pub fn compute_scan_score(
        &self,
        active_graph: &ActiveGraph,
        pattern: &TriplePattern,
        blank_node_mode: BlankNodeMatchingMode,
    ) -> usize {
        let graph_bound = matches!(
            active_graph,
            ActiveGraph::DefaultGraph | ActiveGraph::Union(_)
        );
        let subject_bound = is_term_bound(pattern.subject.clone(), blank_node_mode);
        let predicate_bound =
            is_named_node_bound(pattern.predicate.clone(), blank_node_mode);
        let object_bound = is_term_bound(pattern.object.clone(), blank_node_mode);

        let mut score = 0;
        for (i, component) in self.components.inner().iter().enumerate() {
            let is_bound = match component {
                QuadComponent::GraphName => graph_bound,
                QuadComponent::Subject => subject_bound,
                QuadComponent::Predicate => predicate_bound,
                QuadComponent::Object => object_bound,
            };

            // We don't stop after finding a non-bound component, because components that are
            // part of the sort order should still exhibit a better clustering, even though the
            // scan is no longer restricted to a slice of the index.
            if is_bound {
                let position = 3 - i;
                score += 1 << position;
            }
        }
        score
    }

    /// Updates the index to the given `target_version` by applying the changes from the log.
    pub async fn update(
        &self,
        state: &SessionState,
        changeset: DeltaStorageLogChangesetRef,
    ) -> Result<(), DeltaQuadStorageError> {
        let updater = DeltaStorageQuadIndexUpdater::new(
            self.snapshot().await?,
            self.table.read().await.table.clone(),
            changeset,
            state.clone(),
            self.create_write_properties_for_update(),
        );

        let (new_table, new_version) = updater.apply_update().await?;
        self.update_table_state(new_table, new_version).await?;

        Ok(())
    }

    /// Validates the index by ensuring that the index
    /// - ... contains no duplicates
    pub async fn validate(
        &self,
        state: &SessionState,
    ) -> Result<(), DeltaQuadStorageError> {
        let snapshot = self.snapshot().await?;
        validate_index(state, &snapshot).await
    }

    /// Sets the new table state.
    async fn update_table_state(
        &self,
        new_table: DeltaTable,
        log_transaction_version: u64,
    ) -> Result<(), DeltaQuadStorageError> {
        let mut table_lock = self.table.write().await;
        let snapshot = new_table.snapshot()?.snapshot().clone();
        let active_files = snapshot
            .file_views(new_table.log_store().as_ref(), None)
            .map_ok(|fv| {
                #[allow(deprecated)]
                fv.add_action()
            })
            .try_collect::<Vec<_>>()
            .await?;

        *table_lock = IndexTableState::new(
            new_table,
            Arc::new(active_files),
            log_transaction_version,
        );
        Ok(())
    }

    /// Creates the Parquet writer properties for the index update.
    fn create_write_properties_for_update(&self) -> WriterProperties {
        let sorting_columns = self
            .components
            .inner()
            .iter()
            .map(|c| SortingColumn {
                column_idx: c.gspo_index() as i32,
                descending: false,
                nulls_first: true,
            })
            .collect();

        let last_component = self.components.inner()[3];
        let mut writer_properties_builder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(8_192 * 16))
            .set_data_page_row_count_limit(8_192)
            .set_bloom_filter_enabled(false)
            .set_sorting_columns(Some(sorting_columns))
            .set_statistics_enabled(EnabledStatistics::Page);

        if self.storage_encoding.term_type().is_primitive() {
            writer_properties_builder = writer_properties_builder
                .set_encoding(Encoding::RLE)
                .set_column_encoding(
                    ColumnPath::new(vec![last_component.column_name().to_owned()]),
                    Encoding::PLAIN,
                );
        }
        writer_properties_builder.build()
    }
}

fn is_term_bound(pattern: TermPattern, mode: BlankNodeMatchingMode) -> bool {
    match pattern {
        TermPattern::NamedNode(_) | TermPattern::Literal(_) => true,
        TermPattern::BlankNode(_) => mode == BlankNodeMatchingMode::Filter,
        TermPattern::Variable(_) => false,
    }
}

fn is_named_node_bound(pattern: NamedNodePattern, _mode: BlankNodeMatchingMode) -> bool {
    match pattern {
        NamedNodePattern::NamedNode(_) => true,
        NamedNodePattern::Variable(_) => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::delta::DeltaQuadStorage;
    use crate::index::IndexComponents;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use deltalake::delta_datafusion::{DeltaScanConfig, DeltaTableProvider};
    use rdf_fusion_encoding::{
        QuadStorageEncoding, QuadStorageEncodingName, quads_to_plain_term_dataframe,
    };
    use rdf_fusion_extensions::storage::QuadStorage;
    use rdf_fusion_model::{GraphName, NamedNode, Quad, TriplePattern, Variable};

    #[tokio::test]
    async fn test_scan_score_fully_bound() {
        let index = create_test_index(IndexComponents::GSPO).await;

        let pattern = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = index.compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 15);
    }

    #[tokio::test]
    async fn test_scan_score_longer_prefixes_score_higher() {
        let index = create_test_index(IndexComponents::GSPO).await;

        let pattern_g = TriplePattern {
            subject: variable_term(),
            predicate: variable_named_node(),
            object: variable_term(),
        };
        let score_g = index.compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_g,
            BlankNodeMatchingMode::Filter,
        );

        let pattern_gs = TriplePattern {
            subject: bound_term(),
            predicate: variable_named_node(),
            object: variable_term(),
        };
        let score_gs = index.compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_gs,
            BlankNodeMatchingMode::Filter,
        );

        let pattern_gsp = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: variable_term(),
        };
        let score_gsp = index.compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_gsp,
            BlankNodeMatchingMode::Filter,
        );

        // Assert that a longer continuous prefix strictly increases the score
        assert!(score_gsp > score_gs);
        assert!(score_gs > score_g);
    }

    #[tokio::test]
    async fn test_scan_score_broken_prefix() {
        let index = create_test_index(IndexComponents::GSPO).await;

        let pattern_broken = TriplePattern {
            subject: variable_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = index.compute_scan_score(
            &ActiveGraph::DefaultGraph,
            &pattern_broken,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 11);
    }

    #[tokio::test]
    async fn test_scan_score_unbound_first_component() {
        let index = create_test_index(IndexComponents::GSPO).await;

        let pattern = TriplePattern {
            subject: bound_term(),
            predicate: bound_named_node(),
            object: bound_term(),
        };

        let score = index.compute_scan_score(
            &ActiveGraph::AllGraphs,
            &pattern,
            BlankNodeMatchingMode::Filter,
        );

        assert_eq!(score, 7);
    }

    #[tokio::test]
    async fn test_index_update_with_only_adds() {
        let config = SessionConfig::new().with_target_partitions(1);
        let session_context = SessionContext::new_with_config(config);
        let storage = DeltaQuadStorage::new_in_memory(
            QuadStorageEncodingName::PlainTerm,
            vec![IndexComponents::GSPO],
            Arc::new(Default::default()),
        )
        .await;

        storage
            .insert(quads_to_plain_term_dataframe(
                &session_context,
                &[
                    Quad::new(
                        NamedNode::new_unchecked("https://my.test/1"),
                        NamedNode::new_unchecked("https://my.test/1"),
                        NamedNode::new_unchecked("https://my.test/1"),
                        GraphName::DefaultGraph,
                    ),
                    Quad::new(
                        NamedNode::new_unchecked("https://my.test/2"),
                        NamedNode::new_unchecked("https://my.test/2"),
                        NamedNode::new_unchecked("https://my.test/2"),
                        GraphName::DefaultGraph,
                    ),
                ],
            ))
            .await
            .unwrap();

        // Update indexes
        let state = session_context.state();
        storage.optimize(&state).await.unwrap();

        let index = storage.indexes()[0].clone();
        assert_quad_count(session_context, index, 2).await;
    }

    async fn create_test_index(components: IndexComponents) -> DeltaStorageQuadIndex {
        let encoding = QuadStorageEncoding::PlainTerm;
        DeltaStorageQuadIndex::try_new(encoding, "memory:///test", components)
            .await
            .expect("Failed to create test index")
    }

    fn bound_term() -> TermPattern {
        NamedNode::new_unchecked("http://example.org/bound").into()
    }

    fn variable_term() -> TermPattern {
        Variable::new_unchecked("v").into()
    }

    fn bound_named_node() -> NamedNodePattern {
        NamedNode::new_unchecked("http://example.org/bound").into()
    }

    fn variable_named_node() -> NamedNodePattern {
        Variable::new_unchecked("v").into()
    }

    async fn assert_quad_count(
        session_context: SessionContext,
        index: Arc<DeltaStorageQuadIndex>,
        expected_count: usize,
    ) {
        let index = index.snapshot().await.unwrap();
        let table_provider = DeltaTableProvider::try_new(
            index.snapshot().clone(),
            index.log_store().clone(),
            DeltaScanConfig::default(),
        )
        .unwrap();
        let count = session_context
            .read_table(Arc::new(table_provider))
            .unwrap()
            .count()
            .await
            .unwrap();
        assert_eq!(count, expected_count);
    }
}
