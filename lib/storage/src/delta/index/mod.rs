mod snapshot;
mod update;
mod validation;

use crate::delta::error::DeltaQuadStorageError;
use crate::delta::index::update::DeltaStorageQuadIndexUpdater;
use crate::delta::index::validation::validate_index;
use crate::delta::log::DeltaQuadStorageLogChangesetRef;
use crate::index::IndexComponents;
use datafusion::execution::SessionState;
use datafusion::parquet::basic::Encoding;
use datafusion::parquet::file::properties::WriterProperties;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{Add, Transaction};
use deltalake::logstore::LogStoreRef;
use deltalake::operations::create::CreateBuilder;
use deltalake::parquet::basic::{Compression, ZstdLevel};
use deltalake::parquet::file::metadata::SortingColumn;
use deltalake::parquet::file::properties::EnabledStatistics;
use deltalake::parquet::schema::types::ColumnPath;
use deltalake::{DataType as DeltaDataType, DeltaTable, DeltaTableConfig, StructField};
use futures::TryStreamExt;
use rdf_fusion_encoding::QuadStorageEncoding;
use rdf_fusion_model::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_model::{BlankNodeMatchingMode, NamedNodePattern, TermPattern};
pub use snapshot::DeltaQuadStorageIndexSnapshot;
use std::sync::Arc;
use tokio::sync::RwLock;

/// TODO: Make this configurable
const PAGE_ROW_COUNT: usize = 8_192;
/// TODO: Make this configurable
const ROW_GROUP_ROW_COUNT: usize = PAGE_ROW_COUNT * 32;
/// TODO: Make this configurable
const FILE_ROW_COUNT: usize = ROW_GROUP_ROW_COUNT * 32;

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
pub struct DeltaQuadStorageIndex {
    /// The encodings used for storing quads
    storage_encoding: QuadStorageEncoding,
    /// The underlying delta table (guarded for concurrent mutable updates).
    table: Arc<RwLock<IndexTableState>>,
    /// The components of the index.
    components: IndexComponents,
}

impl DeltaQuadStorageIndex {
    /// The application id used to store the log version in delta transactions.
    const APP_ID: &'static str = "rdf_fusion.index_updater";

    /// Tries to create a new [`DeltaQuadStorageIndex`].
    pub async fn try_new(
        storage_encoding: QuadStorageEncoding,
        log_store: LogStoreRef,
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
            .with_log_store(log_store)
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

    /// TODO
    pub async fn try_load(
        storage_encoding: QuadStorageEncoding,
        log_store: LogStoreRef,
        components: IndexComponents,
    ) -> Result<Self, DeltaQuadStorageError> {
        let mut table =
            DeltaTable::new(Arc::clone(&log_store), DeltaTableConfig::default());
        table.load().await?;

        let snapshot = table.snapshot()?.snapshot();
        let active_files = Arc::new(
            snapshot
                .log_data()
                .into_iter()
                .map(|file| {
                    #[allow(deprecated)]
                    file.add_action().clone()
                })
                .collect(),
        );
        let log_transaction_version = snapshot
            .transaction_version(log_store.as_ref(), Self::APP_ID)
            .await?
            .unwrap_or(0) as u64;

        Ok(Self {
            storage_encoding,
            table: Arc::new(RwLock::new(IndexTableState::new(
                table,
                active_files,
                log_transaction_version,
            ))),
            components,
        })
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
    ) -> Result<DeltaQuadStorageIndexSnapshot, DeltaQuadStorageError> {
        let guard = self.table.read().await;
        Ok(DeltaQuadStorageIndexSnapshot::new(
            self.storage_encoding.clone(),
            guard.table.snapshot()?.snapshot().clone(),
            guard.table.log_store(),
            Arc::clone(&guard.active_files),
            self.components,
            guard.log_transaction_version,
        ))
    }

    /// Updates the index to the given `target_version` by applying the changes from the log.
    pub async fn update(
        &self,
        state: &SessionState,
        changeset: DeltaQuadStorageLogChangesetRef,
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

        let last_component =
            ColumnPath::new(vec![self.components.inner()[3].column_name().to_owned()]);
        let mut writer_properties_builder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROW_GROUP_ROW_COUNT))
            .set_data_page_row_count_limit(PAGE_ROW_COUNT)
            .set_bloom_filter_enabled(false)
            .set_column_bloom_filter_enabled(last_component.clone(), true)
            .set_sorting_columns(Some(sorting_columns))
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_column_dictionary_enabled(last_component.clone(), false)
            .set_statistics_enabled(EnabledStatistics::Page);

        if self.storage_encoding.term_type().is_primitive() {
            writer_properties_builder = writer_properties_builder
                .set_column_encoding(last_component, Encoding::PLAIN);
        } else {
            writer_properties_builder = writer_properties_builder
                .set_encoding(Encoding::DELTA_LENGTH_BYTE_ARRAY) // Good for common prefixes
                .set_statistics_truncate_length(Some(256)) // IRIs might be long
                .set_column_index_truncate_length(Some(256)) // IRIs might be long;
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
