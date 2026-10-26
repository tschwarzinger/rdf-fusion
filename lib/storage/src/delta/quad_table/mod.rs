mod snapshot;
mod update;
mod validation;

use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::log::DeltaQuadsStorageLogChangesetRef;
use crate::delta::quad_table::update::DeltaStorageQuadTableUpdater;
use crate::delta::quad_table::validation::validate_quad_table;
use crate::parquet::{ParquetMetadataFetcher, max_concurrent_files};
use crate::quad_tables::QuadTableName;
use chrono::DateTime;
use datafusion::execution::SessionState;
use deltalake::kernel::engine::arrow_conversion::TryFromArrow;
use deltalake::kernel::transaction::CommitProperties;
use deltalake::kernel::{Add, Transaction};
use deltalake::logstore::LogStoreRef;
use deltalake::operations::create::CreateBuilder;
use deltalake::{DataType as DeltaDataType, DeltaTable, StructField, TableProperty};
use futures::TryStreamExt;
use object_store::ObjectMeta;
use object_store::path::Path;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_encoding::QuadStorageEncoding;
pub use snapshot::DeltaQuadsQuadTableSnapshot;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// The state of the quad_table table.
///
/// As we only support a single writer for now, we can cache the active files in memory.
struct QuadTableState {
    /// The underlying delta table.
    table: DeltaTable,
    /// The active files in the quad_table.
    active_files: Arc<Vec<Add>>,
    /// The log transaction version of the quad_table.
    log_transaction_version: u64,
}

impl QuadTableState {
    /// Creates a new [`QuadTableState`].
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

/// Represents a mutable quad_table for the Delta storage.
///
/// An quad_table is a Delta table that stores a full snapshot of the quads at a specific log version.
pub struct DeltaQuadsQuadTable {
    /// The encodings used for storing quads
    storage_encoding: QuadStorageEncoding,
    /// The underlying delta table (guarded for concurrent mutable updates).
    table: Arc<RwLock<QuadTableState>>,
    /// The components of the quad_table.
    components: QuadTableName,
}

impl DeltaQuadsQuadTable {
    /// The application id used to store the log version in delta transactions.
    const APP_ID: &'static str = "rdf_fusion.quad_table_updater";

    /// Tries to create a new [`DeltaQuadsQuadTable`].
    pub async fn try_new(
        storage_encoding: QuadStorageEncoding,
        log_store: LogStoreRef,
        components: QuadTableName,
    ) -> Result<Self, DeltaQuadsStorageError> {
        let data_type = storage_encoding.term_type().clone();
        let delta_data_type = DeltaDataType::try_from_arrow(&data_type)
            .map_err(|_| DeltaQuadsStorageError::UnsupportedArrowType(data_type))?;

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
            .with_configuration_property(
                TableProperty::TargetFileSize,
                Some("1073741824"), // 1 GiB
            )
            .with_table_name(format!("QuadTable_{components}"))
            .await?;

        let quad_table = Self {
            storage_encoding,
            table: Arc::new(RwLock::new(QuadTableState::new(table, Arc::new(vec![]), 0))),
            components,
        };

        Ok(quad_table)
    }

    /// Loads an existing quad_table and warms the file metadata cache of the `state`'s runtime
    /// environment with the footers of its files.
    pub async fn try_load(
        state: &SessionState,
        storage_encoding: QuadStorageEncoding,
        log_store: LogStoreRef,
        components: QuadTableName,
    ) -> Result<Self, DeltaQuadsStorageError> {
        let mut table = DeltaTable::new(Arc::clone(&log_store));
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
                .collect::<Vec<_>>(),
        );
        let log_transaction_version = snapshot
            .transaction_version(log_store.as_ref(), Self::APP_ID)
            .await?
            .unwrap_or(0) as u64;

        info!(
            "Loading Parquet footers for {} files in quad_table {}.",
            active_files.len(),
            components.to_string()
        );
        load_parquet_metadata_for_files(state, log_store.as_ref(), &active_files).await?;
        info!(
            "Parquet footers loaded for quad_table {}.",
            components.to_string()
        );

        Ok(Self {
            storage_encoding,
            table: Arc::new(RwLock::new(QuadTableState::new(
                table,
                active_files,
                log_transaction_version,
            ))),
            components,
        })
    }

    /// Returns a reference to the used [`QuadTableName`].
    pub fn components(&self) -> QuadTableName {
        self.components
    }

    /// Takes a point-in-time snapshot of the quad_table.
    ///
    /// Use this snapshot to read the log version and query the data without risking race conditions
    /// from concurrent updates. We assume that the files of the snapshot will not be deleted by
    /// another process (vacuuming).
    pub async fn snapshot(
        &self,
    ) -> Result<DeltaQuadsQuadTableSnapshot, DeltaQuadsStorageError> {
        let guard = self.table.read().await;
        Ok(DeltaQuadsQuadTableSnapshot::new(
            self.storage_encoding.clone(),
            guard.table.snapshot()?.snapshot().clone(),
            guard.table.log_store(),
            Arc::clone(&guard.active_files),
            self.components,
            guard.log_transaction_version,
        ))
    }

    /// Updates the quad_table to the given `target_version` by applying the changes from the log.
    pub async fn update(
        &self,
        state: &SessionState,
        changeset: DeltaQuadsStorageLogChangesetRef,
    ) -> Result<(), DeltaQuadsStorageError> {
        let updater = DeltaStorageQuadTableUpdater::new(
            self.snapshot().await?,
            self.table.read().await.table.clone(),
            changeset,
            state.clone(),
        );

        let (new_table, new_version) = Box::pin(updater.apply_update()).await?;
        self.update_table_state(state, new_table, new_version)
            .await?;

        Ok(())
    }

    /// Validates the quad_table by ensuring that the quad_table
    /// - ... contains no duplicates
    pub async fn validate(
        &self,
        state: &SessionState,
    ) -> Result<(), DeltaQuadsStorageError> {
        let snapshot = self.snapshot().await?;
        validate_quad_table(state, &snapshot).await
    }

    /// Sets the new table state.
    async fn update_table_state(
        &self,
        state: &SessionState,
        new_table: DeltaTable,
        log_transaction_version: u64,
    ) -> Result<(), DeltaQuadsStorageError> {
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

        // Unchanged files are already in the cache and are not loaded again.
        load_parquet_metadata_for_files(
            state,
            new_table.log_store().as_ref(),
            &active_files,
        )
        .await?;

        *table_lock = QuadTableState::new(
            new_table,
            Arc::new(active_files),
            log_transaction_version,
        );
        Ok(())
    }
}

/// Returns the [`ObjectMeta`] of a file in a quad_table, as derived from the Delta log.
///
/// The [`ObjectMeta`] is used as a key for DataFusion's file metadata cache. Hence, the planner
/// and the cache warm-up must use the same [`ObjectMeta`] for a given file.
///
/// Returns an error if the modification time in the Delta log is out of range, as all such files
/// would otherwise share the same fallback timestamp.
pub fn add_to_object_meta(
    table_path: &Path,
    add: &Add,
) -> Result<ObjectMeta, DeltaQuadsStorageError> {
    let last_modified = DateTime::from_timestamp_millis(add.modification_time)
        .ok_or_else(|| {
            DeltaQuadsStorageError::Corruption(format!(
                "Invalid modification time {} for file '{}' in the Delta log.",
                add.modification_time, add.path
            ))
        })?;
    Ok(ObjectMeta {
        location: table_path.clone().join(add.path.as_str()),
        last_modified,
        size: add.size as u64,
        e_tag: None,
        version: None,
    })
}

/// Loads the Parquet footers of the given files into the file metadata cache of the `state`'s
/// runtime environment. Multiple requests are issued concurrently.
///
/// The page index is not loaded, as it is only required for files that survive the row group
/// pruning during planning. It is loaded on demand by the planner.
async fn load_parquet_metadata_for_files(
    state: &SessionState,
    log_store: &dyn deltalake::logstore::LogStore,
    active_files: &[Add],
) -> Result<(), DeltaQuadsStorageError> {
    let table_path = Path::from(log_store.config().location().path());
    let object_metas = active_files
        .iter()
        .map(|add| add_to_object_meta(&table_path, add))
        .collect::<Result<Vec<_>, _>>()?;
    ParquetMetadataFetcher::new(state, log_store.root_object_store(None))
        .fetch_footers(&object_metas, max_concurrent_files(state))
        .await
        .map_err(|e| DeltaQuadsStorageError::Other(e.to_string()))?;

    Ok(())
}
