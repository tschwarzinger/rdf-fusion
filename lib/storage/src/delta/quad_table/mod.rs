mod snapshot;
mod update;
mod validation;

use crate::delta::error::DeltaQuadsStorageError;
use crate::delta::log::DeltaQuadsStorageLogChangesetRef;
use crate::delta::quad_table::update::DeltaStorageQuadTableUpdater;
use crate::delta::quad_table::validation::validate_quad_table;
use crate::parquet::{
    PreloadedBloomFilters, PreloadedParquetMetadata,
    load_parquet_metadata_and_bloom_filters,
};
use crate::quad_tables::QuadTableName;
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
use deltalake::{
    DataType as DeltaDataType, DeltaTable, DeltaTableConfig, StructField, TableProperty,
};
use futures::TryStreamExt;
use object_store::ObjectStoreExt;
use rdf_fusion_common::quads::{COL_GRAPH, COL_OBJECT, COL_PREDICATE, COL_SUBJECT};
use rdf_fusion_common::{BlankNodeMatchingMode, NamedNodePattern, TermPattern};
use rdf_fusion_encoding::QuadStorageEncoding;
pub use snapshot::DeltaQuadsQuadTableSnapshot;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::info;

/// TODO: Make this configurable
const PAGE_ROW_COUNT: usize = 8_192;
/// TODO: Make this configurable
const ROW_GROUP_ROW_COUNT: usize = PAGE_ROW_COUNT * 32;
/// TODO: Make this configurable
const FILE_ROW_COUNT: usize = ROW_GROUP_ROW_COUNT * 32;

/// The state of the quad_table table.
///
/// As we only support a single writer for now, we can cache the active files in memory.
struct QuadTableState {
    /// The underlying delta table.
    table: DeltaTable,
    /// The active files in the quad_table.
    active_files: Arc<Vec<Add>>,
    /// Preloaded parquet metadata.
    parquet_metadata: PreloadedParquetMetadata,
    /// Preloaded bloom filters.
    bloom_filters: PreloadedBloomFilters,
    /// The log transaction version of the quad_table.
    log_transaction_version: u64,
}

impl QuadTableState {
    /// Creates a new [`QuadTableState`].
    fn new(
        table: DeltaTable,
        active_files: Arc<Vec<Add>>,
        parquet_metadata: PreloadedParquetMetadata,
        bloom_filters: PreloadedBloomFilters,
        log_transaction_version: u64,
    ) -> Self {
        Self {
            table,
            active_files,
            parquet_metadata,
            bloom_filters,
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
            table: Arc::new(RwLock::new(QuadTableState::new(
                table,
                Arc::new(vec![]),
                PreloadedParquetMetadata::new(),
                PreloadedBloomFilters::new(),
                0,
            ))),
            components,
        };

        Ok(quad_table)
    }

    /// TODO
    pub async fn try_load(
        storage_encoding: QuadStorageEncoding,
        log_store: LogStoreRef,
        components: QuadTableName,
    ) -> Result<Self, DeltaQuadsStorageError> {
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
                .collect::<Vec<_>>(),
        );
        let log_transaction_version = snapshot
            .transaction_version(log_store.as_ref(), Self::APP_ID)
            .await?
            .unwrap_or(0) as u64;

        info!(
            "Loading Parquet metadata and Bloom filters for {} files in quad_table {}.",
            active_files.len(),
            components.to_string()
        );
        let (parquet_metadata, bloom_filters) = load_parquet_metadata_for_files(
            log_store.as_ref(),
            &active_files,
            None,
            None,
        )
        .await?;
        info!(
            "Parquet metadata and Bloom filters loaded for quad_table {}.",
            components.to_string()
        );

        Ok(Self {
            storage_encoding,
            table: Arc::new(RwLock::new(QuadTableState::new(
                table,
                active_files,
                parquet_metadata,
                bloom_filters,
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
            guard.parquet_metadata.clone(),
            guard.bloom_filters.clone(),
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
            self.create_write_properties_for_update(),
        );

        let (new_table, new_version) = Box::pin(updater.apply_update()).await?;
        self.update_table_state(new_table, new_version).await?;

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

        let (parquet_metadata, bloom_filters) = load_parquet_metadata_for_files(
            new_table.log_store().as_ref(),
            &active_files,
            Some(table_lock.parquet_metadata.clone()),
            Some(table_lock.bloom_filters.clone()),
        )
        .await?;

        *table_lock = QuadTableState::new(
            new_table,
            Arc::new(active_files),
            parquet_metadata,
            bloom_filters,
            log_transaction_version,
        );
        Ok(())
    }

    /// Creates the Parquet writer properties for the quad_table update.
    fn create_write_properties_for_update(&self) -> WriterProperties {
        let sorting_columns = self
            .components
            .inner()
            .iter()
            .map(|c| SortingColumn {
                column_idx: c.gspo_quad_table() as i32,
                descending: false,
                nulls_first: true,
            })
            .collect();

        let last_component =
            ColumnPath::new(vec![self.components.inner()[3].column_name().to_owned()]);
        let mut writer_properties_builder = WriterProperties::builder()
            .set_max_row_group_row_count(Some(ROW_GROUP_ROW_COUNT))
            .set_data_page_row_count_limit(PAGE_ROW_COUNT)
            .set_bloom_filter_enabled(true)
            .set_column_bloom_filter_enabled(last_component.clone(), true)
            .set_sorting_columns(Some(sorting_columns))
            .set_column_dictionary_enabled(last_component.clone(), false)
            .set_statistics_enabled(EnabledStatistics::Page);

        if self.storage_encoding.term_type().is_primitive() {
            writer_properties_builder = writer_properties_builder
                .set_column_encoding(last_component, Encoding::PLAIN)
                .set_compression(Compression::UNCOMPRESSED)
        } else {
            writer_properties_builder = writer_properties_builder
                .set_encoding(Encoding::DELTA_LENGTH_BYTE_ARRAY) // Good for common prefixes
                .set_statistics_truncate_length(Some(256)) // IRIs might be long
                .set_column_index_truncate_length(Some(256)) // IRIs might be long;
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
        }

        writer_properties_builder.build()
    }
}

/// Loads the Parquet Metadata and Bloom filters for the given files. Multiple requests are issued
/// concurrently.
async fn load_parquet_metadata_for_files(
    log_store: &dyn deltalake::logstore::LogStore,
    active_files: &[Add],
    existing_meta: Option<PreloadedParquetMetadata>,
    existing_bloom: Option<PreloadedBloomFilters>,
) -> Result<(PreloadedParquetMetadata, PreloadedBloomFilters), DeltaQuadsStorageError> {
    use object_store::path::Path;
    use std::sync::Arc;
    use tokio::task::JoinSet;

    let object_store = log_store.object_store(None);
    let base_path = Path::from(log_store.config().location().path());
    let new_meta = PreloadedParquetMetadata::new();
    let new_bloom = PreloadedBloomFilters::new();
    let existing_meta = Arc::new(existing_meta);
    let existing_bloom = Arc::new(existing_bloom);
    let mut join_set: JoinSet<Result<_, DeltaQuadsStorageError>> = JoinSet::new();

    for add in active_files {
        let path_str = add.path.clone();
        let ext_meta_clone = Arc::clone(&existing_meta);
        let ext_bloom_clone = Arc::clone(&existing_bloom);
        let object_store_clone = Arc::clone(&object_store);
        let base_path_clone = base_path.clone();

        join_set.spawn(async move {
            let relative_path = Path::from(path_str.as_str());
            let absolute_path = base_path_clone.join(path_str.as_str());

            if let (Some(meta_cache), Some(bloom_cache)) =
                (ext_meta_clone.as_ref(), ext_bloom_clone.as_ref())
            {
                if let (Some(meta), Some(bloom)) = (
                    meta_cache.get(&absolute_path),
                    bloom_cache.get_all(&absolute_path),
                ) {
                    return Ok((absolute_path, Some((meta, bloom)), None));
                }
            }

            let mut object_meta = object_store_clone
                .head(&relative_path)
                .await
                .map_err(|e| DeltaQuadsStorageError::Other(e.to_string()))?;

            let (parquet_meta, bloom_filters) = load_parquet_metadata_and_bloom_filters(
                object_store_clone,
                relative_path.clone(),
                object_meta.clone(),
            )
            .await
            .map_err(|e| DeltaQuadsStorageError::Other(e.to_string()))?;

            object_meta.location = absolute_path.clone();

            Ok((
                absolute_path,
                None,
                Some((parquet_meta, object_meta, bloom_filters)),
            ))
        });
    }

    let mut remaining = active_files.len();
    while let Some(res) = join_set.join_next().await {
        remaining -= 1;
        if remaining > 0 && remaining % 10 == 0 {
            info!("Progress: {} files remaining to process", remaining);
        }

        let (absolute_path, cached, fetched) = res.map_err(|e| {
            DeltaQuadsStorageError::Other(format!("Task execution failed: {e}"))
        })??;

        if let Some((meta, bloom)) = cached {
            new_meta.insert(absolute_path.clone(), meta);
            new_bloom.insert_arc(absolute_path, bloom);
        } else if let Some((parquet_meta, object_meta, bloom_filters)) = fetched {
            new_meta.insert(absolute_path.clone(), (parquet_meta, object_meta));
            new_bloom.insert(absolute_path, bloom_filters);
        }
    }

    Ok((new_meta, new_bloom))
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
