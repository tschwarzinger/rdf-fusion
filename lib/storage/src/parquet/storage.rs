use crate::block_cache::BlockCache;
use crate::parquet::reader::{ParquetMetadataFetcher, max_concurrent_files};
use crate::parquet::snapshot::ParquetQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::Schema;
use datafusion::common::Result as DFResult;
use datafusion::common::instant::Instant;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl, PartitionedFile,
};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::FileGroup;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::execution::context::SessionState;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use rdf_fusion_common::StorageError;
use rdf_fusion_common::config::ParquetStorageOptions;
use rdf_fusion_encoding::object_id::ObjectIdDictionary;
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName};
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use tracing::info;
use url::Url;

/// Builder for [`ParquetQuadStorage`].
pub struct ParquetQuadStorageBuilder {
    url: Url,
    encoding: QuadStorageEncodingName,
    options: ParquetStorageOptions,
    cache: Option<Arc<BlockCache>>,
}

impl ParquetQuadStorageBuilder {
    /// Creates a new [`ParquetQuadStorageBuilder`].
    ///
    /// The URL can either point to a single Parquet file, to a directory / prefix containing
    /// Parquet files, or to a glob (see [`ListingTableUrl`]).
    pub fn new(url: Url) -> Self {
        Self {
            url,
            encoding: QuadStorageEncodingName::String,
            options: ParquetStorageOptions::default(),
            cache: None,
        }
    }

    /// Sets the encoding for the storage.
    pub fn with_encoding(mut self, encoding: QuadStorageEncodingName) -> Self {
        self.encoding = encoding;
        self
    }

    /// Sets the options for the Parquet storage.
    pub fn with_options(mut self, options: ParquetStorageOptions) -> Self {
        self.options = options;
        self
    }

    /// Sets the block cache for caching Parquet file reads.
    pub fn with_cache(mut self, cache: Option<Arc<BlockCache>>) -> Self {
        self.cache = cache;
        self
    }

    /// Builds the [`ParquetQuadStorage`].
    ///
    /// This lists all Parquet files and eagerly loads their footers into the file metadata cache
    /// of the `state`'s runtime environment. The page index of a file is only loaded when a scan
    /// needs it. The object store for the URL must be registered in this runtime environment.
    ///
    /// The same runtime environment should be used for querying the storage. Otherwise, the
    /// metadata is loaded again during the first query.
    pub async fn build(
        self,
        state: &SessionState,
    ) -> Result<ParquetQuadStorage, StorageError> {
        let encoding = match self.encoding {
            QuadStorageEncodingName::PlainTerm => QuadStorageEncoding::PlainTerm,
            QuadStorageEncodingName::String => QuadStorageEncoding::String,
            QuadStorageEncodingName::ObjectId => {
                return Err(StorageError::Other(
                    "ObjectId encoding is not supported for Parquet storage".into(),
                ));
            }
        };

        let table_url =
            ListingTableUrl::parse(self.url.as_str()).map_err(to_storage_error)?;

        let cache = if let Some(cache) = self.cache {
            Some(cache)
        } else if self.options.data_cache_enabled {
            Some(Arc::new(BlockCache::new(
                self.options.data_cache_block_size,
                self.options.data_cache_num_blocks,
            )))
        } else {
            None
        };

        info!(
            "Listing Parquet files and loading their footers for {}...",
            self.url
        );
        let start = Instant::now();

        let files = list_parquet_files(state, &table_url, &encoding)
            .await
            .map_err(to_storage_error)?;
        if files.is_empty() {
            return Err(StorageError::Other(
                format!("No Parquet files found at {}", self.url).into(),
            ));
        }
        let footers = load_footers(state, &table_url, &files)
            .await
            .map_err(to_storage_error)?;
        let schema = encoding.quad_schema();
        let files = with_footer_statistics(schema.inner(), files, &footers)
            .map_err(to_storage_error)?;
        let output_ordering = output_ordering_from_footers(schema.inner(), &footers);

        let num_quads = files
            .iter()
            .filter_map(|file| file.statistics.as_ref()?.num_rows.get_value().copied())
            .sum::<usize>();
        info!(
            "Loaded footers of {} Parquet files ({num_quads} quads) for {} in {:.2?}.",
            files.len(),
            self.url,
            start.elapsed()
        );

        Ok(ParquetQuadStorage {
            url: self.url,
            object_store_url: table_url.object_store(),
            encoding,
            files: Arc::new(files),
            output_ordering,
            options: self.options,
            cache,
        })
    }
}

/// Lists the Parquet files at `table_url` using DataFusion's [`ListingTable`]. The files are
/// sorted by their path and do not have statistics.
async fn list_parquet_files(
    state: &SessionState,
    table_url: &ListingTableUrl,
    encoding: &QuadStorageEncoding,
) -> DFResult<Vec<PartitionedFile>> {
    // DataFusion's statistics collection would also load the page index of every file. Hence, the
    // statistics are computed from the footers in `with_footer_statistics`.
    let mut listing_state = state.clone();
    listing_state
        .config_mut()
        .options_mut()
        .execution
        .collect_statistics = false;

    // Only the files of a directory are filtered by their extension. A URL that points to a single
    // object is used as is, as it may not have an extension (e.g., objects in IndexedDB).
    let file_extension = if table_url.is_collection() {
        ".parquet"
    } else {
        ""
    };
    let format =
        ParquetFormat::default().with_options(state.table_options().parquet.clone());
    let listing_options =
        ListingOptions::new(Arc::new(format)).with_file_extension(file_extension);
    let listing_table = ListingTable::try_new(
        ListingTableConfig::new(table_url.clone())
            .with_listing_options(listing_options)
            .with_schema(Arc::clone(encoding.quad_schema().inner())),
    )?;
    let listed = listing_table
        .list_files_for_scan(&listing_state, &[], None)
        .await?;

    let mut files = listed
        .file_groups
        .into_iter()
        .flat_map(FileGroup::into_inner)
        .collect::<Vec<_>>();
    files.sort_by(|a, b| a.path().cmp(b.path()));
    Ok(files)
}

/// Loads the footers of the `files` into the file metadata cache. The footers are returned in the
/// order of `files`.
///
/// Only the footers are loaded. The page index of a file is loaded on demand during planning, if
/// the file survives the row group pruning.
async fn load_footers(
    state: &SessionState,
    table_url: &ListingTableUrl,
    files: &[PartitionedFile],
) -> DFResult<Vec<Arc<ParquetMetaData>>> {
    let store = state.runtime_env().object_store(table_url.object_store())?;
    let object_metas = files
        .iter()
        .map(|file| file.object_meta.clone())
        .collect::<Vec<_>>();
    ParquetMetadataFetcher::new(state, store)
        .fetch_footers(&object_metas, max_concurrent_files(state))
        .await
}

/// Attaches the statistics computed from the `footers` to the `files`.
fn with_footer_statistics(
    schema: &Arc<Schema>,
    files: Vec<PartitionedFile>,
    footers: &[Arc<ParquetMetaData>],
) -> DFResult<Vec<PartitionedFile>> {
    files
        .into_iter()
        .zip(footers)
        .map(|(file, footer)| {
            let statistics =
                DFParquetMetadata::statistics_from_parquet_metadata(footer, schema)?;
            Ok(file.with_statistics(Arc::new(statistics)))
        })
        .collect()
}

/// Returns the ordering of the quads within each file, if all files declare the same
/// `sorting_columns` in all their row groups. Otherwise, returns [`None`].
///
/// Whether the concatenation of multiple files is also ordered is checked by DataFusion based on
/// the statistics of the files.
fn output_ordering_from_footers(
    schema: &Schema,
    footers: &[Arc<ParquetMetaData>],
) -> Option<LexOrdering> {
    let (first, rest) = footers.split_first()?;
    let ordering = file_output_ordering(schema, first)?;
    rest.iter()
        .all(|footer| file_output_ordering(schema, footer).as_ref() == Some(&ordering))
        .then_some(ordering)
}

/// Returns the ordering of a single file, if all its row groups declare the same
/// `sorting_columns`.
///
/// Only sorting columns that refer to top-level columns of the `schema` are supported, as the
/// sorting columns refer to the leaf columns of the Parquet schema.
fn file_output_ordering(
    schema: &Schema,
    footer: &ParquetMetaData,
) -> Option<LexOrdering> {
    let row_groups = footer.row_groups();
    let sorting_columns = row_groups.first()?.sorting_columns()?;
    if row_groups
        .iter()
        .any(|row_group| row_group.sorting_columns() != Some(sorting_columns))
    {
        return None;
    }

    let parquet_schema = footer.file_metadata().schema_descr();
    let sort_exprs = sorting_columns
        .iter()
        .map(|sorting_column| {
            let leaf_idx = usize::try_from(sorting_column.column_idx).ok()?;
            let column = parquet_schema.columns().get(leaf_idx)?;
            let [name] = column.path().parts() else {
                return None;
            };
            let idx = schema.index_of(name).ok()?;
            Some(PhysicalSortExpr::new(
                Arc::new(Column::new(name, idx)),
                SortOptions {
                    descending: sorting_column.descending,
                    nulls_first: sorting_column.nulls_first,
                },
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    LexOrdering::new(sort_exprs)
}

fn to_storage_error(
    error: impl std::error::Error + Send + Sync + 'static,
) -> StorageError {
    StorageError::Other(Box::new(error))
}

/// A quad storage that reads from one or multiple Parquet files.
///
/// The core approach of this storage layer is based on the storing a large triples / quad table
/// in Parquet. The Parquet files themselves are not specialized for RDF. Any off-the-shelve Parquet
/// reader can access them.
///
/// The files are discovered using DataFusion's [`ListingTable`] machinery. Their metadata (footer
/// and, once needed, page index) is held in DataFusion's file metadata cache, while the data (including Bloom
/// filters) can be cached in an optional [`BlockCache`].
///
/// # References
///
/// - [COTTAS](https://link.springer.com/chapter/10.1007/978-3-032-09530-5_18) that discusses the
///   storage of a triple/quad table in Parquet. Note that we do not follow all recommendations of
///   the paper.
#[derive(Clone)]
pub struct ParquetQuadStorage {
    url: Url,
    object_store_url: ObjectStoreUrl,
    encoding: QuadStorageEncoding,
    files: Arc<Vec<PartitionedFile>>,
    /// The ordering of the quads within each file, as declared in the Parquet footers.
    output_ordering: Option<LexOrdering>,
    options: ParquetStorageOptions,
    cache: Option<Arc<BlockCache>>,
}

impl ParquetQuadStorage {
    /// Creates a [`ParquetQuadStorageBuilder`].
    pub fn builder(url: Url) -> ParquetQuadStorageBuilder {
        ParquetQuadStorageBuilder::new(url)
    }

    /// Returns the Parquet files of this storage.
    pub fn files(&self) -> &[PartitionedFile] {
        &self.files
    }

    /// Returns the ordering of the quads within each file, if all files declare the same sort
    /// order in their footers.
    pub fn output_ordering(&self) -> Option<&LexOrdering> {
        self.output_ordering.as_ref()
    }

    pub fn options(&self) -> &ParquetStorageOptions {
        &self.options
    }

    pub fn cache(&self) -> Option<&Arc<BlockCache>> {
        self.cache.as_ref()
    }
}

impl Debug for ParquetQuadStorage {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ParquetQuadStorage")
            .field("url", &self.url)
            .field("encoding", &self.encoding)
            .field("num_files", &self.files.len())
            .field("options", &self.options)
            .finish()
    }
}

#[async_trait]
impl QuadStorage for ParquetQuadStorage {
    fn encoding(&self) -> QuadStorageEncoding {
        self.encoding.clone()
    }

    fn object_id_mapping(&self) -> Option<Arc<dyn ObjectIdDictionary>> {
        None
    }

    async fn snapshot(&self) -> Result<Arc<dyn QuadStorageSnapshot>, StorageError> {
        Ok(Arc::new(ParquetQuadStorageSnapshot::new(
            self.encoding.clone(),
            self.url.clone(),
            self.object_store_url.clone(),
            Arc::clone(&self.files),
            self.output_ordering.clone(),
            self.cache.clone(),
        )))
    }

    async fn begin_transaction(
        &self,
        _state: &SessionState,
    ) -> Result<Box<dyn QuadStorageTransaction>, StorageError> {
        Err(StorageError::Other("Parquet storage is read-only".into()))
    }

    async fn optimize(&self, _state: &SessionState) -> Result<(), StorageError> {
        Ok(())
    }

    async fn validate(&self, _state: &SessionState) -> Result<(), StorageError> {
        // TODO: Validate that quads are unique.
        Ok(())
    }
}
