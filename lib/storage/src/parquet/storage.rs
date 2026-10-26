use crate::block_cache::BlockCache;
use crate::parquet::dataset::{
    DATASET_DESCRIPTION_FILE, ParquetDatasetDescription, ParquetQuadTableDescription,
};
use crate::parquet::reader::{ParquetMetadataFetcher, max_concurrent_files};
use crate::parquet::snapshot::ParquetQuadStorageSnapshot;
use async_trait::async_trait;
use datafusion::arrow::compute::SortOptions;
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::common::Result as DFResult;
use datafusion::common::ScalarValue;
use datafusion::common::instant::Instant;
use datafusion::datasource::listing::{ListingTableUrl, PartitionedFile};
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use datafusion::execution::context::SessionState;
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::expressions::Column;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use object_store::path::Path;
use object_store::{ObjectMeta, ObjectStoreExt};
use rdf_fusion_common::config::ParquetStorageOptions;
use rdf_fusion_common::{RdfSortOrder, StorageError, url_to_object_store_url};
use rdf_fusion_encoding::object_id::ObjectIdDictionary;
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName};
use rdf_fusion_extensions::storage::{
    QuadStorage, QuadStorageSnapshot, QuadStorageTransaction,
};
use std::cmp::Ordering;
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
    /// The URL either points to the directory of a dataset (ending with `/`), to its
    /// [`DATASET_DESCRIPTION_FILE`], or to a single Parquet file.
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
    /// If the URL points to the directory of a dataset (or to its [`DATASET_DESCRIPTION_FILE`]),
    /// the quad tables, files, and statistics are read from the description. Hence, neither
    /// listing the directory (which static file hosts usually do not support) nor opening a file
    /// is necessary. A directory without a description is an error. Otherwise, the URL points to a
    /// single Parquet file, whose footer is eagerly loaded into the file metadata cache of the
    /// `state`'s runtime environment.
    ///
    /// The page index of a file is only loaded when a scan needs it. The object store for the URL
    /// must be registered in this runtime environment.
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

        let start = Instant::now();
        let schema = encoding.quad_schema();
        let quad_tables = match load_dataset_description(
            state,
            &self.url,
            &table_url,
            schema.inner(),
        )
        .await?
        {
            Some((directory, description)) => {
                if description.encoding != self.encoding {
                    return Err(StorageError::Other(
                        format!(
                            "The dataset at {} uses the {} encoding, but {} was requested",
                            self.url, description.encoding, self.encoding
                        )
                        .into(),
                    ));
                }
                description
                    .quad_tables
                    .into_iter()
                    .map(|table| {
                        ParquetQuadTable::from_description(
                            &directory,
                            table,
                            schema.inner(),
                        )
                    })
                    .collect::<Vec<_>>()
            }
            None => {
                info!("Loading the footer of {}...", self.url);
                vec![
                    ParquetQuadTable::from_file(state, &table_url, &encoding)
                        .await
                        .map_err(to_storage_error)?,
                ]
            }
        };
        if quad_tables.iter().all(|table| table.files.is_empty()) {
            return Err(StorageError::Other(
                format!("No Parquet files found at {}", self.url).into(),
            ));
        }

        let num_quads = quad_tables[0]
            .files
            .iter()
            .filter_map(|file| file.statistics.as_ref()?.num_rows.get_value().copied())
            .sum::<usize>();
        info!(
            "Opened {} quad tables with {} Parquet files ({num_quads} quads) for {} in {:.2?}.",
            quad_tables.len(),
            quad_tables.iter().map(|t| t.files.len()).sum::<usize>(),
            self.url,
            start.elapsed()
        );

        Ok(ParquetQuadStorage {
            url: self.url,
            object_store_url: table_url.object_store(),
            encoding,
            quad_tables: Arc::new(quad_tables),
            options: self.options,
            cache,
        })
    }
}

/// A quad table of a [`ParquetQuadStorage`]: Parquet files that store all quads of the storage,
/// optionally sorted.
#[derive(Debug, Clone)]
pub struct ParquetQuadTable {
    /// The order of the quads in the files, if known.
    sort_order: Option<RdfSortOrder>,
    /// The files, with their statistics.
    files: Arc<Vec<PartitionedFile>>,
    /// The ordering of the quads within each file.
    output_ordering: Option<LexOrdering>,
}

impl ParquetQuadTable {
    /// Returns the order of the quads in the files, if known.
    pub fn sort_order(&self) -> Option<&RdfSortOrder> {
        self.sort_order.as_ref()
    }

    /// Returns the files of the quad table.
    pub fn files(&self) -> &Arc<Vec<PartitionedFile>> {
        &self.files
    }

    /// Returns the ordering of the quads within each file.
    pub fn output_ordering(&self) -> Option<&LexOrdering> {
        self.output_ordering.as_ref()
    }

    /// Creates a quad table from the single Parquet file at `table_url`, using the statistics and
    /// the sort order declared in its footer.
    async fn from_file(
        state: &SessionState,
        table_url: &ListingTableUrl,
        encoding: &QuadStorageEncoding,
    ) -> DFResult<Self> {
        let store = state.runtime_env().object_store(table_url.object_store())?;
        let files = vec![PartitionedFile::from(store.head(table_url.prefix()).await?)];
        let footers = load_footers(state, table_url, &files).await?;
        let schema = encoding.quad_schema();
        let files = with_footer_statistics(schema.inner(), files, &footers)?;
        let output_ordering = output_ordering_from_footers(schema.inner(), &footers);
        Ok(Self {
            sort_order: None,
            files: Arc::new(files),
            output_ordering,
        })
    }

    /// Creates a quad table from its description. The paths of the files are relative to the
    /// `directory` of the dataset.
    fn from_description(
        directory: &Path,
        table: ParquetQuadTableDescription,
        schema: &Schema,
    ) -> Self {
        let files = table
            .files
            .into_iter()
            .map(|file| {
                // The size and the time the file was written identify the cached metadata of
                // the file (see `dataset.rs`).
                let object_meta = ObjectMeta {
                    location: Path::from(format!("{}/{}", directory.as_ref(), file.path)),
                    last_modified: file.written_at.unwrap_or_default(),
                    size: file.size,
                    e_tag: None,
                    version: None,
                };
                PartitionedFile::from(object_meta)
                    .with_statistics(Arc::new(file.statistics))
            })
            .collect();
        let output_ordering = table
            .sort_order
            .as_ref()
            .and_then(|sort_order| output_ordering_from_sort_order(schema, sort_order));
        let files = match &output_ordering {
            Some(ordering) => sort_files_by_statistics(files, ordering),
            None => files,
        };
        Self {
            sort_order: table.sort_order,
            files: Arc::new(files),
            output_ordering,
        }
    }
}

/// Loads the [`ParquetDatasetDescription`] of the dataset at `url`. Returns the directory of the
/// dataset and the description, or [`None`] if `url` does not point to a dataset (but to a single
/// file).
async fn load_dataset_description(
    state: &SessionState,
    url: &Url,
    table_url: &ListingTableUrl,
    schema: &Schema,
) -> Result<Option<(Path, ParquetDatasetDescription)>, StorageError> {
    let (directory, description_url) = if let Some(directory) = url
        .path()
        .strip_suffix(DATASET_DESCRIPTION_FILE)
        .filter(|directory| directory.ends_with('/'))
    {
        (Path::from(directory), url.clone())
    } else if table_url.is_collection() {
        let description_url = url
            .join(DATASET_DESCRIPTION_FILE)
            .map_err(to_storage_error)?;
        (Path::from(url.path()), description_url)
    } else {
        return Ok(None);
    };

    let object_store_url = url_to_object_store_url(url).map_err(to_storage_error)?;
    let store = state
        .runtime_env()
        .object_store(&object_store_url)
        .map_err(to_storage_error)?;
    let location = directory.clone().join(DATASET_DESCRIPTION_FILE);
    let bytes = match store.get(&location).await {
        Ok(result) => result.bytes().await.map_err(to_storage_error)?,
        Err(object_store::Error::NotFound { .. }) => {
            return Err(StorageError::Other(
                format!(
                    "No {DATASET_DESCRIPTION_FILE} found at {description_url}. A directory must \
                     contain a dataset written by the RdfParquetLoader."
                )
                .into(),
            ));
        }
        Err(error) => return Err(to_storage_error(error)),
    };
    let description =
        ParquetDatasetDescription::from_turtle(&bytes, description_url.as_str(), schema)
            .map_err(to_storage_error)?;
    Ok(Some((directory, description)))
}

/// Returns the ordering of the quads within each file for a native `sort_order`. Other sort
/// orders do not correspond to the order of the values in the file.
///
/// Only top-level columns that are not structs are supported (see also
/// [`file_output_ordering`]).
fn output_ordering_from_sort_order(
    schema: &Schema,
    sort_order: &RdfSortOrder,
) -> Option<LexOrdering> {
    let RdfSortOrder::NativeOrder(components) = sort_order else {
        return None;
    };
    let sort_exprs = components
        .iter()
        .map(|component| {
            let name = component.column_name();
            let idx = schema.index_of(name).ok()?;
            if matches!(schema.field(idx).data_type(), DataType::Struct(_)) {
                return None;
            }
            Some(PhysicalSortExpr::new(
                Arc::new(Column::new(name, idx)),
                SortOptions {
                    descending: false,
                    nulls_first: true,
                },
            ))
        })
        .collect::<Option<Vec<_>>>()?;
    LexOrdering::new(sort_exprs)
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

/// Sorts the `files` by the first row of each file according to `ordering`, as given by the
/// statistics of the files.
///
/// If the files do not overlap, their concatenation is ordered, which DataFusion verifies based on
/// the same statistics. Hence, the order does not depend on the file names (e.g., the unpadded
/// indices of the files written by DataFusion's sinks). If a file lacks the required statistics,
/// the files are kept in their path order.
fn sort_files_by_statistics(
    files: Vec<PartitionedFile>,
    ordering: &LexOrdering,
) -> Vec<PartitionedFile> {
    let Some(keys) = files
        .iter()
        .map(|file| first_row_key(file, ordering))
        .collect::<Option<Vec<_>>>()
    else {
        return files;
    };

    let mut keyed = keys.into_iter().zip(files).collect::<Vec<_>>();
    // The sort is stable. Hence, files with the same key remain in their path order.
    keyed.sort_by(|(a, _), (b, _)| compare_keys(a, b, ordering));
    keyed.into_iter().map(|(_, file)| file).collect()
}

/// Returns the values of the first row of the `file` according to `ordering`, where [`None`]
/// represents a null value.
///
/// Returns [`None`] if the statistics of the file are insufficient.
fn first_row_key(
    file: &PartitionedFile,
    ordering: &LexOrdering,
) -> Option<Vec<Option<ScalarValue>>> {
    let statistics = file.statistics.as_ref()?;
    ordering
        .iter()
        .map(|sort_expr| {
            let column = sort_expr.expr.downcast_ref::<Column>()?;
            let column_statistics = statistics.column_statistics.get(column.index())?;
            let has_nulls = column_statistics
                .null_count
                .get_value()
                .is_some_and(|null_count| *null_count > 0);
            if has_nulls && sort_expr.options.nulls_first {
                return Some(None);
            }

            let first_value = if sort_expr.options.descending {
                &column_statistics.max_value
            } else {
                &column_statistics.min_value
            };
            match first_value.get_value() {
                Some(value) if !value.is_null() => Some(Some(value.clone())),
                // Only null values
                _ if has_nulls => Some(None),
                _ => None,
            }
        })
        .collect()
}

/// Compares two keys created by [`first_row_key`] according to `ordering`.
fn compare_keys(
    a: &[Option<ScalarValue>],
    b: &[Option<ScalarValue>],
    ordering: &LexOrdering,
) -> Ordering {
    a.iter()
        .zip(b)
        .zip(ordering.iter())
        .map(|((a, b), sort_expr)| match (a, b) {
            (None, None) => Ordering::Equal,
            (None, Some(_)) if sort_expr.options.nulls_first => Ordering::Less,
            (None, Some(_)) => Ordering::Greater,
            (Some(_), None) if sort_expr.options.nulls_first => Ordering::Greater,
            (Some(_), None) => Ordering::Less,
            (Some(a), Some(b)) => {
                let ordering = a.partial_cmp(b).unwrap_or(Ordering::Equal);
                if sort_expr.options.descending {
                    ordering.reverse()
                } else {
                    ordering
                }
            }
        })
        .find(|ordering| ordering.is_ne())
        .unwrap_or(Ordering::Equal)
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
/// The files are either read from the description of a dataset (see [`ParquetDatasetDescription`])
/// or the storage consists of a single file. Their metadata (footer and, once needed, page index) is held in DataFusion's file metadata cache, while the data (including Bloom
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
    /// The quad tables. Each quad table stores all quads, possibly in a different sort order.
    quad_tables: Arc<Vec<ParquetQuadTable>>,
    options: ParquetStorageOptions,
    cache: Option<Arc<BlockCache>>,
}

impl ParquetQuadStorage {
    /// Creates a [`ParquetQuadStorageBuilder`].
    pub fn builder(url: Url) -> ParquetQuadStorageBuilder {
        ParquetQuadStorageBuilder::new(url)
    }

    /// Returns the quad tables of this storage.
    pub fn quad_tables(&self) -> &[ParquetQuadTable] {
        &self.quad_tables
    }

    /// Returns the Parquet files of all quad tables of this storage.
    pub fn files(&self) -> Vec<PartitionedFile> {
        self.quad_tables
            .iter()
            .flat_map(|table| table.files.iter().cloned())
            .collect()
    }

    /// Returns the ordering of the quads within each file, if the storage has a single quad table
    /// with a known ordering.
    pub fn output_ordering(&self) -> Option<&LexOrdering> {
        match self.quad_tables.as_slice() {
            [table] => table.output_ordering(),
            _ => None,
        }
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
            .field("num_quad_tables", &self.quad_tables.len())
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
            Arc::clone(&self.quad_tables),
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
