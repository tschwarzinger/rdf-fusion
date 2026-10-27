use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
use datafusion::logical_expr::{Extension, LogicalPlan};
use datafusion::physical_plan::displayable;
use datafusion::prelude::{DataFrame, SessionConfig, SessionContext};
use futures::StreamExt;
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use rdf_fusion_common::config::RdfFusionOptions;
use rdf_fusion_common::{GraphName, RdfFormat, RdfInput, RdfInputSource, RdfSortOrder};
use rdf_fusion_encoding::{QuadStorageEncoding, QuadStorageEncodingName};
use rdf_fusion_execution::results::QueryResults;
use rdf_fusion_execution::sparql::QueryOptions;
use rdf_fusion_execution::{RdfFusionContext, RdfFusionContextBuilder};
use rdf_fusion_extensions::storage::QuadStorage;
use rdf_fusion_sparql_parser::ParserOptions;
use rdf_fusion_storage::delta::DeltaQuadsStorageBuilder;
use rdf_fusion_storage::parquet::{
    ParquetQuadStorage, RdfParquetLoader, RdfParquetLoadingError,
};
use rdf_fusion_storage::rdf_files::{ParseRdfFileNode, RdfFileScanOptions};
use std::sync::Arc;
use url::Url;

const NUM_TRIPLES: usize = 100;
const MAX_ROWS_PER_FILE: usize = 25;

#[tokio::test]
async fn test_parquet_loader_writes_and_reads_multiple_files() {
    let env = TestEnv::new(MAX_ROWS_PER_FILE).await;
    env.put_ntriples("input.nt", NUM_TRIPLES).await;

    let output_url = Url::parse("memory:///dataset/").unwrap();
    env.load("memory:///input.nt", &output_url).await;

    // Writing: the quads are split into multiple, non-overlapping files with the expected size,
    // and the dataset is described in a `dataset.ttl`.
    let files = env.list_files("dataset").await;
    assert_eq!(files[0], "dataset/dataset.ttl");
    assert_eq!(files.len(), 5);
    assert!(
        files[1..]
            .iter()
            .all(|file| file.starts_with("dataset/sparql-spog/")),
        "Expected all files in the SPOG quad table: {files:?}"
    );
    let storage = env.open_storage(output_url).await;
    // The limit is approximate, as batches (of 10 rows) are not split.
    assert_eq!(rows_per_file(&storage), vec![30, 30, 30, 10]);
    assert_files_do_not_overlap(&storage);

    // Reading: all files are found and contain all quads.
    assert_eq!(
        storage
            .snapshot()
            .await
            .unwrap()
            .len(&env.context.state())
            .await
            .unwrap(),
        NUM_TRIPLES
    );

    let rdf_context = env.rdf_fusion_context(Arc::clone(&storage) as _);
    let (count, _) = run_query(&rdf_context, "SELECT * WHERE { ?s ?p ?o }").await;
    assert_eq!(count, NUM_TRIPLES);

    // As the files are sorted by subject, a lookup by subject only scans a single file.
    let (count, plan) = run_query(
        &rdf_context,
        "SELECT ?o WHERE { <http://example.org/s042> ?p ?o }",
    )
    .await;
    assert_eq!(count, 1);
    let second_file = storage.files()[1].path().to_string();
    assert!(
        plan.contains(&format!("file_groups={{1 group: [[{second_file}]]}}")),
        "Expected only a single file to be scanned: {plan}"
    );
}

#[tokio::test]
async fn test_parquet_loader_writes_non_overlapping_files() {
    let env = TestEnv::new(10).await;
    env.put_ntriples("input.nt", 120).await;

    let output_url = Url::parse("memory:///dataset/").unwrap();
    env.load_with_sort_orders("memory:///input.nt", &output_url, &["Native(SPOG)"])
        .await
        .unwrap();

    let storage = env.open_storage(output_url).await;
    assert_eq!(rows_per_file(&storage), vec![10; 12]);
    assert_files_do_not_overlap(&storage);

    let rdf_context = env.rdf_fusion_context(storage);
    let (count, _) = run_query(
        &rdf_context,
        "SELECT (COUNT(*) AS ?c) WHERE { ?s ?p ?o } GROUP BY ?s",
    )
    .await;
    assert_eq!(count, 120);
}

/// A URL without a trailing `/` produces a single file, even if it has no `.parquet` extension
/// (e.g., objects in IndexedDB).
#[tokio::test]
async fn test_parquet_loader_writes_single_file_without_extension() {
    let env = TestEnv::new(MAX_ROWS_PER_FILE).await;
    env.put_ntriples("input.nt", NUM_TRIPLES).await;

    let output_url = Url::parse("memory:///dataset").unwrap();
    env.load("memory:///input.nt", &output_url).await;
    // A single object, not a directory.
    assert!(env.store.head(&Path::from("dataset")).await.is_ok());
    assert!(env.list_files("dataset").await.is_empty());

    let storage = env.open_storage(output_url).await;
    assert_eq!(rows_per_file(&storage), vec![NUM_TRIPLES]);
}

/// A dataset with multiple sort orders stores one quad table per sort order. The storage reads
/// the files and their statistics from the dataset description, chooses the best quad table for
/// each pattern, and only opens the files that may contain matching quads.
#[tokio::test]
async fn test_parquet_loader_writes_dataset_with_multiple_sort_orders() {
    let env = TestEnv::new(MAX_ROWS_PER_FILE).await;
    env.put_ntriples("input.nt", NUM_TRIPLES).await;

    let output_url = Url::parse("memory:///dataset/").unwrap();
    env.load_with_sort_orders(
        "memory:///input.nt",
        &output_url,
        &["NATIVE(SPOG)", "NATIVE(OSPG)"],
    )
    .await
    .unwrap();

    // Writing: one quad table per sort order and the description of the dataset.
    let files = env.list_files("dataset").await;
    assert_eq!(files[0], "dataset/dataset.ttl");
    for table in ["dataset/ospg/", "dataset/spog/"] {
        assert_eq!(
            files.iter().filter(|file| file.starts_with(table)).count(),
            4,
            "Expected four files in {table}: {files:?}"
        );
    }
    assert_eq!(files.len(), 9);

    // Reading: the files and their statistics are read from the description, so no file is
    // opened. The dataset can be opened via its directory or its description.
    let explicit = env
        .open_storage(output_url.join("dataset.ttl").unwrap())
        .await;
    assert_eq!(explicit.files().len(), 8);
    // The size of each file and the time it was written are recorded when writing, such that a
    // rewritten file does not use stale cache entries.
    for file in explicit.files() {
        let object_meta = env.store.head(&file.object_meta.location).await.unwrap();
        assert_eq!(file.object_meta.size, object_meta.size);
        assert_eq!(file.object_meta.last_modified, object_meta.last_modified);
    }
    let storage = env.open_storage(output_url).await;
    assert_eq!(storage.quad_tables().len(), 2);
    let rdf_context =
        env.rdf_fusion_context(Arc::clone(&storage) as Arc<dyn QuadStorage>);
    assert_eq!(cached_files(&rdf_context, &storage), Vec::<String>::new());
    // The quad tables are ordered by their directory.
    let second_ospg_file =
        file_containing(&storage, 0, "object", "<http://example.org/o042>");
    let second_spog_file =
        file_containing(&storage, 1, "subject", "<http://example.org/s042>");
    assert!(second_ospg_file.starts_with("dataset/ospg/"));
    assert!(second_spog_file.starts_with("dataset/spog/"));

    // A lookup by subject uses the SPOG quad table, and a lookup by object the OSPG quad table.
    // Only the file whose statistics may contain the term is opened.
    let (count, plan) = run_query(
        &rdf_context,
        "SELECT ?o WHERE { <http://example.org/s042> ?p ?o }",
    )
    .await;
    assert_eq!(count, 1);
    assert!(
        plan.contains(&format!("file_groups={{1 group: [[{second_spog_file}]]}}")),
        "Expected a single file of the SPOG quad table to be scanned: {plan}"
    );
    let (count, plan) = run_query(
        &rdf_context,
        "SELECT ?s WHERE { ?s ?p <http://example.org/o042> }",
    )
    .await;
    assert_eq!(count, 1);
    assert!(
        plan.contains(&format!("file_groups={{1 group: [[{second_ospg_file}]]}}")),
        "Expected a single file of the OSPG quad table to be scanned: {plan}"
    );
    assert_eq!(
        cached_files(&rdf_context, &storage),
        vec![second_ospg_file, second_spog_file]
    );

    // All quads are in each quad table.
    let (count, _) = run_query(&rdf_context, "SELECT * WHERE { ?s ?p ?o }").await;
    assert_eq!(count, NUM_TRIPLES);
}

/// All quad tables of a dataset rename the blank nodes in the same way. Otherwise, joining two
/// patterns that are answered from different quad tables would not match.
#[tokio::test]
async fn test_parquet_loader_renames_blank_nodes_consistently_across_quad_tables() {
    let env = TestEnv::new(MAX_ROWS_PER_FILE).await;
    let ntriples = (0..NUM_TRIPLES)
        .map(|i| {
            format!(
                "_:b{i} <http://example.org/type> <http://example.org/Thing> .\n\
                 _:b{i} <http://example.org/name> \"name {i}\" .\n"
            )
        })
        .collect::<String>();
    env.store
        .put(&Path::from("input.nt"), PutPayload::from(ntriples))
        .await
        .unwrap();

    let output_url = Url::parse("memory:///dataset/").unwrap();
    env.load_with_sort_orders(
        "memory:///input.nt",
        &output_url,
        &["NATIVE(SPOG)", "NATIVE(OSPG)"],
    )
    .await
    .unwrap();
    let storage = env.open_storage(output_url).await;
    let rdf_context = env.rdf_fusion_context(storage);

    // The first pattern binds the object and is answered from OSPG, the second one from SPOG.
    let (count, plan) = run_query(
        &rdf_context,
        "SELECT ?name WHERE {
            ?b <http://example.org/type> <http://example.org/Thing> .
            ?b <http://example.org/name> ?name .
        }",
    )
    .await;
    assert!(
        plan.contains("dataset/ospg/") && plan.contains("dataset/spog/"),
        "Expected both quad tables to be scanned: {plan}"
    );
    assert_eq!(count, NUM_TRIPLES);
}

/// A directory is only opened via its `dataset.ttl`. Parquet files in a directory without a
/// description are not listed.
#[tokio::test]
async fn test_parquet_storage_rejects_directory_without_description() {
    let env = TestEnv::new(MAX_ROWS_PER_FILE).await;
    env.put_ntriples("input.nt", NUM_TRIPLES).await;
    env.load(
        "memory:///input.nt",
        &Url::parse("memory:///dataset/file.parquet").unwrap(),
    )
    .await;

    let state = env.context.state();
    let result = ParquetQuadStorage::builder(Url::parse("memory:///dataset/").unwrap())
        .with_encoding(QuadStorageEncodingName::String)
        .build(&state)
        .await;
    let Err(error) = result else {
        panic!("Expected an error for a directory without a dataset.ttl");
    };
    assert!(error.to_string().contains("dataset.ttl"), "{error}");
}

/// A single file can only be written in a single sort order.
#[tokio::test]
async fn test_parquet_loader_rejects_multiple_sort_orders_for_single_file() {
    let env = TestEnv::new(MAX_ROWS_PER_FILE).await;
    env.put_ntriples("input.nt", NUM_TRIPLES).await;

    let output_url = Url::parse("memory:///dataset.parquet").unwrap();
    let result = env
        .load_with_sort_orders(
            "memory:///input.nt",
            &output_url,
            &["NATIVE(SPOG)", "NATIVE(OSPG)"],
        )
        .await;
    assert!(matches!(
        result,
        Err(RdfParquetLoadingError::MultipleSortOrdersForSingleFile)
    ));
}

/// Returns the sorted paths of the files of the storage whose metadata is cached.
fn cached_files(
    rdf_context: &RdfFusionContext,
    storage: &ParquetQuadStorage,
) -> Vec<String> {
    let metadata_cache = rdf_context
        .session_context()
        .runtime_env()
        .cache_manager
        .get_file_metadata_cache();
    let mut files = storage
        .files()
        .iter()
        .filter(|file| {
            metadata_cache
                .get(&file.object_meta.location)
                .is_some_and(|entry| {
                    entry
                        .file_metadata
                        .as_any()
                        .downcast_ref::<CachedParquetMetaData>()
                        .is_some()
                })
        })
        .map(|file| file.object_meta.location.to_string())
        .collect::<Vec<_>>();
    files.sort();
    files
}

/// A session with an in-memory object store registered for `memory:///`.
struct TestEnv {
    session_config: SessionConfig,
    context: SessionContext,
    store: Arc<InMemory>,
}

impl TestEnv {
    async fn new(max_rows_per_file: usize) -> Self {
        Self::with_config(
            SessionConfig::new()
                .with_batch_size(10)
                .with_target_partitions(1)
                .set_usize(
                    "datafusion.execution.soft_max_rows_per_output_file",
                    max_rows_per_file,
                ),
        )
        .await
    }

    async fn with_config(session_config: SessionConfig) -> Self {
        let context = SessionContext::new_with_config(session_config.clone());
        let store = Arc::new(InMemory::new());
        context
            .runtime_env()
            .object_store_registry
            .register_store(&Url::parse("memory:///").unwrap(), Arc::clone(&store) as _);
        Self {
            session_config,
            context,
            store,
        }
    }

    /// Writes an N-Triples file with `num_triples` triples (sortable by subject) to `path`.
    async fn put_ntriples(&self, path: &str, num_triples: usize) {
        let ntriples = (0..num_triples)
            .map(|i| {
                format!(
                    "<http://example.org/s{i:03}> <http://example.org/p> <http://example.org/o{i:03}> .\n"
                )
            })
            .collect::<String>();
        self.put(path, ntriples).await;
    }

    async fn put(&self, path: &str, content: String) {
        self.store
            .put(&Path::from(path), PutPayload::from(content))
            .await
            .unwrap();
    }

    /// Returns the physical plan for parsing the N-Triples file at `input_url`.
    async fn parse_plan(&self, input_url: &str) -> String {
        let node = ParseRdfFileNode::new(
            RdfInputSource::from_url(Url::parse(input_url).unwrap()),
            RdfFileScanOptions::with_format(RdfFormat::NTriples)
                .with_rename_blank_nodes(true),
            QuadStorageEncoding::String.quad_schema(),
        );
        let plan = DataFrame::new(
            self.loading_context().await.session_context().state(),
            LogicalPlan::Extension(Extension {
                node: Arc::new(node),
            }),
        )
        .create_physical_plan()
        .await
        .unwrap();
        displayable(plan.as_ref()).indent(true).to_string()
    }

    /// Loads the RDF file at `input_url` into a Parquet storage (sorted by SPOG) at `output_url`.
    async fn load(&self, input_url: &str, output_url: &Url) {
        self.load_with_sort_orders(input_url, output_url, &["SPOG"])
            .await
            .unwrap();
    }

    /// Loads the RDF file at `input_url` into a Parquet storage at `output_url`, writing each of
    /// the `sort_orders`.
    async fn load_with_sort_orders(
        &self,
        input_url: &str,
        output_url: &Url,
        sort_orders: &[&str],
    ) -> Result<(), RdfParquetLoadingError> {
        let loading_context = self.loading_context().await;
        let loader = RdfParquetLoader::try_new(
            loading_context.session_context().clone(),
            loading_context.create_view(),
            QuadStorageEncodingName::String,
            sort_orders
                .iter()
                .map(|order| order.parse::<RdfSortOrder>().unwrap())
                .collect(),
        )
        .unwrap();
        let input =
            RdfInput::try_new(Url::parse(input_url).unwrap(), GraphName::DefaultGraph)
                .unwrap();
        loader.load(input, output_url.clone()).await
    }

    /// Creates an [`RdfFusionContext`] for planning the load. The storage is a dummy.
    async fn loading_context(&self) -> RdfFusionContext {
        let dummy_storage =
            Arc::new(DeltaQuadsStorageBuilder::new().build().await.unwrap());
        self.rdf_fusion_context(dummy_storage)
    }

    /// Returns the sorted paths of all objects below `prefix`.
    async fn list_files(&self, prefix: &str) -> Vec<String> {
        let mut files = self
            .store
            .list(Some(&Path::from(prefix)))
            .map(|meta| meta.unwrap().location.to_string())
            .collect::<Vec<_>>()
            .await;
        files.sort();
        files
    }

    async fn open_storage(&self, url: Url) -> Arc<ParquetQuadStorage> {
        let state = self.context.state();
        Arc::new(
            ParquetQuadStorage::builder(url)
                .with_encoding(QuadStorageEncodingName::String)
                .build(&state)
                .await
                .unwrap(),
        )
    }

    /// Creates an [`RdfFusionContext`] that shares the runtime environment of this session.
    fn rdf_fusion_context(&self, storage: Arc<dyn QuadStorage>) -> RdfFusionContext {
        RdfFusionContextBuilder::new(storage)
            .with_session_config(Some(self.session_config.clone()))
            .with_runtime_env(Some(self.context.runtime_env()))
            .build()
            .unwrap()
    }
}

const NUM_SUBJECTS: usize = 210;
const NUM_BLANK_NODES: usize = 7;

#[tokio::test]
async fn test_parquet_loader_parses_line_based_files_in_parallel() {
    let mut options = RdfFusionOptions::default();
    // Split even the small test file into chunks.
    options.storage.rdf_files.parallel_parsing_min_chunk_size = 1024;
    let env = TestEnv::with_config(
        SessionConfig::new()
            .with_batch_size(16)
            .with_target_partitions(4)
            .with_option_extension(options),
    )
    .await;
    env.put("input.nt", ntriples_with_shared_blank_nodes())
        .await;

    let parse_plan = env.parse_plan("memory:///input.nt").await;
    assert!(
        parse_plan.contains("RdfParserExec: partitions=4"),
        "Expected the file to be parsed in parallel: {parse_plan}"
    );

    let output_url = Url::parse("memory:///dataset/").unwrap();
    env.load("memory:///input.nt", &output_url).await;
    let storage = env.open_storage(output_url).await;
    let rdf_context = env.rdf_fusion_context(storage);

    // Every triple is loaded exactly once.
    let (count, _) = run_query(&rdf_context, "SELECT * WHERE { ?s ?p ?o }").await;
    assert_eq!(count, 3 * NUM_SUBJECTS);

    // Blank nodes are renamed consistently across chunks (otherwise, each chunk would produce
    // its own blank nodes) and positions.
    let (count, _) = run_query(
        &rdf_context,
        "SELECT DISTINCT ?b WHERE { ?b ?p ?o FILTER(isBlank(?b)) }",
    )
    .await;
    assert_eq!(count, NUM_BLANK_NODES);
    let (count, _) = run_query(
        &rdf_context,
        "SELECT * WHERE {
            ?s <http://example.org/group> ?b .
            ?b <http://example.org/member> ?s .
            FILTER(isBlank(?b))
        }",
    )
    .await;
    assert_eq!(count, NUM_SUBJECTS);

    // Escaped line breaks in literals are preserved.
    let (count, _) = run_query(
        &rdf_context,
        "SELECT * WHERE { <http://example.org/s5> <http://example.org/label> \"line\\nbreak 5\" }",
    )
    .await;
    assert_eq!(count, 1);
}

/// Returns an N-Triples document with three triples per subject.
///
/// The blank nodes are used in the entire document, such that their occurrences end up in
/// different chunks. Literals contain escaped line breaks, which must not split a chunk. The
/// document also contains comments, empty lines, and mixed line endings.
fn ntriples_with_shared_blank_nodes() -> String {
    let mut ntriples = String::from("# A comment that spans the first line.\n\n");
    for i in 0..NUM_SUBJECTS {
        let line_end = if i % 3 == 0 { "\r\n" } else { "\n" };
        let blank_node = i % NUM_BLANK_NODES;
        ntriples.push_str(&format!(
            "<http://example.org/s{i}> <http://example.org/label> \"line\\nbreak {i}\" .{line_end}\
             _:b{blank_node} <http://example.org/member> <http://example.org/s{i}> .{line_end}\
             <http://example.org/s{i}> <http://example.org/group> _:b{blank_node} .{line_end}\
             {line_end}"
        ));
    }
    ntriples
}

/// Asserts that the subject ranges of the files of the storage do not overlap (except at their
/// boundaries).
fn assert_files_do_not_overlap(storage: &ParquetQuadStorage) {
    let subject = QuadStorageEncoding::String
        .quad_schema()
        .inner()
        .index_of("subject")
        .unwrap();
    let ranges = storage
        .files()
        .iter()
        .map(|file| {
            let stats = &file.statistics.as_ref().unwrap().column_statistics[subject];
            (
                stats.min_value.get_value().unwrap().clone(),
                stats.max_value.get_value().unwrap().clone(),
            )
        })
        .collect::<Vec<_>>();
    let mut ranges = ranges;
    ranges.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
    for window in ranges.windows(2) {
        assert!(window[0].1 <= window[1].0, "Files overlap: {ranges:?}");
    }
}

/// Returns the path of the only file of the quad table at `table` whose statistics for `column`
/// contain `value`.
fn file_containing(
    storage: &ParquetQuadStorage,
    table: usize,
    column: &str,
    value: &str,
) -> String {
    let idx = QuadStorageEncoding::String
        .quad_schema()
        .inner()
        .index_of(column)
        .unwrap();
    let value = ScalarValue::from(value);
    let files = storage.quad_tables()[table]
        .files()
        .iter()
        .filter(|file| {
            let stats = &file.statistics.as_ref().unwrap().column_statistics[idx];
            stats.min_value.get_value().unwrap() <= &value
                && &value <= stats.max_value.get_value().unwrap()
        })
        .map(|file| file.path().to_string())
        .collect::<Vec<_>>();
    assert_eq!(files.len(), 1, "Expected a single file: {files:?}");
    files.into_iter().next().unwrap()
}

/// Returns the exact number of rows in each file of the storage.
fn rows_per_file(storage: &ParquetQuadStorage) -> Vec<usize> {
    storage
        .files()
        .iter()
        .map(|file| {
            let stats = file
                .statistics
                .as_ref()
                .expect("Statistics must be collected");
            match stats.num_rows {
                Precision::Exact(rows) => rows,
                _ => panic!("Expected exact row count"),
            }
        })
        .collect()
}

/// Executes the query and returns the number of solutions and the displayed execution plan.
async fn run_query(context: &RdfFusionContext, query: &str) -> (usize, String) {
    let query = rdf_fusion_sparql_parser::parse_query(
        &context.create_view(),
        query,
        &ParserOptions::default(),
    )
    .unwrap();
    let (results, explanation) = context
        .execute_query(&query, QueryOptions::default())
        .await
        .unwrap();
    let QueryResults::Solutions(mut solutions) = results else {
        panic!("Expected solutions");
    };
    let mut count = 0;
    while let Some(row) = solutions.next().await {
        row.unwrap();
        count += 1;
    }
    let plan = displayable(explanation.execution_plan.as_ref())
        .indent(true)
        .to_string();
    (count, plan)
}
