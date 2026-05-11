use crate::rdf_files::RdfParserOptions;
use crate::rdf_files::UrlRdfParserTableProvider;
use datafusion::catalog::TableProvider;
use datafusion::datasource::memory::MemTable;
use datafusion::error::DataFusionError;
use datafusion::execution::context::SessionState;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::OnceCell;

/// The URL and options for a single RDF file.
type RdfFileKey = (String, RdfParserOptions);

/// The cached value for a single RDF file.
type RdfFileValue = Arc<OnceCell<Arc<MemTable>>>;

/// A manager for RDF files that handles parsing and caching of results.
#[derive(Clone, Default, Debug)]
pub struct RdfFileManager {
    /// Caches the results of parsing RDF dumps as [`MemTable`]s.
    /// Keyed by (url, options).
    cache: Arc<Mutex<HashMap<RdfFileKey, RdfFileValue>>>,
}

impl RdfFileManager {
    /// Creates a new [`RdfFileManager`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets or parses the given URL with the given options.
    pub async fn get_or_parse(
        &self,
        url: String,
        options: RdfParserOptions,
        state: &SessionState,
    ) -> Result<Arc<MemTable>, DataFusionError> {
        let key = (url.clone(), options.clone());
        let cell = {
            let mut cache = self.cache.lock().unwrap();
            let value = cache
                .entry(key)
                .or_insert_with(|| Arc::new(OnceCell::new()));
            Arc::clone(value)
        };

        cell.get_or_try_init(|| async {
            // Not in cache, parse it.
            let provider = UrlRdfParserTableProvider::try_new(url, options)?;
            let plan = provider.scan(state, None, &[], None).await?;
            let batches =
                datafusion::physical_plan::collect(plan, state.task_ctx()).await?;

            let schema = provider.schema();
            let mem_table = Arc::new(MemTable::try_new(schema, vec![batches])?);
            Ok(mem_table)
        })
        .await
        .map(Arc::clone)
    }
}
