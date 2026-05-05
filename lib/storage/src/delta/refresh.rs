use crate::delta::error::DeltaQuadStorageError;
use datafusion::common::instant::Instant;
use deltalake::DeltaTable;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Mutex, RwLock};

/// Manages refreshing a Delta Table based on a maximum age.
pub struct DeltaTableRefresher {
    max_age: RwLock<Option<Duration>>,
    /// The timestamp of the last *successful* refresh.
    last_success: RwLock<Instant>,
    /// Serializes refresh attempts so only one network call happens at a time.
    refresh_mutex: Mutex<()>,
}

impl DeltaTableRefresher {
    /// Creates a new [`DeltaTableRefresher`].
    pub fn new(max_age: Option<Duration>) -> Self {
        Self {
            max_age: RwLock::new(max_age),
            // Default to now, or use a past Instant if you want to force immediate load
            last_success: RwLock::new(Instant::now()),
            refresh_mutex: Mutex::new(()),
        }
    }

    /// Updates the maximum age.
    pub async fn set_max_age(&self, max_age: Option<Duration>) {
        *self.max_age.write().await = max_age;
    }

    /// Ensures that the table is fresh enough for the query that arrived at the given `arrival_time`.
    pub async fn ensure_fresh(
        &self,
        arrival_time: Instant,
        table_ref: &Arc<RwLock<DeltaTable>>,
    ) -> Result<(), DeltaQuadStorageError> {
        let max_age = *self.max_age.read().await;
        let Some(max_age) = max_age else {
            return Ok(());
        };

        // Fast path: check if currently fresh enough without locking the mutex.
        if self.is_fresh(arrival_time, max_age).await {
            return Ok(());
        }

        // Double-check freshness inside the lock.
        let _refresh_guard = self.refresh_mutex.lock().await;
        if self.is_fresh(arrival_time, max_age).await {
            return Ok(());
        }

        let start_time = Instant::now();
        let mut local_table = table_ref.read().await.clone();
        local_table.load().await?;

        let mut main_table = table_ref.write().await;
        if local_table.version() >= main_table.version() {
            *main_table = local_table;
        }

        // Update the success timestamp ONLY if it succeeds.
        *self.last_success.write().await = start_time;
        Ok(())
    }

    /// Checks whether the current last successful refresh is considered fresh by the given
    /// arguments.
    async fn is_fresh(&self, arrival_time: Instant, max_age: Duration) -> bool {
        let last = *self.last_success.read().await;
        last >= arrival_time || arrival_time.duration_since(last) <= max_age
    }
}
