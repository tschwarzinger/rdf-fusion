use crate::environment::BenchmarkContext;

// TODO: Caching will be handled in a separate PR.
// The previous implementation was removed as per PR feedback.

pub struct CacheMetricsRecorder {}

impl CacheMetricsRecorder {
    pub fn new(_context: &BenchmarkContext<'_>) -> anyhow::Result<Self> {
        Ok(Self {})
    }

    pub fn record_run(
        &mut self,
        _context: &BenchmarkContext<'_>,
        _query_name: &str,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn write_summary(&self, _context: &BenchmarkContext<'_>) -> anyhow::Result<()> {
        Ok(())
    }
}
