use crate::environment::BenchmarkContext;
use rdf_fusion::execution::cache::CacheMetrics;
use std::fs;
use std::fs::File;
use std::io::Write;

#[derive(Default, Clone, Copy)]
struct CacheMetricsSnapshot {
    pub get_opts_hits: u64,
    pub get_opts_misses: u64,
    pub get_ranges_hits: u64,
    pub get_ranges_misses: u64,
}

impl From<&CacheMetrics> for CacheMetricsSnapshot {
    fn from(metrics: &CacheMetrics) -> Self {
        Self {
            get_opts_hits: metrics.get_opts_hits(),
            get_opts_misses: metrics.get_opts_misses(),
            get_ranges_hits: metrics.get_ranges_hits(),
            get_ranges_misses: metrics.get_ranges_misses(),
        }
    }
}

pub struct CacheMetricsRecorder {
    metrics_file: Option<csv::Writer<File>>,
    initial_metrics: CacheMetricsSnapshot,
}

impl CacheMetricsRecorder {
    pub fn new(context: &BenchmarkContext<'_>) -> anyhow::Result<Self> {
        let mut metrics_file = None;
        let initial_metrics = context
            .parent()
            .metrics()
            .as_ref()
            .map(CacheMetricsSnapshot::from)
            .unwrap_or_default();

        if context.parent().options().verbose_results {
            let mut path = context.results_dir();
            path.push("cache");
            fs::create_dir_all(&path)?;

            path.push("hit_rate_acc.csv");
            let file = File::create(path)?;
            let mut wtr = csv::Writer::from_writer(file);
            wtr.write_record([
                "query_name",
                "get_opts_hits_acc",
                "get_opts_misses_acc",
                "get_ranges_hits_acc",
                "get_ranges_misses_acc",
                "hit_rate_acc",
            ])?;
            metrics_file = Some(wtr);
        }

        Ok(Self {
            metrics_file,
            initial_metrics,
        })
    }

    pub fn record_run(
        &mut self,
        context: &BenchmarkContext<'_>,
        query_name: &str,
    ) -> anyhow::Result<()> {
        if let Some(ref mut wtr) = self.metrics_file {
            let current_metrics_raw = context.parent().metrics();
            let current_metrics = current_metrics_raw
                .as_ref()
                .map(CacheMetricsSnapshot::from)
                .unwrap_or_default();

            let acc_opts_hits =
                current_metrics.get_opts_hits - self.initial_metrics.get_opts_hits;
            let acc_opts_misses =
                current_metrics.get_opts_misses - self.initial_metrics.get_opts_misses;
            let acc_ranges_hits =
                current_metrics.get_ranges_hits - self.initial_metrics.get_ranges_hits;
            let acc_ranges_misses = current_metrics.get_ranges_misses
                - self.initial_metrics.get_ranges_misses;

            let total_requests =
                acc_opts_hits + acc_opts_misses + acc_ranges_hits + acc_ranges_misses;
            let hit_rate = if total_requests > 0 {
                (acc_opts_hits + acc_ranges_hits) as f64 / total_requests as f64
            } else {
                0.0
            };

            wtr.write_record([
                query_name,
                &acc_opts_hits.to_string(),
                &acc_opts_misses.to_string(),
                &acc_ranges_hits.to_string(),
                &acc_ranges_misses.to_string(),
                &format!("{hit_rate:.4}"),
            ])?;
        }
        Ok(())
    }

    pub fn write_summary(&self, context: &BenchmarkContext<'_>) -> anyhow::Result<()> {
        if context.parent().options().verbose_results {
            let mut path = context.results_dir();
            path.push("cache");
            path.push("summary.txt");

            let metrics_raw = context.parent().metrics();
            let mut file = File::create(path)?;

            if let Some(metrics) = metrics_raw {
                writeln!(file, "Cache Summary Report")?;
                writeln!(file, "====================")?;
                writeln!(file, "Number of Evictions: {}", metrics.eviction_count())?;
                writeln!(
                    file,
                    "Total Cache Size (Entries): {}",
                    metrics.data_cache_size()
                )?;
                writeln!(
                    file,
                    "Total Cache Weight (Bytes): {}",
                    metrics.data_cache_weight()
                )?;
                writeln!(
                    file,
                    "Total Hits: {}",
                    metrics.get_opts_hits() + metrics.get_ranges_hits()
                )?;
                writeln!(
                    file,
                    "Total Misses: {}",
                    metrics.get_opts_misses() + metrics.get_ranges_misses()
                )?;
            } else {
                writeln!(file, "No cache metrics available.")?;
            }
        }
        Ok(())
    }
}
