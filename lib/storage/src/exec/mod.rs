mod verify_not_null;

pub use verify_not_null::VerifyNotNullExec;

use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_plan::execution_plan::SchedulingType;
use datafusion::physical_plan::metrics::{Metric, MetricValue, MetricsSet};
use std::borrow::Cow;
use std::sync::Arc;

/// Recursively find and alias target metrics from an inner execution plan.
///
/// This function looks for metrics with specific prefixes (e.g., related to Parquet scanning)
/// and aliases them with an `index_` prefix to make them visible in the outer scan's metrics.
pub fn extract_and_alias_inner_metrics(
    plan: &Arc<dyn ExecutionPlan>,
    set: &mut MetricsSet,
) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            let name_opt = match metric.value() {
                MetricValue::Count { name, .. } => Some(name.as_ref()),
                MetricValue::Time { name, .. } => Some(name.as_ref()),
                MetricValue::Gauge { name, .. } => Some(name.as_ref()),
                MetricValue::PruningMetrics { name, .. } => Some(name.as_ref()),
                _ => None,
            };

            if let Some(name) = name_opt {
                // Using `starts_with` handles DataFusion's implicit `_matched`
                // and `_total` suffixes for pruning metrics transparently.
                let target_prefixes = [
                    "time_elapsed_processing",
                    "time_elapsed_opening",
                    "files_pruned",
                    "files_scanned",
                    "row_groups_pruned_statistics",
                    "page_index_rows_pruned",
                    "elapsed_compute",
                ];

                if target_prefixes
                    .iter()
                    .any(|prefix| name.starts_with(prefix))
                {
                    // If it's the inner AggregateExec elapsed_compute, we rename it to deduplicate_compute
                    let new_name: Cow<'static, str> =
                        if name == "elapsed_compute" && plan.name() == "AggregateExec" {
                            "deduplicate_compute".into()
                        } else if name == "elapsed_compute" {
                            continue; // Do not alias elapsed_compute for other operators
                        } else {
                            name.to_string().into()
                        };

                    let partition = metric.partition().unwrap_or(0);
                    let final_name: Cow<'static, str> =
                        format!("file_{partition}_{new_name}").into();

                    // Clone the underlying atomic references so the new metric updates automatically
                    let new_value = match metric.value() {
                        MetricValue::Count { count, .. } => MetricValue::Count {
                            name: final_name,
                            count: count.clone(),
                        },
                        MetricValue::Time { time, .. } => MetricValue::Time {
                            name: final_name,
                            time: time.clone(),
                        },
                        MetricValue::Gauge { gauge, .. } => MetricValue::Gauge {
                            name: final_name,
                            gauge: gauge.clone(),
                        },
                        MetricValue::PruningMetrics {
                            pruning_metrics, ..
                        } => MetricValue::PruningMetrics {
                            name: final_name,
                            pruning_metrics: pruning_metrics.clone(),
                        },
                        _ => unreachable!(),
                    };

                    // Push the newly aliased metric
                    set.push(Arc::new(Metric::new(new_value, metric.partition())));
                }
            }
        }
    }

    // Recurse down the execution plan tree
    for child in plan.children() {
        extract_and_alias_inner_metrics(child, set);
    }
}

/// Checks whether each path in the execution plan contains a cooperative execution plan.
pub fn is_cooperative_on_all_paths(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if plan.properties().scheduling_type == SchedulingType::Cooperative {
        return true;
    }

    plan.children()
        .iter()
        .all(|child| is_cooperative_on_all_paths(child))
}
