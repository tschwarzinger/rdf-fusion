use datafusion::physical_expr_common::metrics::Time;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A wrapper that measures strictly the CPU time spent actively polling.
pub struct MeasurePoll<F> {
    pub inner: Pin<Box<F>>,
    pub time_metric: Time,
}

impl<F: Future> Future for MeasurePoll<F> {
    type Output = F::Output;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let cloned_inner = self.time_metric.clone();
        let handle = cloned_inner.timer();
        let result = self.inner.as_mut().poll(cx);
        drop(handle);
        result
    }
}
