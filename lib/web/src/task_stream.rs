use datafusion::common::runtime::SpawnedTask;
use futures::Stream;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A [`Stream`] that owns the [`SpawnedTask`] producing its items.
///
/// Dropping a [`SpawnedTask`] aborts it. Bundling the task with the stream keeps the producer
/// alive while the stream is consumed and aborts it once the stream is dropped.
pub struct TaskBoundStream<S, T> {
    stream: S,
    _task: SpawnedTask<T>,
}

impl<S, T> TaskBoundStream<S, T> {
    /// Creates a new [`TaskBoundStream`].
    pub fn new(stream: S, task: SpawnedTask<T>) -> Self {
        Self {
            stream,
            _task: task,
        }
    }
}

impl<S: Stream + Unpin, T> Stream for TaskBoundStream<S, T> {
    type Item = S::Item;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.stream).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
