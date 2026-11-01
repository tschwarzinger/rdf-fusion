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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use tokio::sync::mpsc;
    use tokio_stream::wrappers::ReceiverStream;

    fn spawn_producer(tx: mpsc::Sender<u32>) -> SpawnedTask<()> {
        SpawnedTask::spawn(async move {
            for i in 0..10 {
                if tx.send(i).await.is_err() {
                    return;
                }
            }
        })
    }

    #[tokio::test]
    async fn test_task_bound_stream_yields_all_items() {
        let (tx, rx) = mpsc::channel(1);
        let task = spawn_producer(tx);

        let stream = TaskBoundStream::new(ReceiverStream::new(rx), task);
        let items = stream.collect::<Vec<_>>().await;

        assert_eq!(items, (0..10).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn test_dropping_task_handle_ends_stream_early() {
        // Documents the failure mode that `TaskBoundStream` prevents.
        let (tx, rx) = mpsc::channel(1);
        drop(spawn_producer(tx));

        let items = ReceiverStream::new(rx).collect::<Vec<_>>().await;

        // The task is aborted before it is ever polled on the current-thread test runtime.
        assert!(items.is_empty());
    }

    #[tokio::test]
    async fn test_dropping_stream_aborts_task() {
        let (tx, rx) = mpsc::channel(1);
        let producer_tx = tx.clone();
        // Keeps its sender forever, so only aborting the task drops it. A producer that exits once
        // the receiver is gone would make this test pass without the abort.
        let task = SpawnedTask::spawn(async move {
            producer_tx.send(0).await.expect("receiver should be alive");
            std::future::pending::<()>().await;
        });

        let mut stream = TaskBoundStream::new(ReceiverStream::new(rx), task);
        assert_eq!(stream.next().await, Some(0));
        drop(stream);

        // Once the task is aborted, it drops its sender, so only `tx` remains.
        for _ in 0..100 {
            if tx.strong_count() == 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
        panic!("Producer task should be aborted once the stream is dropped");
    }
}
