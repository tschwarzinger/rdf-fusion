use crate::delta::objectids::DeltaObjectIdMapping;
use datafusion::arrow::array::{ArrayRef, RecordBatch};
use datafusion::arrow::datatypes::{Fields, SchemaRef};
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use deltalake::arrow::datatypes::Schema;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, ready};
use rdf_fusion_encoding::object_id::ObjectIdMapping;
use rdf_fusion_encoding::plain_term::PlainTermArray;
use rdf_fusion_model::DFResult;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicI64;
use std::task::{Context, Poll};

/// The state of the [`ObjectIdEncodingStream`].
enum EncoderStreamState {
    /// Encode the inner stream and pass it to the consumers.
    EncodeStream,
    /// Holds the future for flushing the dictionary to Delta Lake
    Flushing(BoxFuture<'static, DFResult<()>>),
    /// The stream is done, no more data will be produced.
    Done,
}

/// A stream that encodes plain term arrays into object id arrays.
pub struct ObjectIdEncodingStream {
    /// The inner stream that provides the plain term arrays.
    input: SendableRecordBatchStream,
    /// The mapping used for encoding.
    mapping: Arc<DeltaObjectIdMapping>,
    /// The number of partitions in the plan
    num_running_streams: Arc<AtomicI64>,
    /// The schema of the result
    schema: SchemaRef,
    /// The state of the stream.
    state: EncoderStreamState,
}

impl ObjectIdEncodingStream {
    /// Creates a new [`ObjectIdEncodingStream`].
    pub fn new(
        input: SendableRecordBatchStream,
        mapping: Arc<DeltaObjectIdMapping>,
        num_running_streams: Arc<AtomicI64>,
    ) -> Self {
        let encoded_type = mapping.object_id_data_type().term_type();
        let fields = input
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_data_type(encoded_type.clone()))
            .collect::<Fields>();
        let schema = Arc::new(Schema::new(fields));
        Self {
            input,
            mapping,
            num_running_streams,
            schema,
            state: EncoderStreamState::EncodeStream,
        }
    }
}

impl Stream for ObjectIdEncodingStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                EncoderStreamState::EncodeStream => {
                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(batch)) => {
                            // Extract arrays from your PlainTerm batch.
                            // Assuming column order: [graphs, subjects, predicates, objects]
                            // Adjust indices based on your exact schema layout.

                            let encoded_arrays: Result<Vec<ArrayRef>, _> = (0..4)
                                .map(|i| {
                                    let plain_array = PlainTermArray::try_from(
                                        Arc::clone(batch.column(i)),
                                    )
                                    .unwrap();
                                    self.mapping.encode_array(&plain_array).map_err(|e| {
                                        exec_datafusion_err!("Encoding failed: {}", e)
                                    })
                                })
                                .collect();

                            return match encoded_arrays {
                                Ok(arrays) => {
                                    let encoded_batch = RecordBatch::try_new(
                                        Arc::clone(&self.schema),
                                        arrays,
                                    )?;
                                    Poll::Ready(Some(Ok(encoded_batch)))
                                }
                                Err(e) => Poll::Ready(Some(Err(e))),
                            };
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            // The stream is exhausted. Transition to the flushing phase.
                            let stream_id = self
                                .num_running_streams
                                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                            let last_stream = stream_id == 1;

                            if last_stream {
                                let mapping = Arc::clone(&self.mapping);
                                let flush_future = Box::pin(async move {
                                    mapping.flush().await.map_err(|err| {
                                        DataFusionError::External(Box::new(err))
                                    })
                                });
                                self.state = EncoderStreamState::Flushing(flush_future);
                            } else {
                                self.state = EncoderStreamState::Done;
                            }
                        }
                    }
                }
                EncoderStreamState::Flushing(fut) => {
                    // Poll the flush future until it completes
                    return match ready!(fut.as_mut().poll(cx)) {
                        Ok(_) => {
                            self.state = EncoderStreamState::Done;
                            Poll::Ready(None) // Signal stream is completely finished
                        }
                        Err(e) => {
                            self.state = EncoderStreamState::Done;
                            Poll::Ready(Some(Err(e)))
                        }
                    };
                }
                EncoderStreamState::Done => return Poll::Ready(None),
            }
        }
    }
}

impl RecordBatchStream for ObjectIdEncodingStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}
