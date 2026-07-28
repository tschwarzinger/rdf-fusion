use crate::delta::objectids::DeltaObjectIdDictionary;
use crate::delta::objectids::encoding::writer::{
    DeltaObjectIdDictionaryWriter, EncodeSharedResult, ForceCommitResult, TxnOutcome,
};
use crate::local_object_ids::LocalObjectIdTransaction;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Fields, SchemaRef};
use datafusion::common::{DataFusionError, exec_datafusion_err};
use datafusion::execution::SessionState;
use datafusion::physical_plan::{RecordBatchStream, SendableRecordBatchStream};
use deltalake::arrow::datatypes::Schema;
use futures::future::BoxFuture;
use futures::{Stream, StreamExt, ready};
use rdf_fusion_common::DFResult;
use rdf_fusion_encoding::object_id::ObjectIdDictionary;
use rdf_fusion_encoding::plain_term::PlainTermArray;
use rdf_fusion_encoding::string::StringTermArray;
use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::sync::watch;

/// The state machine for the `ObjectIdEncodingStream`.
enum EncoderStreamState {
    /// Initializing the intended delta version.
    AwaitingInitialVersion(BoxFuture<'static, u64>),
    /// Ready to pull a new batch or check if the current transaction needs to be flushed.
    ReadyToProcess,
    /// Currently waiting for a batch to finish encoding asynchronously.
    AwaitingEncoding(BoxFuture<'static, DFResult<(EncodeSharedResult, RecordBatch)>>),
    /// Currently waiting for a forced commit.
    AwaitingForceCommit(BoxFuture<'static, DFResult<ForceCommitResult>>),
    /// Currently waiting for the active transaction to commit to the dictionary.
    AwaitingDictionaryDeltaCommit(
        BoxFuture<'static, DFResult<bool>>,
        watch::Sender<Option<TxnOutcome>>,
    ),
    /// A transaction conflict occurred. Waiting to sync the local dictionary before retrying.
    AwaitingLocalDictionaryUpdate(
        BoxFuture<'static, DFResult<u64>>,
        watch::Sender<Option<TxnOutcome>>,
    ),
    /// The stream has fully exhausted its input and all transactions have been committed.
    Done,
}

/// A stream that encodes plain term or string arrays into object id arrays.
pub struct ObjectIdEncodingStream {
    input: SendableRecordBatchStream,
    mapping: Arc<DeltaObjectIdDictionary>,
    writer: Arc<DeltaObjectIdDictionaryWriter>,
    schema: SchemaRef,
    session_state: SessionState,

    // --- State Buffers ---
    retry_queue: VecDeque<RecordBatch>,
    active_txn_raw_batches: Vec<RecordBatch>,
    active_txn_encoded_batches: Vec<RecordBatch>,
    ready_to_yield_batches: VecDeque<RecordBatch>,

    // --- Transaction State ---
    intended_delta_version: u64,
    max_buffered_rows: usize,
    max_buffered_ids: usize,

    state: EncoderStreamState,
    is_exhausted: bool,
}

impl ObjectIdEncodingStream {
    pub fn new(
        input: SendableRecordBatchStream,
        mapping: Arc<DeltaObjectIdDictionary>,
        max_buffered_rows: usize,
        max_buffered_ids: usize,
        session_state: SessionState,
    ) -> Self {
        let encoded_type = mapping.object_id_data_type().term_type();
        let fields = input
            .schema()
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_data_type(encoded_type.clone()))
            .collect::<Fields>();
        let schema = Arc::new(Schema::new(fields));

        let writer = mapping.shared_writer();
        let writer_clone = Arc::clone(&writer);

        Self {
            input,
            mapping,
            writer,
            schema,
            session_state,
            retry_queue: VecDeque::new(),
            active_txn_raw_batches: Vec::new(),
            active_txn_encoded_batches: Vec::new(),
            ready_to_yield_batches: VecDeque::new(),
            intended_delta_version: 0,
            max_buffered_rows,
            max_buffered_ids,
            state: EncoderStreamState::AwaitingInitialVersion(Box::pin(async move {
                writer_clone.active_delta_version().await
            })),
            is_exhausted: false,
        }
    }

    /// Checks whether limits have been reached and the active transaction should be committed.
    fn should_force_commit(&self) -> bool {
        let pending_rows = self
            .active_txn_encoded_batches
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        let is_input_empty = self.is_exhausted && self.retry_queue.is_empty();
        let reached_row_limit = pending_rows >= self.max_buffered_rows;

        // Note: reached_id_limit is checked by the writer during encode.
        (is_input_empty || reached_row_limit) && pending_rows > 0
    }

    /// Handles the success path: migrating encoded batches to the output buffer and resetting counters.
    fn handle_successful_commit(&mut self) {
        let unflushed = std::mem::take(&mut self.active_txn_encoded_batches);
        self.ready_to_yield_batches.extend(unflushed);

        self.active_txn_raw_batches.clear();
        self.intended_delta_version += 1;

        if self.is_exhausted && self.retry_queue.is_empty() {
            self.state = EncoderStreamState::Done;
        } else {
            self.state = EncoderStreamState::ReadyToProcess;
        }
    }

    /// Handles the conflict path: transferring raw batches to the retry queue and wiping invalid encoded ones.
    fn handle_conflict(&mut self, next_version: u64) {
        let failed_batches = std::mem::take(&mut self.active_txn_raw_batches);
        self.retry_queue.extend(failed_batches);

        self.active_txn_encoded_batches.clear();
        self.intended_delta_version = next_version;

        self.state = EncoderStreamState::ReadyToProcess;
    }

    // --- Async Future Generators ---

    fn create_encoding_future(
        writer: Arc<DeltaObjectIdDictionaryWriter>,
        txn_id: u64,
        batch: RecordBatch,
        max_buffered_ids: usize,
    ) -> BoxFuture<'static, DFResult<(EncodeSharedResult, RecordBatch)>> {
        Box::pin(async move {
            let mut plain_arrays = Vec::with_capacity(batch.num_columns());

            for i in 0..batch.num_columns() {
                let column = batch.column(i);

                let plain_array = if column.data_type() == &DataType::Utf8 {
                    StringTermArray::new_unchecked(Arc::clone(column))
                        .as_plain_term_array()
                        .map_err(|e| {
                            exec_datafusion_err!(
                                "Failed to convert String to PlainTerm: {}",
                                e
                            )
                        })?
                } else {
                    PlainTermArray::try_from(Arc::clone(column)).map_err(|e| {
                        exec_datafusion_err!("Failed to convert to PlainTerm: {}", e)
                    })?
                };
                plain_arrays.push(plain_array);
            }

            let array_refs: Vec<&PlainTermArray> = plain_arrays.iter().collect();

            let result = writer
                .encode_shared(txn_id, &array_refs, max_buffered_ids)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            Ok((result, batch))
        })
    }

    fn create_commit_future(
        mapping: Arc<DeltaObjectIdDictionary>,
        txn: Box<dyn LocalObjectIdTransaction>,
    ) -> BoxFuture<'static, DFResult<bool>> {
        Box::pin(async move {
            let success = mapping
                .commit_dictionary_transaction_to_delta(txn.as_ref())
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            if success {
                let delta_version = mapping.delta_version().await;
                txn.commit(delta_version)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(true)
            } else {
                txn.abort()
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(false)
            }
        })
    }

    fn create_sync_future(
        mapping: Arc<DeltaObjectIdDictionary>,
        writer: Arc<DeltaObjectIdDictionaryWriter>,
        session_state: SessionState,
    ) -> BoxFuture<'static, DFResult<u64>> {
        Box::pin(async move {
            mapping
                .update_local_dictionary(&session_state)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let new_version = mapping.delta_version().await;
            writer.sync_active_version(new_version + 1).await;
            Ok(new_version + 1)
        })
    }
}

impl Stream for ObjectIdEncodingStream {
    type Item = DFResult<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        loop {
            let writer = Arc::clone(&self.writer);
            let mapping = Arc::clone(&self.mapping);
            let schema = Arc::clone(&self.schema);
            let max_ids = self.max_buffered_ids;
            let session_state = self.session_state.clone();
            let intended_delta_version = self.intended_delta_version;
            if let Some(batch) = self.ready_to_yield_batches.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }

            match &mut self.state {
                EncoderStreamState::AwaitingInitialVersion(fut) => {
                    self.intended_delta_version = ready!(fut.as_mut().poll(cx));
                    self.state = EncoderStreamState::ReadyToProcess;
                }

                EncoderStreamState::ReadyToProcess => {
                    if let Some(batch) = self.retry_queue.pop_front() {
                        self.active_txn_raw_batches.push(batch.clone());
                        self.state = EncoderStreamState::AwaitingEncoding(
                            Self::create_encoding_future(
                                writer,
                                intended_delta_version,
                                batch,
                                max_ids,
                            ),
                        );
                        continue;
                    }

                    if self.should_force_commit() {
                        self.state = EncoderStreamState::AwaitingForceCommit(Box::pin(
                            async move {
                                writer
                                    .force_commit(intended_delta_version)
                                    .await
                                    .map_err(|e| DataFusionError::External(Box::new(e)))
                            },
                        ));
                        continue;
                    }

                    if self.is_exhausted {
                        self.state = EncoderStreamState::Done;
                        continue;
                    }

                    match ready!(self.input.poll_next_unpin(cx)) {
                        Some(Ok(b)) => {
                            self.active_txn_raw_batches.push(b.clone());
                            self.state = EncoderStreamState::AwaitingEncoding(
                                Self::create_encoding_future(
                                    writer,
                                    intended_delta_version,
                                    b,
                                    max_ids,
                                ),
                            );
                        }
                        Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                        None => {
                            self.is_exhausted = true;
                            continue;
                        }
                    }
                }

                EncoderStreamState::AwaitingForceCommit(fut) => {
                    match ready!(fut.as_mut().poll(cx))? {
                        ForceCommitResult::TriggeredCommit { txn, sender, .. } => {
                            self.state =
                                EncoderStreamState::AwaitingDictionaryDeltaCommit(
                                    Self::create_commit_future(mapping, txn),
                                    sender,
                                );
                        }
                        ForceCommitResult::Closed(TxnOutcome::Committed) => {
                            self.handle_successful_commit();
                        }
                        ForceCommitResult::Closed(TxnOutcome::Conflicted) => {
                            let next_version = self.intended_delta_version + 1;
                            self.handle_conflict(next_version);
                        }
                    }
                }

                EncoderStreamState::AwaitingEncoding(fut) => {
                    let (result, _raw_batch) = ready!(fut.as_mut().poll(cx))?;

                    match result {
                        EncodeSharedResult::Buffering { encoded, .. } => {
                            let encoded_columns =
                                encoded.into_iter().map(|a| Arc::new(a) as _).collect();
                            let res_batch = RecordBatch::try_new(
                                Arc::clone(&schema),
                                encoded_columns,
                            )
                            .map_err(|e| {
                                exec_datafusion_err!("Batch creation failed: {}", e)
                            })?;
                            self.active_txn_encoded_batches.push(res_batch);
                            self.state = EncoderStreamState::ReadyToProcess;
                        }
                        EncodeSharedResult::TriggeredCommit {
                            encoded,
                            txn,
                            sender,
                            ..
                        } => {
                            let encoded_columns =
                                encoded.into_iter().map(|a| Arc::new(a) as _).collect();
                            let res_batch = RecordBatch::try_new(
                                Arc::clone(&schema),
                                encoded_columns,
                            )
                            .map_err(|e| {
                                exec_datafusion_err!("Batch creation failed: {}", e)
                            })?;
                            self.active_txn_encoded_batches.push(res_batch);

                            self.state =
                                EncoderStreamState::AwaitingDictionaryDeltaCommit(
                                    Self::create_commit_future(mapping, txn),
                                    sender,
                                );
                        }
                        EncodeSharedResult::Closed(TxnOutcome::Committed) => {
                            let failed_batch = self.active_txn_raw_batches.pop().unwrap();
                            self.retry_queue.push_front(failed_batch);
                            self.handle_successful_commit();
                        }
                        EncodeSharedResult::Closed(TxnOutcome::Conflicted) => {
                            let next_version = self.intended_delta_version + 1;
                            self.handle_conflict(next_version);
                        }
                    }
                }

                EncoderStreamState::AwaitingDictionaryDeltaCommit(fut, sender) => {
                    let success = ready!(fut.as_mut().poll(cx))?;

                    if success {
                        let _ = sender.send(Some(TxnOutcome::Committed));
                        self.handle_successful_commit();
                    } else {
                        self.state = EncoderStreamState::AwaitingLocalDictionaryUpdate(
                            Self::create_sync_future(
                                mapping,
                                writer,
                                session_state.clone(),
                            ),
                            sender.clone(),
                        );
                    }
                }

                EncoderStreamState::AwaitingLocalDictionaryUpdate(fut, sender) => {
                    let new_version = ready!(fut.as_mut().poll(cx))?;
                    let _ = sender.send(Some(TxnOutcome::Conflicted));
                    self.handle_conflict(new_version);
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
