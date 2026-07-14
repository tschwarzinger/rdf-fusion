use crate::delta::objectids::DeltaObjectIdDictionary;
use crate::local_object_ids::LocalObjectIdTransaction;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::{DataType, Fields, SchemaRef};
use datafusion::common::{DataFusionError, exec_datafusion_err};
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

/// The outcome of attempting to commit a dictionary transaction.
enum CommitResult {
    Success,
    Conflict,
}

/// The state machine for the `ObjectIdEncodingStream`.
enum EncoderStreamState {
    /// Ready to pull a new batch or check if the current transaction needs to be flushed.
    ReadyToProcess,
    /// Currently waiting for a new dictionary transaction to be initialized.
    AwaitingDictionaryTransactionInit(
        BoxFuture<'static, DFResult<LocalObjectIdTransaction>>,
    ),
    /// Currently waiting for a batch to finish encoding asynchronously.
    AwaitingEncoding(
        BoxFuture<'static, DFResult<(LocalObjectIdTransaction, RecordBatch)>>,
    ),
    /// Currently waiting for the active transaction to commit to the dictionary.
    AwaitingDictionaryDeltaCommit(BoxFuture<'static, DFResult<CommitResult>>),
    /// A transaction conflict occurred. Waiting to sync the local dictionary before retrying.
    AwaitingLocalDictionaryUpdate(BoxFuture<'static, DFResult<()>>),
    /// The stream has fully exhausted its input and all transactions have been committed.
    Done,
}

/// A stream that encodes plain term or string arrays into object id arrays.
pub struct ObjectIdEncodingStream {
    input: SendableRecordBatchStream,
    mapping: Arc<DeltaObjectIdDictionary>,
    schema: SchemaRef,

    // --- State Buffers ---
    retry_queue: VecDeque<RecordBatch>,
    active_txn_raw_batches: Vec<RecordBatch>,
    active_txn_encoded_batches: Vec<RecordBatch>,
    ready_to_yield_batches: VecDeque<RecordBatch>,

    // --- Transaction State ---
    current_txn: Option<LocalObjectIdTransaction>,
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
            schema,
            retry_queue: VecDeque::new(),
            active_txn_raw_batches: Vec::new(),
            active_txn_encoded_batches: Vec::new(),
            ready_to_yield_batches: VecDeque::new(),
            current_txn: None,
            max_buffered_rows,
            max_buffered_ids,
            state: EncoderStreamState::ReadyToProcess,
            is_exhausted: false,
        }
    }

    /// Checks whether limits have been reached and the active transaction should be committed.
    fn should_commit_dictionary(&self) -> bool {
        let Some(current_txn) = &self.current_txn else {
            return false;
        };

        let pending_rows = self
            .active_txn_encoded_batches
            .iter()
            .map(|b| b.num_rows())
            .sum::<usize>();
        let is_input_empty = self.is_exhausted && self.retry_queue.is_empty();
        let reached_row_limit = pending_rows > self.max_buffered_rows;
        let reached_id_limit = current_txn.pending_ids().len() > self.max_buffered_ids;

        is_input_empty || reached_row_limit || reached_id_limit
    }

    /// Handles the success path: migrating encoded batches to the output buffer and resetting counters.
    fn handle_successful_commit(&mut self) {
        let unflushed = std::mem::take(&mut self.active_txn_encoded_batches);
        self.ready_to_yield_batches.extend(unflushed);

        self.active_txn_raw_batches.clear();

        if self.is_exhausted && self.retry_queue.is_empty() {
            self.state = EncoderStreamState::Done;
        } else {
            self.state = EncoderStreamState::ReadyToProcess;
        }
    }

    /// Handles the conflict path: transferring raw batches to the retry queue and wiping invalid encoded ones.
    fn handle_conflict_sync(&mut self) {
        let failed_batches = std::mem::take(&mut self.active_txn_raw_batches);
        self.retry_queue.extend(failed_batches);

        self.active_txn_encoded_batches.clear();

        self.state = EncoderStreamState::ReadyToProcess;
    }

    // --- Async Future Generators ---

    fn create_transaction_future(
        mapping: Arc<DeltaObjectIdDictionary>,
    ) -> BoxFuture<'static, DFResult<LocalObjectIdTransaction>> {
        Box::pin(async move {
            mapping
                .dictionary()
                .transaction()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))
        })
    }

    fn create_encoding_future(
        mut txn: LocalObjectIdTransaction,
        batch: RecordBatch,
        schema: SchemaRef,
    ) -> BoxFuture<'static, DFResult<(LocalObjectIdTransaction, RecordBatch)>> {
        Box::pin(async move {
            let mut encoded_columns = Vec::with_capacity(batch.num_columns());

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

                let encoded = txn
                    .encode_array(&plain_array)
                    .await
                    .map_err(|e| exec_datafusion_err!("Encoding failed: {}", e))?;

                encoded_columns.push(Arc::new(encoded) as _);
            }

            let res_batch = RecordBatch::try_new(schema, encoded_columns)
                .map_err(|e| exec_datafusion_err!("Batch creation failed: {}", e))?;

            Ok((txn, res_batch))
        })
    }

    fn create_commit_future(
        mapping: Arc<DeltaObjectIdDictionary>,
        txn: LocalObjectIdTransaction,
    ) -> BoxFuture<'static, DFResult<CommitResult>> {
        Box::pin(async move {
            let success = mapping
                .commit_dictionary_transaction_to_delta(&txn)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            if success {
                txn.commit()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(CommitResult::Success)
            } else {
                txn.abort()
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                Ok(CommitResult::Conflict)
            }
        })
    }

    fn create_sync_future(
        mapping: Arc<DeltaObjectIdDictionary>,
    ) -> BoxFuture<'static, DFResult<()>> {
        Box::pin(async move {
            mapping
                .update_local_dictionary()
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            Ok(())
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
            // Yield any fully ready-to-go batches to the consumer
            if let Some(batch) = self.ready_to_yield_batches.pop_front() {
                return Poll::Ready(Some(Ok(batch)));
            }

            match &mut self.state {
                EncoderStreamState::ReadyToProcess => {
                    // 1. If limits are reached, commit the current transaction.
                    if self.should_commit_dictionary() {
                        let txn = self.current_txn.take().unwrap();
                        let mapping = Arc::clone(&self.mapping);

                        self.state = EncoderStreamState::AwaitingDictionaryDeltaCommit(
                            Self::create_commit_future(mapping, txn),
                        );
                        continue;
                    }

                    // 2. Otherwise, grab the next batch to process.
                    let batch = if let Some(b) = self.retry_queue.pop_front() {
                        b
                    } else {
                        if self.is_exhausted {
                            self.state = EncoderStreamState::Done;
                            continue;
                        }

                        match ready!(self.input.poll_next_unpin(cx)) {
                            Some(Ok(b)) => b,
                            Some(Err(e)) => return Poll::Ready(Some(Err(e))),
                            None => {
                                self.is_exhausted = true;
                                continue;
                            }
                        }
                    };

                    // 3. If we grabbed a batch but don't have a transaction, initialize one!
                    if self.current_txn.is_none() {
                        // Put the batch back to process it once the transaction is ready
                        self.retry_queue.push_front(batch);

                        let mapping = Arc::clone(&self.mapping);
                        self.state =
                            EncoderStreamState::AwaitingDictionaryTransactionInit(
                                Self::create_transaction_future(mapping),
                            );
                        continue;
                    }

                    // 4. We have a batch and an active transaction. Start encoding.
                    self.active_txn_raw_batches.push(batch.clone());

                    let txn = self.current_txn.take().unwrap();
                    let schema = Arc::clone(&self.schema);

                    self.state = EncoderStreamState::AwaitingEncoding(
                        Self::create_encoding_future(txn, batch, schema),
                    );
                }

                EncoderStreamState::AwaitingDictionaryTransactionInit(fut) => {
                    let txn = ready!(fut.as_mut().poll(cx))?;
                    self.current_txn = Some(txn);
                    self.state = EncoderStreamState::ReadyToProcess;
                }

                EncoderStreamState::AwaitingEncoding(fut) => {
                    let (txn, encoded_batch) = ready!(fut.as_mut().poll(cx))?;

                    self.current_txn = Some(txn);
                    self.active_txn_encoded_batches.push(encoded_batch);
                    self.state = EncoderStreamState::ReadyToProcess;
                }

                EncoderStreamState::AwaitingDictionaryDeltaCommit(fut) => {
                    match ready!(fut.as_mut().poll(cx))? {
                        CommitResult::Success => {
                            // current_txn is already None. We will lazily create a new one.
                            self.handle_successful_commit();
                        }
                        CommitResult::Conflict => {
                            let mapping = Arc::clone(&self.mapping);
                            self.state =
                                EncoderStreamState::AwaitingLocalDictionaryUpdate(
                                    Self::create_sync_future(mapping),
                                );
                        }
                    }
                }

                EncoderStreamState::AwaitingLocalDictionaryUpdate(fut) => {
                    ready!(fut.as_mut().poll(cx))?;
                    // current_txn is still None. Dictionary is updated.
                    self.handle_conflict_sync();
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
