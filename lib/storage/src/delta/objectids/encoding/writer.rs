use crate::delta::error::DeltaQuadsStorageError;
use crate::local_object_ids::{LocalObjectIdDictionary, LocalObjectIdTransaction};
use datafusion::arrow::array::Int64Array;
use rdf_fusion_encoding::plain_term::PlainTermArray;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, watch};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TxnOutcome {
    Committed,
    Conflicted,
}

pub enum EncodeSharedResult {
    /// Encoded successfully, still buffering.
    Buffering {
        encoded: Vec<Int64Array>,
        txn_id: u64,
    },
    /// Encoded successfully, but this batch triggered a commit.
    TriggeredCommit {
        encoded: Vec<Int64Array>,
        txn_id: u64,
        txn: Box<dyn LocalObjectIdTransaction>,
        sender: watch::Sender<Option<TxnOutcome>>,
    },
    /// The requested txn_id is already closed. Here is the outcome.
    Closed(TxnOutcome),
}

pub enum ForceCommitResult {
    /// The commit was triggered by this call, and here is the outcome.
    TriggeredCommit {
        txn_id: u64,
        txn: Box<dyn LocalObjectIdTransaction>,
        sender: watch::Sender<Option<TxnOutcome>>,
    },
    /// The requested txn_id was already closed. Here is the outcome.
    Closed(TxnOutcome),
}

struct SharedWriterState {
    active_delta_version: u64,
    active_txn: Option<Box<dyn LocalObjectIdTransaction>>,
    active_outcome_sender: Option<watch::Sender<Option<TxnOutcome>>>,
    outcome_receivers: HashMap<u64, watch::Receiver<Option<TxnOutcome>>>,
}

pub struct DeltaObjectIdDictionaryWriter {
    local_mapping: Arc<dyn LocalObjectIdDictionary>,
    state: Mutex<SharedWriterState>,
}

impl std::fmt::Debug for DeltaObjectIdDictionaryWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DeltaObjectIdDictionaryWriter")
    }
}

impl DeltaObjectIdDictionaryWriter {
    pub fn new(
        local_mapping: Arc<dyn LocalObjectIdDictionary>,
        active_delta_version: u64,
    ) -> Self {
        let (tx, rx) = watch::channel(None);
        let mut outcome_receivers = HashMap::new();
        outcome_receivers.insert(active_delta_version, rx);

        Self {
            local_mapping,
            state: Mutex::new(SharedWriterState {
                active_delta_version,
                active_txn: None,
                active_outcome_sender: Some(tx),
                outcome_receivers,
            }),
        }
    }

    pub async fn encode_shared(
        &self,
        intended_delta_version: u64,
        arrays: &[&PlainTermArray],
        max_buffered_ids: usize,
    ) -> Result<EncodeSharedResult, DeltaQuadsStorageError> {
        let rx_to_await;

        {
            let mut state = self.state.lock().await;

            if intended_delta_version < state.active_delta_version {
                if let Some(rx) = state.outcome_receivers.get(&intended_delta_version) {
                    rx_to_await = rx.clone();
                } else {
                    return Err(DeltaQuadsStorageError::Other(format!(
                        "Unknown transaction id: {intended_delta_version}",
                    )));
                }
            } else if intended_delta_version == state.active_delta_version {
                if state.active_txn.is_none() {
                    let txn = self.local_mapping.transaction().await.map_err(|e| {
                        DeltaQuadsStorageError::Other(format!(
                            "Failed to create transaction: {e:?}",
                        ))
                    })?;
                    state.active_txn = Some(txn);
                }

                let txn = state.active_txn.as_mut().unwrap();
                let mut encoded_arrays = Vec::with_capacity(arrays.len());
                for array in arrays {
                    let encoded = txn.encode_array(array).await.map_err(|e| {
                        DeltaQuadsStorageError::Other(format!(
                            "Failed to encode array: {e:?}",
                        ))
                    })?;
                    encoded_arrays.push(encoded);
                }

                if txn.pending_ids().len() > max_buffered_ids {
                    let txn_to_commit = state.active_txn.take().unwrap();
                    let sender_to_return = state.active_outcome_sender.take().unwrap();

                    let next_delta_version = state.active_delta_version + 1;
                    state.active_delta_version = next_delta_version;

                    let (tx, rx) = watch::channel(None);
                    state.active_outcome_sender = Some(tx);
                    state.outcome_receivers.insert(next_delta_version, rx);

                    return Ok(EncodeSharedResult::TriggeredCommit {
                        encoded: encoded_arrays,
                        txn_id: intended_delta_version,
                        txn: txn_to_commit,
                        sender: sender_to_return,
                    });
                } else {
                    return Ok(EncodeSharedResult::Buffering {
                        encoded: encoded_arrays,
                        txn_id: intended_delta_version,
                    });
                }
            } else {
                return Err(DeltaQuadsStorageError::Other(format!(
                    "Intended delta version {intended_delta_version} is in the future",
                )));
            }
        } // drop lock

        let mut rx = rx_to_await;
        loop {
            let val = *rx.borrow();
            if let Some(outcome) = val {
                return Ok(EncodeSharedResult::Closed(outcome));
            }
            if rx.changed().await.is_err() {
                return Err(DeltaQuadsStorageError::Other(
                    "Sender dropped without outcome".into(),
                ));
            }
        }
    }

    pub async fn force_commit(
        &self,
        intended_delta_version: u64,
    ) -> Result<ForceCommitResult, DeltaQuadsStorageError> {
        let rx_to_await;

        {
            let mut state = self.state.lock().await;

            if intended_delta_version < state.active_delta_version {
                if let Some(rx) = state.outcome_receivers.get(&intended_delta_version) {
                    rx_to_await = rx.clone();
                } else {
                    return Err(DeltaQuadsStorageError::Other(format!(
                        "Unknown transaction id: {intended_delta_version}",
                    )));
                }
            } else if intended_delta_version == state.active_delta_version {
                if let Some(txn_to_commit) = state.active_txn.take() {
                    let sender_to_return = state.active_outcome_sender.take().unwrap();

                    let next_delta_version = state.active_delta_version + 1;
                    state.active_delta_version = next_delta_version;

                    let (tx, rx) = watch::channel(None);
                    state.active_outcome_sender = Some(tx);
                    state.outcome_receivers.insert(next_delta_version, rx);

                    return Ok(ForceCommitResult::TriggeredCommit {
                        txn_id: intended_delta_version,
                        txn: txn_to_commit,
                        sender: sender_to_return,
                    });
                } else {
                    return Ok(ForceCommitResult::Closed(TxnOutcome::Committed));
                }
            } else {
                return Err(DeltaQuadsStorageError::Other(format!(
                    "Intended delta version {intended_delta_version} is in the future",
                )));
            }
        }

        let mut rx = rx_to_await;
        loop {
            let val = *rx.borrow();
            if let Some(outcome) = val {
                return Ok(ForceCommitResult::Closed(outcome));
            }
            if rx.changed().await.is_err() {
                return Err(DeltaQuadsStorageError::Other(
                    "Sender dropped without outcome".into(),
                ));
            }
        }
    }

    pub async fn active_delta_version(&self) -> u64 {
        self.state.lock().await.active_delta_version
    }

    pub async fn sync_active_version(&self, new_version: u64) {
        let mut state = self.state.lock().await;
        if new_version > state.active_delta_version {
            state.active_delta_version = new_version;
            let (tx, rx) = watch::channel(None);
            state.active_outcome_sender = Some(tx);
            state.outcome_receivers.insert(new_version, rx);
        }
    }
}
