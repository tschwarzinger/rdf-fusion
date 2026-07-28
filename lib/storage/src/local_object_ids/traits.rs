use super::OwnedTermTuple;
use crate::local_object_ids::error::LocalObjectIdError;
use async_trait::async_trait;
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use rdf_fusion_encoding::plain_term::{PlainTermArray, PlainTermScalar};
use std::collections::HashMap;
use std::sync::Arc;

#[async_trait]
pub trait LocalObjectIdDictionary: Send + Sync + std::fmt::Debug {
    async fn snapshot(
        &self,
    ) -> Result<Arc<dyn LocalObjectIdDictionarySnapshot>, LocalObjectIdError>;

    async fn transaction(
        &self,
    ) -> Result<Box<dyn LocalObjectIdTransaction>, LocalObjectIdError>;
}

#[async_trait]
pub trait LocalObjectIdDictionarySnapshot: Send + Sync {
    async fn resolve_plain_terms(
        &self,
        ids: &Int64Array,
    ) -> Result<ArrayRef, LocalObjectIdError>;

    async fn len(&self) -> Result<u64, LocalObjectIdError>;

    async fn read_claimed_object_ids(
        &self,
    ) -> Result<Option<(i64, i64)>, LocalObjectIdError>;

    async fn is_empty(&self) -> Result<bool, LocalObjectIdError>;

    async fn get_id_by_term(&self, term: &PlainTermScalar) -> Option<i64>;

    async fn get_synced_version(&self) -> Result<Option<u64>, LocalObjectIdError>;
}

#[async_trait]
pub trait LocalObjectIdTransaction: Send + Sync {
    async fn encode_array(
        &mut self,
        array: &PlainTermArray,
    ) -> Result<Int64Array, LocalObjectIdError>;

    async fn add_global_batch(
        &mut self,
        batch: &RecordBatch,
    ) -> Result<(), LocalObjectIdError>;

    async fn commit(
        self: Box<Self>,
        delta_version: u64,
    ) -> Result<(), LocalObjectIdError>;

    async fn abort(self: Box<Self>) -> Result<(), LocalObjectIdError>;

    fn pending_ids(&self) -> &HashMap<i64, Arc<OwnedTermTuple>>;
}
