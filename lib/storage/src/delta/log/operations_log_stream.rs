use datafusion::arrow::datatypes::Schema;
use datafusion::execution::SendableRecordBatchStream;

/// A wrapper around a [`SendableRecordBatchStream`] that represents entries in the operations log
/// table.
#[allow(dead_code)]
pub struct OperationsLogStream {
    inner: SendableRecordBatchStream,
}

#[allow(dead_code)]
impl OperationsLogStream {
    pub const COMMIT_ID_IDX: usize = 0;
    pub const SEQ_IDX: usize = 1;
    pub const OP_IDX: usize = 2;
    pub const GRAPH_IDX: usize = 3;
    pub const SUBJECT_IDX: usize = 4;
    pub const PREDICATE_IDX: usize = 5;
    pub const OBJECT_IDX: usize = 6;

    /// Creates a new [`OperationsLogStream`] or returns an error.
    ///
    /// TODO: replace panic with error
    pub fn try_new(inner: SendableRecordBatchStream) -> Self {
        validate_schema(inner.schema().as_ref());
        Self { inner }
    }

    /// Returns a reference to the inner stream.
    pub fn inner(&self) -> &SendableRecordBatchStream {
        &self.inner
    }

    /// Unwraps this stream and returns the inner stream.
    pub fn into_inner(self) -> SendableRecordBatchStream {
        self.inner
    }
}

/// Validates the operation log schema:
/// - `_commit_version`
/// - `sequence_id`
/// - `operation`
/// - `graph`
/// - `subject`
/// - `predicate`
/// - `object`
#[allow(dead_code)]
fn validate_schema(schema: &Schema) {
    assert_eq!(
        schema.fields().len(),
        7,
        "Operations log stream should have 7 fields"
    );
}
