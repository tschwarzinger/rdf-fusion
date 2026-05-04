use datafusion::arrow::datatypes::Schema;
use datafusion::execution::SendableRecordBatchStream;

/// A wrapper around a [`SendableRecordBatchStream`] that represents entries in the operations log
/// table.
pub struct OperationsChangesetStream {
    inner: SendableRecordBatchStream,
}

impl OperationsChangesetStream {
    pub const OP_IDX: usize = 0;
    pub const GRAPH_IDX: usize = 1;
    pub const SUBJECT_IDX: usize = 2;
    pub const PREDICATE_IDX: usize = 3;
    pub const OBJECT_IDX: usize = 4;

    /// Creates a new [`OperationsChangesetStream`] or returns an error.
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
/// - `operation`
/// - `graph`
/// - `subject`
/// - `predicate`
/// - `object`
fn validate_schema(schema: &Schema) {
    assert_eq!(
        schema.fields().len(),
        5,
        "Operations changeset stream should have 5 fields"
    );
}
