use crate::delta::error::DeltaQuadStorageError;
use crate::delta::log::{COL_OPERATION, DeltaStorageLogOperation};
use deltalake::kernel::Add;

/// Marks a Parquet file in the operations log that is associated with a table version.
pub struct OperationLogFile {
    version: u64,
    file: Add,
}

impl OperationLogFile {
    /// Creates a new [`OperationLogFile`]
    pub fn new(version: u64, file: Add) -> Self {
        Self { version, file }
    }

    /// Returns the version that this file belongs to.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// Returns a reference to the underlying Delta Lake file.
    pub fn inner(&self) -> &Add {
        &self.file
    }

    /// Returns whether this file only contains [`DeltaStorageLogOperation::InsertQuad`] operations.
    ///
    /// Returns [`None`], if this cannot be determined (e.g., missing statistics) or an error if the
    /// statistics are invalid.
    pub fn only_contains_quad_insertions(
        &self,
    ) -> Result<Option<bool>, DeltaQuadStorageError> {
        let parsed_stats = self.file.get_stats().map_err(|e| {
            DeltaQuadStorageError::Other(format!("Failed to parse stats: {e}"))
        })?;

        if let Some(stats) = parsed_stats {
            if stats.num_records == 0 {
                return Ok(None);
            }

            let min_op = stats.min_values.get(COL_OPERATION);
            let max_op = stats.max_values.get(COL_OPERATION);

            match (min_op, max_op) {
                (Some(min), Some(max)) => {
                    // TODO: error handling
                    let min = min.as_value().unwrap().as_u64().unwrap();
                    let max = max.as_value().unwrap().as_u64().unwrap();

                    let add_operation =
                        DeltaStorageLogOperation::InsertQuad.as_stored() as u64;
                    let is_only_sparql_adds =
                        min == add_operation && max == add_operation;

                    Ok(Some(is_only_sparql_adds))
                }
                _ => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}
