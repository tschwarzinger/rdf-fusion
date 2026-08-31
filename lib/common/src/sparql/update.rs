use crate::sparql::{GraphTarget, QuadPattern};
use crate::{GraphName, NamedNode};
use datafusion::arrow::array::RecordBatch;
use datafusion::common::test_util::format_batches;
use datafusion::logical_expr::LogicalPlan;
use spargebra::term::GroundQuadPattern;
use std::fmt::{Display, Formatter};

/// Represents a parsed SPARQL Update script (a sequence of operations).
#[derive(Clone, Debug)]
pub struct RdfFusionUpdate {
    operations: Vec<UpdateOperation>,
}

impl RdfFusionUpdate {
    /// Creates a new [`RdfFusionUpdate`].
    pub fn new(operations: Vec<UpdateOperation>) -> Self {
        Self { operations }
    }

    /// Returns a reference to the parsed operations.
    pub fn operations(&self) -> &[UpdateOperation] {
        &self.operations
    }

    /// Returns a [`Display`] implementation that formats all contained operations in a list.
    pub fn display_list_operations<'a>(&'a self) -> impl Display + 'a {
        RdfFusionUpdateDisplayListOperations(self)
    }
}

struct RdfFusionUpdateDisplayListOperations<'a>(&'a RdfFusionUpdate);

impl<'a> Display for RdfFusionUpdateDisplayListOperations<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        for operation in self.0.operations() {
            writeln!(f, "{}", operation.display_indent())?;
        }
        Ok(())
    }
}

/// Defines the operation variant of an [`UpdateOperation`].
#[derive(Clone, Debug)]
pub enum UpdateOperation {
    InsertData {
        quads: RecordBatch,
    },
    DeleteData {
        quads: RecordBatch,
    },
    DeleteInsert {
        delete: Vec<GroundQuadPattern>,
        insert: Vec<QuadPattern>,
        pattern: LogicalPlan,
    },
    Load {
        silent: bool,
        source: NamedNode,
        destination: GraphName,
    },
    Clear {
        silent: bool,
        graph: GraphTarget,
    },
    Drop {
        silent: bool,
        graph: GraphTarget,
    },
    Create {
        silent: bool,
        graph: NamedNode,
    },
}

impl UpdateOperation {
    /// Returns a [`Display`] implementation that prints the variant name.
    pub fn display_indent<'a>(&'a self) -> impl Display + 'a {
        UpdateOperationDisplayIndent(self)
    }
}

/// Only prints the variant name.
struct UpdateOperationDisplayIndent<'a>(&'a UpdateOperation);

impl<'a> Display for UpdateOperationDisplayIndent<'a> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            UpdateOperation::InsertData { .. } => f.write_str("INSERT DATA"),
            UpdateOperation::DeleteData { .. } => f.write_str("DELETE DATA"),
            UpdateOperation::Load {
                source,
                destination,
                silent,
            } => write!(
                f,
                "LOAD(source={source}, destination={destination}, silent={silent})"
            ),
            UpdateOperation::Clear { graph, silent } => {
                write!(f, "CLEAR(graph={graph}, silent={silent})")
            }
            UpdateOperation::Drop { graph, silent } => {
                write!(f, "DROP(graph={graph}, silent={silent})")
            }
            UpdateOperation::Create { graph, silent } => {
                write!(f, "CREATE(graph={graph}, silent={silent})")
            }
            UpdateOperation::DeleteInsert { delete, insert, .. } => {
                f.write_str("DELETE INSERT\n")?;

                if !delete.is_empty() {
                    f.write_str("    DELETE:\n")?;
                    for pattern in delete {
                        writeln!(f, "        {pattern}")?;
                    }
                }

                if !insert.is_empty() {
                    f.write_str("    INSERT:\n")?;
                    for pattern in insert {
                        writeln!(f, "        {pattern}")?;
                    }
                }

                Ok(())
            }
        }?;
        f.write_str("\n")?;

        match self.0 {
            UpdateOperation::InsertData { quads }
            | UpdateOperation::DeleteData { quads } => {
                writeln!(
                    f,
                    "{}",
                    format_batches(std::slice::from_ref(quads))
                        .map_err(|_| std::fmt::Error)?
                )?;
            }
            UpdateOperation::DeleteInsert { pattern, .. } => {
                writeln!(f, "{}", pattern.display_indent())?;
            }
            _ => {}
        };

        Ok(())
    }
}
