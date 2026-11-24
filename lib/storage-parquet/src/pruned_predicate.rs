use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::datatypes::{DataType, FieldRef, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Result as DFResult;
use datafusion::logical_expr::ColumnarValue;
use datafusion::physical_expr::utils::{conjunction, split_conjunction};
use datafusion::physical_plan::PhysicalExpr;
use std::fmt::{Display, Formatter};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// A predicate that has already been used for pruning the files of a scan.
///
/// The Parquet source uses its predicate for two purposes: filtering the decoded rows and pruning
/// row groups and pages whenever a file is opened. If the files have already been pruned with a
/// predicate during planning, pruning them again with the same predicate cannot skip anything
/// else, while it is often more expensive than reading the few pages that remain.
///
/// DataFusion derives its pruning predicates only from expressions it knows and assumes that
/// any other expression may be true for every row group and page. This expression exploits that
/// by hiding the inner predicate from the pruning, while it still evaluates to the inner predicate
/// for filtering rows. Further filters that are pushed into the scan during execution (e.g., the
/// dynamic filter of a join) are not hidden and are therefore still used for pruning.
#[derive(Debug, Clone, Eq)]
pub(crate) struct PrunedPredicateExpr {
    inner: Arc<dyn PhysicalExpr>,
}

impl PrunedPredicateExpr {
    /// Wraps every conjunct of the `predicate` separately, such that the Parquet source can
    /// still evaluate (and order) the conjuncts individually while filtering rows.
    pub(crate) fn wrap_conjuncts(
        predicate: &Arc<dyn PhysicalExpr>,
    ) -> Arc<dyn PhysicalExpr> {
        conjunction(split_conjunction(predicate).into_iter().map(|conjunct| {
            Arc::new(Self {
                inner: Arc::clone(conjunct),
            }) as Arc<dyn PhysicalExpr>
        }))
    }
}

impl PartialEq for PrunedPredicateExpr {
    fn eq(&self, other: &Self) -> bool {
        self.inner.as_ref() == other.inner.as_ref()
    }
}

impl Hash for PrunedPredicateExpr {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.inner.hash(state);
    }
}

impl Display for PrunedPredicateExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        Display::fmt(&self.inner, f)
    }
}

impl PhysicalExpr for PrunedPredicateExpr {
    fn data_type(&self, input_schema: &Schema) -> DFResult<DataType> {
        self.inner.data_type(input_schema)
    }

    fn nullable(&self, input_schema: &Schema) -> DFResult<bool> {
        self.inner.nullable(input_schema)
    }

    fn evaluate(&self, batch: &RecordBatch) -> DFResult<ColumnarValue> {
        self.inner.evaluate(batch)
    }

    fn return_field(&self, input_schema: &Schema) -> DFResult<FieldRef> {
        self.inner.return_field(input_schema)
    }

    fn evaluate_selection(
        &self,
        batch: &RecordBatch,
        selection: &BooleanArray,
    ) -> DFResult<ColumnarValue> {
        self.inner.evaluate_selection(batch, selection)
    }

    fn children(&self) -> Vec<&Arc<dyn PhysicalExpr>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn PhysicalExpr>>,
    ) -> DFResult<Arc<dyn PhysicalExpr>> {
        match (children.pop(), children.is_empty()) {
            (Some(inner), true) => Ok(Arc::new(Self { inner })),
            _ => datafusion::common::internal_err!(
                "PrunedPredicateExpr requires exactly one child"
            ),
        }
    }

    fn fmt_sql(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt_sql(f)
    }
}
