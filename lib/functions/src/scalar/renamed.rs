use datafusion::arrow::datatypes::{DataType, FieldRef};
use datafusion::common::ExprSchema;
use datafusion::logical_expr::interval_arithmetic::Interval;
use datafusion::logical_expr::simplify::{ExprSimplifyResult, SimplifyContext};
use datafusion::logical_expr::sort_properties::{ExprProperties, SortProperties};
use datafusion::logical_expr::{
    ColumnarValue, Documentation, Expr, ReturnFieldArgs, ScalarFunctionArgs,
    ScalarUDFImpl, Signature,
};
use rdf_fusion_common::DFResult;
use std::any::Any;
use std::hash::{Hash, Hasher};

/// Renames an existing [`ScalarUDFImpl`] with a new name. This can be useful if you want to
/// register an existing UDF (e.g., SQL's `coalesce`) but with a different name (SPARQL's
/// `COALESCE`).
///
/// # Equality
///
/// Two [`RenamedScalarUdfImpl`] are only considered to be equal if both have the same name.
#[derive(Debug, Clone)]
pub struct RenamedScalarUdfImpl<TUDFImpl: ScalarUDFImpl> {
    /// The new name of the UDF
    name: String,
    /// The inner UDF
    inner: TUDFImpl,
    /// An optional override for the signature.
    signature_override: Option<Signature>,
}

impl<TUDFImpl: ScalarUDFImpl + 'static> RenamedScalarUdfImpl<TUDFImpl> {
    /// Creates a new [`RenamedScalarUdfImpl`].
    pub fn new(name: String, inner: TUDFImpl) -> Self {
        Self {
            name,
            inner,
            signature_override: None,
        }
    }

    /// Sets an override for the signature of the function.
    pub fn with_signature(
        mut self,
        signature: Signature,
    ) -> RenamedScalarUdfImpl<TUDFImpl> {
        self.signature_override = Some(signature);
        self
    }
}

impl<TUDFImpl: ScalarUDFImpl + 'static> ScalarUDFImpl for RenamedScalarUdfImpl<TUDFImpl> {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        self.signature_override
            .as_ref()
            .unwrap_or_else(|| self.inner.signature())
    }

    fn return_type(&self, arg_types: &[DataType]) -> DFResult<DataType> {
        self.inner.return_type(arg_types)
    }

    fn return_field_from_args(&self, args: ReturnFieldArgs) -> DFResult<FieldRef> {
        self.inner.return_field_from_args(args)
    }

    fn is_nullable(&self, args: &[Expr], schema: &dyn ExprSchema) -> bool {
        #[allow(deprecated)]
        self.inner.is_nullable(args, schema)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        self.inner.invoke_with_args(args)
    }

    fn simplify(
        &self,
        args: Vec<Expr>,
        info: &SimplifyContext,
    ) -> DFResult<ExprSimplifyResult> {
        self.inner.simplify(args, info)
    }

    fn short_circuits(&self) -> bool {
        self.inner.short_circuits()
    }

    fn evaluate_bounds(&self, input: &[&Interval]) -> DFResult<Interval> {
        self.inner.evaluate_bounds(input)
    }

    fn propagate_constraints(
        &self,
        interval: &Interval,
        inputs: &[&Interval],
    ) -> DFResult<Option<Vec<Interval>>> {
        self.inner.propagate_constraints(interval, inputs)
    }

    fn output_ordering(&self, inputs: &[ExprProperties]) -> DFResult<SortProperties> {
        self.inner.output_ordering(inputs)
    }

    fn preserves_lex_ordering(&self, inputs: &[ExprProperties]) -> DFResult<bool> {
        self.inner.preserves_lex_ordering(inputs)
    }

    fn coerce_types(&self, arg_types: &[DataType]) -> DFResult<Vec<DataType>> {
        self.inner.coerce_types(arg_types)
    }

    fn documentation(&self) -> Option<&Documentation> {
        self.inner.documentation()
    }
}

impl<TUDFImpl: ScalarUDFImpl + 'static> Hash for RenamedScalarUdfImpl<TUDFImpl> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.name.hash(state);
        self.inner.dyn_hash(state);
    }
}

impl<TUDFImpl: ScalarUDFImpl + 'static> PartialEq for RenamedScalarUdfImpl<TUDFImpl> {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name && self.inner.dyn_eq(&other.inner)
    }
}

impl<TUDFImpl: ScalarUDFImpl + 'static> Eq for RenamedScalarUdfImpl<TUDFImpl> {}
