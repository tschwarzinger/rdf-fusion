mod expression_simplifier;
mod scalars;

use datafusion::logical_expr::Expr;
pub use expression_simplifier::*;
use rdf_fusion_extensions::functions::BuiltinName;

/// Returns an inner expression by unwrapping all encoding changes.
pub fn unwrap_encoding_changes(expr: &Expr) -> &Expr {
    match expr {
        Expr::ScalarFunction(sf) => {
            let function_name = sf.func.name();
            let Some(function_name) = BuiltinName::try_from(function_name).ok() else {
                return expr;
            };

            match function_name {
                BuiltinName::WithPlainTermEncoding
                | BuiltinName::WithTypedFamilyEncoding
                | BuiltinName::WithSortableEncoding => {
                    unwrap_encoding_changes(&sf.args[0])
                }
                _ => expr,
            }
        }
        _ => expr,
    }
}
