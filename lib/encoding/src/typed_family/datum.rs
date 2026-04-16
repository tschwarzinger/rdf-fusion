use crate::typed_family::{TypedFamilyChild, TypedFamilyEncoding};
use crate::{EncodingDatum, EncodingScalar};
use datafusion::logical_expr::ColumnarValue;

impl EncodingDatum<TypedFamilyEncoding> {
    // TODO
    pub fn try_get_homogeneous_child(
        &self,
        number_rows: usize,
    ) -> Option<TypedFamilyChild> {
        match self {
            EncodingDatum::Array(array) => array.try_get_homogeneous_child(),
            // A scalar is always homogeneous
            EncodingDatum::Scalar(scalar) => {
                let type_id = scalar.type_id();
                let value = scalar.inner_value();
                let encoding = scalar.encoding();
                Some(TypedFamilyChild {
                    family: encoding.type_families()[type_id as usize].clone(),
                    number_rows,
                    value: ColumnarValue::Scalar(value.clone()),
                })
            }
        }
    }
}
