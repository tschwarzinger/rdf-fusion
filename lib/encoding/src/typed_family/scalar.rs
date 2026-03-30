use crate::encoding::EncodingScalar;
use crate::typed_family::{TypedFamilyEncoding, TypedFamilyEncodingRef};
use crate::{EncodingArray, TermEncoding};
use datafusion::common::{ScalarValue, exec_err};
use rdf_fusion_model::DFResult;
use std::sync::Arc;

/// Represents an Arrow scalar with a [`TypedFamilyEncoding`].
#[derive(Clone)]
pub struct TypedFamilyScalar {
    /// The [`TypedFamilyEncoding`] of this scalar.
    encoding: TypedFamilyEncodingRef,
    /// The actual [`ScalarValue`].
    inner: ScalarValue,
}

impl TypedFamilyScalar {
    /// Tries to create a new [`TypedFamilyScalar`] from a regular [`ScalarValue`].
    ///
    /// # Errors
    ///
    /// Returns an error if the data type of `value` is unexpected.
    pub fn try_new(
        encoding: TypedFamilyEncodingRef,
        value: ScalarValue,
    ) -> DFResult<Self> {
        if &value.data_type() != encoding.data_type() {
            return exec_err!(
                "Expected scalar value with TypedFamilyEncoding, got {:?}",
                value
            );
        }
        Ok(Self::new_unchecked(encoding, value))
    }

    /// Creates a new [`TypedFamilyScalar`] without checking invariants.
    pub fn new_unchecked(encoding: TypedFamilyEncodingRef, inner: ScalarValue) -> Self {
        Self { encoding, inner }
    }

    /// Returns the type id of this scalar.
    pub fn type_id(&self) -> i8 {
        match self.inner {
            ScalarValue::Union(Some((type_id, _)), _, _) => type_id,
            _ => panic!("Illegal ScalarValue in TypedFamilyScalar."),
        }
    }

    /// Returns the plain term representation of this scalar.
    pub fn as_plain_term_scalar(&self) -> DFResult<ScalarValue> {
        let array = self.to_array(1)?;
        let plain_term_array = array.as_plain_term_array()?.into_array_ref();
        ScalarValue::try_from_array(&plain_term_array, 0)
    }
}

impl EncodingScalar for TypedFamilyScalar {
    type Encoding = TypedFamilyEncoding;

    fn encoding(&self) -> &Arc<Self::Encoding> {
        &self.encoding
    }

    fn scalar_value(&self) -> &ScalarValue {
        &self.inner
    }

    fn into_scalar_value(self) -> ScalarValue {
        self.inner
    }
}
