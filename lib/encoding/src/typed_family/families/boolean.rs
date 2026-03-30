use crate::plain_term::{PlainTermArray, PlainTermType};
use crate::sortable_term::{SortableTermArray, SortableTermArrayBuilder};
use crate::typed_family::families::{
    FamilyArray, FamilyComparator, TypeClaim, TypedFamily,
};
use crate::typed_family::{TypedFamilyId, make_null_aware_comparator};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, BooleanBuilder, StringArray,
};
use datafusion::arrow::datatypes::DataType;
use datafusion::arrow::error::ArrowError;
use rdf_fusion_model::AResult;
use rdf_fusion_model::vocab::xsd;
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

/// A family that only stores Boolean values.
///
/// # Layout
///
/// ```text
///  Boolean Array
/// ┌───────┐
/// │ true  │
/// │───────│
/// │ false │
/// │───────│
/// │ true  │
/// └───────┘
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum BooleanFamily {}

static DATA_TYPE: LazyLock<DataType> = LazyLock::new(|| DataType::Boolean);

static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| {
    let mut types = BTreeSet::new();
    types.insert(xsd::BOOLEAN.into());
    TypeClaim::Literal(types)
});

impl TypedFamily for BooleanFamily {
    type Array = BooleanFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::Boolean;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &PlainTermArray,
    ) -> AResult<BooleanFamilyArray> {
        validate_input(array)?;

        let parts = array.as_parts();
        let values = parts.value;
        let mut builder = BooleanBuilder::with_capacity(values.len());

        for i in 0..values.len() {
            if values.is_null(i) {
                builder.append_null();
                continue;
            }

            let val = values.value(i);
            match val {
                "true" | "1" => builder.append_value(true),
                "false" | "0" => builder.append_value(false),
                _ => builder.append_null(),
            }
        }

        return Ok(BooleanFamilyArray::from_array_unchecked(Arc::new(
            builder.finish(),
        )));

        /// Detects whether the input contains unexpected terms.
        fn validate_input(array: &PlainTermArray) -> Result<(), ArrowError> {
            let parts = array.as_parts();

            for i in 0..parts.struct_array.len() {
                if parts.struct_array.is_null(i) {
                    continue;
                }

                let term_type =
                    PlainTermType::try_from(parts.term_type.value(i)).unwrap();
                if term_type != PlainTermType::Literal {
                    return Err(ArrowError::InvalidArgumentError(
                        "Not a literal".to_string(),
                    ));
                }

                let datatype = parts.data_type.value(i);
                if !CLAIM.is_responsible_for_datatype(datatype) {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Wrong datatype: {datatype}"
                    )));
                }
            }
            Ok(())
        }
    }
}

impl Debug for BooleanFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(Self::FAMILY_ID.as_str())
    }
}

/// A family-specific array for the [`BooleanFamily`].
#[derive(Debug, Clone)]
pub struct BooleanFamilyArray {
    array: ArrayRef,
}

impl BooleanFamilyArray {
    /// Creates a new [`BooleanFamilyArray`].
    pub fn new(array: BooleanArray) -> Self {
        Self {
            array: Arc::new(array),
        }
    }

    /// Returns a reference to the inner [`BooleanArray`].
    pub fn inner_ref(array: &ArrayRef) -> &BooleanArray {
        array.as_boolean()
    }

    /// Returns a reference to the inner [`BooleanArray`].
    pub fn inner(&self) -> &BooleanArray {
        Self::inner_ref(&self.array)
    }
}

impl FamilyArray for BooleanFamilyArray {
    type Family = BooleanFamily;

    fn from_array_unchecked(array: ArrayRef) -> Self {
        Self { array }
    }

    fn inner_ref(&self) -> &ArrayRef {
        &self.array
    }

    fn into_array_ref(self) -> ArrayRef {
        self.array
    }

    fn comparator(&self, other: &Self) -> Option<FamilyComparator> {
        let lhs = self.inner().clone();
        let lhs_nulls = self.null_buffer();

        let rhs = other.inner().clone();
        let rhs_nulls = other.null_buffer();

        let inner: FamilyComparator = Box::new(move |lhs_idx, rhs_idx| {
            let lhs_val = lhs.value(lhs_idx);
            let rhs_val = rhs.value(rhs_idx);
            Some(lhs_val.cmp(&rhs_val))
        });

        if lhs_nulls.null_count() > 0 || rhs_nulls.null_count() > 0 {
            Some(make_null_aware_comparator(lhs_nulls, rhs_nulls, inner))
        } else {
            Some(inner)
        }
    }

    fn pretty_print(&self) -> Result<StringArray, ArrowError> {
        let bool_array = self.inner();
        Ok(bool_array
            .iter()
            .map(|v| v.map(|b| if b { "true" } else { "false" }))
            .collect::<StringArray>())
    }

    fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError> {
        Ok(self.inner().clone())
    }

    fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        Ok(StringArray::new_repeated(
            xsd::BOOLEAN.as_str(),
            self.array.len(),
        ))
    }

    fn cast_to_plain_term_array(&self) -> AResult<PlainTermArray> {
        let values = self.pretty_print()?;
        let datatypes = self.literal_data_types()?;
        PlainTermArray::try_new_literals(
            values,
            datatypes,
            StringArray::new_null(self.inner_ref().len()),
            None,
        )
    }

    fn cast_to_sortable_array(&self) -> Result<SortableTermArray, ArrowError> {
        let mut builder = SortableTermArrayBuilder::new(self.inner_ref().len());
        for i in 0..self.inner_ref().len() {
            if self.array.is_null(i) {
                builder.append_null();
            } else {
                builder.append_boolean(self.inner().value(i).into());
            }
        }
        Ok(builder.finish().try_into().unwrap())
    }
}

impl From<BooleanArray> for BooleanFamilyArray {
    fn from(value: BooleanArray) -> Self {
        Self::new(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::{pretty_format_batches, pretty_format_columns};
    use insta::assert_snapshot;
    use std::iter::repeat_n;

    #[test]
    fn test_boolean_family_ebv() {
        let array = Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef;
        let family_array = BooleanFamilyArray::from_array_unchecked(array);

        let ebv = family_array.effective_boolean_value().unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("ebv", Arc::new(ebv) as ArrayRef)]).unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        assert_snapshot!(formatted, @r"
        +-------+
        | ebv   |
        +-------+
        | true  |
        | false |
        | true  |
        +-------+");
    }

    #[test]
    fn test_boolean_family_pretty_print() {
        let array = Arc::new(BooleanArray::from(vec![true, false, true])) as ArrayRef;
        let family_array = BooleanFamilyArray::from_array_unchecked(array);

        let pretty = family_array.pretty_print().unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("pretty", Arc::new(pretty) as ArrayRef)])
                .unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        assert_snapshot!(formatted, @r"
        +--------+
        | pretty |
        +--------+
        | true   |
        | false  |
        | true   |
        +--------+");
    }

    #[test]
    fn test_boolean_family_from_plain_term_invalid() {
        let values =
            StringArray::from(vec!["1", "0", "true", "false", "yes", "no", "foo"]);

        let len = values.len();
        let pt_array = PlainTermArray::try_new_literals(
            values,
            StringArray::from_iter_values(repeat_n(xsd::BOOLEAN.as_str(), len)),
            StringArray::new_null(len),
            None,
        )
        .unwrap();

        let family_array =
            BooleanFamily::create_array_from_plain_term(&pt_array).unwrap();
        let result = pretty_format_columns("result", &[family_array.array]).unwrap();
        assert_snapshot!(
            result,
            @"
        +--------+
        | result |
        +--------+
        | true   |
        | false  |
        | true   |
        | false  |
        |        |
        |        |
        |        |
        +--------+
        "
        )
    }
}
