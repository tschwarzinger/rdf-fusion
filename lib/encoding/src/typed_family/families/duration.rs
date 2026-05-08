use crate::plain_term::{PlainTermArray, PlainTermType};
use crate::sortable_term::SortableTermArray;
use crate::typed_family::families::{
    FamilyArray, FamilyComparator, TypeClaim, TypedFamily,
};
use crate::typed_family::{TypedFamilyId, make_null_aware_comparator};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, Decimal128Array, Decimal128Builder, Int64Array,
    Int64Builder, StringArray, StructArray,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::compute::{and, is_null};
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::error::ArrowError;
use rdf_fusion_common::vocab::xsd;
use rdf_fusion_common::{
    AResult, Decimal, Duration, LiteralRef, NamedNodeRef, TypedValueRef,
};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::ops::Not;
use std::sync::{Arc, LazyLock};

/// Family for durations.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum DurationFamily {}

/// The fields of the duration family.
static FIELDS_DURATION: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("months", DataType::Int64, true),
        Field::new(
            "seconds",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            true,
        ),
    ])
});

static DATA_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(FIELDS_DURATION.clone()));

static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| {
    let mut types = BTreeSet::new();
    types.insert(xsd::DURATION.into());
    types.insert(xsd::DAY_TIME_DURATION.into());
    types.insert(xsd::YEAR_MONTH_DURATION.into());
    TypeClaim::Literal(types)
});

impl DurationFamily {
    pub fn fields() -> &'static Fields {
        &FIELDS_DURATION
    }
}

impl TypedFamily for DurationFamily {
    type Array = DurationFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::Duration;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &PlainTermArray,
    ) -> AResult<DurationFamilyArray> {
        validate_input(array)?;

        let parts = array.as_parts();
        let len = parts.struct_array.len();

        let mut months_builder = Int64Builder::with_capacity(len);
        let mut seconds_builder = Decimal128Builder::with_capacity(len)
            .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)?;

        for i in 0..len {
            let datatype = parts.data_type.value(i);
            let value = parts.value.value(i);

            let literal = LiteralRef::new_typed_literal(
                value,
                NamedNodeRef::new_unchecked(datatype),
            );
            let typed_value = TypedValueRef::try_from(literal).ok();

            match typed_value {
                Some(TypedValueRef::DurationLiteral(d)) => {
                    months_builder.append_value(d.months());
                    seconds_builder
                        .append_value(i128::from_be_bytes(d.seconds().to_be_bytes()));
                }
                Some(TypedValueRef::YearMonthDurationLiteral(d)) => {
                    months_builder.append_value(d.months());
                    seconds_builder.append_null();
                }
                Some(TypedValueRef::DayTimeDurationLiteral(d)) => {
                    months_builder.append_null();
                    seconds_builder
                        .append_value(i128::from_be_bytes(d.seconds().to_be_bytes()));
                }
                _ => {
                    months_builder.append_null();
                    seconds_builder.append_null();
                }
            }
        }

        let months = Arc::new(months_builder.finish()) as ArrayRef;
        let seconds = Arc::new(seconds_builder.finish()) as ArrayRef;
        let is_null = and(
            &is_null(months.as_ref()).expect("is_null should not fail"),
            &is_null(months.as_ref()).expect("is_null should not fail"),
        )
        .expect("same length");

        let res_array = Arc::new(StructArray::try_new(
            FIELDS_DURATION.clone(),
            vec![months, seconds],
            (is_null.true_count() > 0)
                .then(|| NullBuffer::from(is_null.into_parts().0.not())),
        )?) as ArrayRef;

        return Ok(DurationFamilyArray::from_array_unchecked(res_array));

        /// Validates whether the input contains terms that are not claimed by this family.
        fn validate_input(array: &PlainTermArray) -> Result<(), ArrowError> {
            let parts = array.as_parts();
            for i in 0..parts.struct_array.len() {
                if parts.struct_array.is_null(i) {
                    return Err(ArrowError::InvalidArgumentError(
                        "Null value in PlainTermArray".to_string(),
                    ));
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

impl Debug for DurationFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(Self::FAMILY_ID.as_str())
    }
}

/// A family-specific array for the [`DurationFamily`].
#[derive(Debug, Clone)]
pub struct DurationFamilyArray {
    array: ArrayRef,
}

impl DurationFamilyArray {
    /// Returns a reference to the inner [`StructArray`].
    pub fn struct_array(&self) -> &StructArray {
        self.array.as_struct()
    }

    /// Returns the months child array.
    pub fn months(&self) -> &Int64Array {
        self.struct_array().column(0).as_primitive()
    }

    /// Returns the seconds child array.
    pub fn seconds(&self) -> &Decimal128Array {
        self.struct_array().column(1).as_primitive()
    }
}

impl FamilyArray for DurationFamilyArray {
    type Family = DurationFamily;

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
        let lhs = self.clone();
        let lhs_nulls = self.null_buffer();

        let rhs = other.clone();
        let rhs_nulls = other.null_buffer();

        let inner: FamilyComparator = Box::new(move |lhs_idx, rhs_idx| {
            let lhs_val = Duration::new(
                if lhs.months().is_null(lhs_idx) {
                    0
                } else {
                    lhs.months().value(lhs_idx)
                },
                if lhs.seconds().is_null(lhs_idx) {
                    Decimal::default()
                } else {
                    Decimal::from_be_bytes(lhs.seconds().value(lhs_idx).to_be_bytes())
                },
            )
            .unwrap();
            let rhs_val = Duration::new(
                if rhs.months().is_null(rhs_idx) {
                    0
                } else {
                    rhs.months().value(rhs_idx)
                },
                if rhs.seconds().is_null(rhs_idx) {
                    Decimal::default()
                } else {
                    Decimal::from_be_bytes(rhs.seconds().value(rhs_idx).to_be_bytes())
                },
            )
            .unwrap();
            lhs_val.partial_cmp(&rhs_val)
        });

        if lhs_nulls.null_count() > 0 || rhs_nulls.null_count() > 0 {
            Some(make_null_aware_comparator(lhs_nulls, rhs_nulls, inner))
        } else {
            Some(inner)
        }
    }

    fn pretty_print(&self) -> Result<StringArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok((0..len)
            .map(|i| {
                if self.array.is_null(i) {
                    None
                } else {
                    let val = Duration::new(
                        self.months().value(i),
                        Decimal::from_be_bytes(self.seconds().value(i).to_be_bytes()),
                    )
                    .unwrap();
                    Some(val.to_string())
                }
            })
            .collect::<StringArray>())
    }

    fn effective_boolean_value(
        &self,
    ) -> Result<datafusion::arrow::array::BooleanArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok(datafusion::arrow::array::BooleanArray::new_null(len))
    }

    fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        let len = self.inner_ref().len();
        let months = self.months();
        let seconds = self.seconds();
        Ok((0..len)
            .map(|i| {
                if self.array.is_null(i) {
                    None
                } else {
                    let m = months.value(i);
                    let s = seconds.value(i);
                    if m == 0 {
                        Some(xsd::DAY_TIME_DURATION.as_str())
                    } else if s == 0 {
                        Some(xsd::YEAR_MONTH_DURATION.as_str())
                    } else {
                        Some(xsd::DURATION.as_str())
                    }
                }
            })
            .collect::<StringArray>())
    }

    fn cast_to_plain_term_array(&self) -> Result<PlainTermArray, ArrowError> {
        let values = self.pretty_print()?;
        let datatypes = self.literal_data_types()?;
        let len = self.inner_ref().len();
        PlainTermArray::try_new_literals(
            values,
            datatypes,
            StringArray::new_null(len),
            None,
        )
    }

    fn cast_to_sortable_array(&self) -> Result<SortableTermArray, ArrowError> {
        let mut builder =
            crate::sortable_term::SortableTermArrayBuilder::new(self.inner_ref().len());
        for i in 0..self.inner_ref().len() {
            if self.array.is_null(i) {
                builder.append_null();
            } else {
                let val = Duration::new(
                    self.months().value(i),
                    Decimal::from_be_bytes(self.seconds().value(i).to_be_bytes()),
                )
                .unwrap();
                builder.append_duration(val);
            }
        }
        Ok(builder.finish().try_into().unwrap())
    }
}
