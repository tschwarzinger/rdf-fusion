use crate::plain_term::{PlainTermArray, PlainTermType};
use crate::typed_family::families::{
    FamilyArray, FamilyComparator, TypeClaim, TypedFamily,
};
use crate::typed_family::{DurationFamily, TypedFamilyId, make_null_aware_comparator};
use datafusion::arrow::array::{
    Array, ArrayRef, AsArray, BinaryArray, BooleanArray, BooleanBufferBuilder,
    Decimal128Array, Decimal128Builder, GenericBinaryBuilder, Int16Array, Int16Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder, StructArray, UInt8Array,
    UInt8Builder,
};
use datafusion::arrow::buffer::NullBuffer;
use datafusion::arrow::datatypes::{DataType, Field, Fields};
use datafusion::arrow::error::ArrowError;
use rdf_fusion_common::{
    AResult, Date, DateTime, DayTimeDuration, Decimal, LiteralRef, NamedNodeRef, Time,
    Timestamp, TimezoneOffset, TypedValueRef, vocab::xsd,
};
use std::collections::BTreeSet;
use std::fmt::{Debug, Formatter};
use std::sync::{Arc, LazyLock};

/// Family of `xsd:dateTime`, `xsd:date` and `xsd:time`.
///
/// # Layout
///
/// The layout of the dates and time family is a struct array with three fields:
/// - A type id which indicates which of the three types is stored (UInt8)
/// - The value of the type (Decimal128)
/// - An offset for the timezone (Int16)
///
/// ```text
/// ┌──────────────────────────────────────────┐
/// │ Struct Array                             │
/// │                                          │
/// │  DT Type      Value          Offset      │
/// │  ┌───────┐   ┌──────────┐   ┌──────────┐ │
/// │  │ 0     │   │ 10.0     │   │ NULL     │ │
/// │  │───────│   │──────────│   │──────────│ │
/// │  │ 1     │   │ 20.0     │   │ -10      │ │
/// │  │───────│   │──────────│   │──────────│ │
/// │  │ 2     │   │ 30.0     │   │ +20      │ │
/// │  └───────┘   └──────────┘   └──────────┘ │
/// └──────────────────────────────────────────┘
/// ```
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub enum DateTimeFamily {}

/// The layout of the timestamp family.
static FIELDS_TIMESTAMP: LazyLock<Fields> = LazyLock::new(|| {
    Fields::from(vec![
        Field::new("date_time_type", DataType::UInt8, false),
        Field::new(
            "value",
            DataType::Decimal128(Decimal::PRECISION, Decimal::SCALE),
            false,
        ),
        Field::new("offset", DataType::Int16, true),
    ])
});

static DATA_TYPE: LazyLock<DataType> =
    LazyLock::new(|| DataType::Struct(FIELDS_TIMESTAMP.clone()));

static CLAIM: LazyLock<TypeClaim> = LazyLock::new(|| {
    let mut types = BTreeSet::new();
    types.insert(xsd::DATE_TIME.into());
    types.insert(xsd::DATE.into());
    types.insert(xsd::TIME.into());
    TypeClaim::Literal(types)
});

impl DateTimeFamily {
    /// The type id for date times.
    pub const DATE_TIME_TYPE_ID: u8 = 0;

    /// The type id for dates.
    pub const DATE_TYPE_ID: u8 = 1;

    /// The type id for times.
    pub const TIME_TYPE_ID: u8 = 2;

    pub fn fields() -> &'static Fields {
        &FIELDS_TIMESTAMP
    }
}

impl TypedFamily for DateTimeFamily {
    type Array = DateTimeFamilyArray;

    const FAMILY_ID: TypedFamilyId = TypedFamilyId::DateTime;

    fn data_type() -> &'static DataType {
        &DATA_TYPE
    }

    fn claim() -> &'static TypeClaim {
        &CLAIM
    }

    fn create_array_from_plain_term(
        array: &PlainTermArray,
    ) -> AResult<DateTimeFamilyArray> {
        validate_input(array)?;

        let parts = array.as_parts();
        let len = parts.struct_array.len();

        let mut dt_types = UInt8Builder::with_capacity(len);
        let mut dt_ts_values = Decimal128Builder::with_capacity(len)
            .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
            .expect("Valid precision and scale");
        let mut dt_ts_offsets = Int16Builder::with_capacity(len);

        for i in 0..len {
            let datatype = parts.data_type.value(i);
            let value = parts.value.value(i);

            let literal = LiteralRef::new_typed_literal(
                value,
                NamedNodeRef::new_unchecked(datatype),
            );
            let typed_value = TypedValueRef::try_from(literal).ok();

            match typed_value {
                Some(TypedValueRef::DateTimeLiteral(dt)) => {
                    dt_types.append_value(DateTimeFamily::DATE_TIME_TYPE_ID);
                    dt_ts_values.append_value(i128::from_be_bytes(
                        dt.timestamp().value().to_be_bytes(),
                    ));
                    dt_ts_offsets
                        .append_option(dt.timestamp().offset().map(|o| o.in_minutes()));
                }
                Some(TypedValueRef::DateLiteral(d)) => {
                    dt_types.append_value(DateTimeFamily::DATE_TYPE_ID);
                    dt_ts_values.append_value(i128::from_be_bytes(
                        d.timestamp().value().to_be_bytes(),
                    ));
                    dt_ts_offsets
                        .append_option(d.timestamp().offset().map(|o| o.in_minutes()));
                }
                Some(TypedValueRef::TimeLiteral(t)) => {
                    dt_types.append_value(DateTimeFamily::TIME_TYPE_ID);
                    dt_ts_values.append_value(i128::from_be_bytes(
                        t.timestamp().value().to_be_bytes(),
                    ));
                    dt_ts_offsets
                        .append_option(t.timestamp().offset().map(|o| o.in_minutes()));
                }
                _ => {
                    dt_types.append_null();
                    dt_ts_values.append_null();
                    dt_ts_offsets.append_null();
                }
            }
        }

        let res_array = DateTimeArrayBuilder::new(
            dt_types.finish(),
            dt_ts_values.finish(),
            dt_ts_offsets.finish(),
        )
        .finish()?;

        return Ok(DateTimeFamilyArray::from_array_unchecked(res_array));

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

impl Debug for DateTimeFamily {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_str(Self::FAMILY_ID.as_str())
    }
}

/// A family-specific array for the [`DateTimeFamily`].
#[derive(Debug, Clone)]
pub struct DateTimeFamilyArray {
    array: ArrayRef,
}

impl DateTimeFamilyArray {
    /// Returns a reference to the inner [`StructArray`].
    pub fn inner_ref(array: &ArrayRef) -> &StructArray {
        array.as_struct()
    }

    /// Returns a reference to the inner [`StructArray`].
    pub fn struct_array(&self) -> &StructArray {
        Self::inner_ref(&self.array)
    }

    /// Returns a reference to the inner [`StructArray`].
    pub fn inner(&self) -> &StructArray {
        Self::inner_ref(&self.array)
    }

    /// Returns the array of date time types.
    pub fn date_time_type(&self) -> &UInt8Array {
        self.struct_array().column(0).as_primitive()
    }

    /// Returns the array of timestamp values.
    pub fn timestamp_values(&self) -> &Decimal128Array {
        self.struct_array().column(1).as_primitive()
    }

    /// Returns the array of timestamp offsets.
    pub fn timestamp_offsets(&self) -> &Int16Array {
        self.struct_array().column(2).as_primitive()
    }

    /// Returns the year for each value in the array.
    pub fn year(&self) -> Int64Array {
        let mut builder = Int64Builder::with_capacity(self.struct_array().len());
        for i in 0..self.struct_array().len() {
            let ts = self.get_timestamp(i);
            builder.append_value(DateTime::new(ts).year());
        }
        builder.finish()
    }

    /// Returns the month for each value in the array.
    pub fn month(&self) -> Int64Array {
        let mut builder = Int64Builder::with_capacity(self.struct_array().len());
        for i in 0..self.struct_array().len() {
            let ts = self.get_timestamp(i);
            builder.append_value(DateTime::new(ts).month().into());
        }
        builder.finish()
    }

    /// Returns the day for each value in the array.
    pub fn day(&self) -> Int64Array {
        let mut builder = Int64Builder::with_capacity(self.struct_array().len());
        for i in 0..self.struct_array().len() {
            let ts = self.get_timestamp(i);
            builder.append_value(DateTime::new(ts).day().into());
        }
        builder.finish()
    }

    /// Returns the hour for each value in the array.
    pub fn hour(&self) -> Int64Array {
        let mut builder = Int64Builder::with_capacity(self.struct_array().len());
        for i in 0..self.struct_array().len() {
            let ts = self.get_timestamp(i);
            builder.append_value(DateTime::new(ts).hour().into());
        }
        builder.finish()
    }

    /// Returns the minute for each value in the array.
    pub fn minute(&self) -> Int64Array {
        let mut builder = Int64Builder::with_capacity(self.struct_array().len());
        for i in 0..self.struct_array().len() {
            let ts = self.get_timestamp(i);
            builder.append_value(DateTime::new(ts).minute().into());
        }
        builder.finish()
    }

    /// Returns the second for each value in the array.
    pub fn second(&self) -> Decimal128Array {
        let mut builder = Decimal128Builder::with_capacity(self.struct_array().len())
            .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
            .unwrap();
        for i in 0..self.struct_array().len() {
            let ts = self.get_timestamp(i);
            builder.append_value(i128::from_be_bytes(
                DateTime::new(ts).second().to_be_bytes(),
            ));
        }
        builder.finish()
    }

    /// Returns the timezone as `xsd:dayTimeDuration` for each value in the array.
    ///
    /// The result will contain null values for values where no timezone exists.
    pub fn timezone(&self) -> AResult<ArrayRef> {
        let mut months_builder = Int64Builder::with_capacity(self.struct_array().len());
        let mut seconds_builder =
            Decimal128Builder::with_capacity(self.struct_array().len())
                .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
                .unwrap();
        let mut valid_builder = BooleanBufferBuilder::new(self.struct_array().len());

        for i in 0..self.struct_array().len() {
            let ts = self.get_timestamp(i);
            if let Some(offset) = ts.offset() {
                let duration = DayTimeDuration::from(offset);
                months_builder.append_null();
                seconds_builder.append_value(i128::from_be_bytes(
                    duration.as_seconds().to_be_bytes(),
                ));
                valid_builder.append(true);
            } else {
                months_builder.append_null();
                seconds_builder.append_null();
                valid_builder.append(false);
            }
        }

        let nulls = NullBuffer::from(valid_builder.finish());
        let child = StructArray::try_new(
            DurationFamily::fields().clone(),
            vec![
                Arc::new(months_builder.finish()) as ArrayRef,
                Arc::new(seconds_builder.finish()) as ArrayRef,
            ],
            Some(nulls),
        )?;

        Ok(Arc::new(child) as ArrayRef)
    }

    /// Returns the timezone as string for each value in the array.
    pub fn tz(&self) -> StringArray {
        let mut builder = StringBuilder::with_capacity(self.struct_array().len(), 0);
        for i in 0..self.struct_array().len() {
            let ts = self.get_timestamp(i);
            if let Some(offset) = ts.offset() {
                builder.append_value(offset.to_string());
            } else {
                builder.append_value("");
            }
        }
        builder.finish()
    }

    fn get_timestamp(&self, i: usize) -> Timestamp {
        Timestamp::new(
            Decimal::from_be_bytes(self.timestamp_values().value(i).to_be_bytes()),
            if self.timestamp_offsets().is_null(i) {
                None
            } else {
                Some(TimezoneOffset::new_unchecked(
                    self.timestamp_offsets().value(i),
                ))
            },
        )
    }
}

impl FamilyArray for DateTimeFamilyArray {
    type Family = DateTimeFamily;

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
            let lhs_type = lhs.date_time_type().value(lhs_idx);
            let rhs_type = rhs.date_time_type().value(rhs_idx);
            if lhs_type != rhs_type {
                return None;
            }

            let lhs_ts = Timestamp::new(
                Decimal::from_be_bytes(
                    lhs.timestamp_values().value(lhs_idx).to_be_bytes(),
                ),
                if lhs.timestamp_offsets().is_null(lhs_idx) {
                    None
                } else {
                    Some(TimezoneOffset::new_unchecked(
                        lhs.timestamp_offsets().value(lhs_idx),
                    ))
                },
            );
            let rhs_ts = Timestamp::new(
                Decimal::from_be_bytes(
                    rhs.timestamp_values().value(rhs_idx).to_be_bytes(),
                ),
                if rhs.timestamp_offsets().is_null(rhs_idx) {
                    None
                } else {
                    Some(TimezoneOffset::new_unchecked(
                        rhs.timestamp_offsets().value(rhs_idx),
                    ))
                },
            );

            lhs_ts.partial_cmp(&rhs_ts)
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
                if self.struct_array().is_valid(i) {
                    let ts = Timestamp::new(
                        Decimal::from_be_bytes(
                            self.timestamp_values().value(i).to_be_bytes(),
                        ),
                        if self.timestamp_offsets().is_null(i) {
                            None
                        } else {
                            Some(TimezoneOffset::new_unchecked(
                                self.timestamp_offsets().value(i),
                            ))
                        },
                    );
                    let dt_type = self.date_time_type().value(i);
                    let formatted = if dt_type == DateTimeFamily::DATE_TIME_TYPE_ID {
                        DateTime::new(ts).to_string()
                    } else if dt_type == DateTimeFamily::DATE_TYPE_ID {
                        Date::new(ts).to_string()
                    } else {
                        Time::new(ts).to_string()
                    };
                    Some(formatted)
                } else {
                    None
                }
            })
            .collect::<StringArray>())
    }

    fn effective_boolean_value(&self) -> Result<BooleanArray, ArrowError> {
        Ok(BooleanArray::new_null(self.inner_ref().len()))
    }

    fn literal_data_types(&self) -> Result<StringArray, ArrowError> {
        let len = self.inner_ref().len();
        Ok(StringArray::from_iter_values((0..len).map(|i| {
            let dt_type = self.date_time_type().value(i);
            if dt_type == DateTimeFamily::DATE_TIME_TYPE_ID {
                xsd::DATE_TIME.as_str()
            } else if dt_type == DateTimeFamily::DATE_TYPE_ID {
                xsd::DATE.as_str()
            } else {
                xsd::TIME.as_str()
            }
        })))
    }

    fn cast_to_plain_term_array(&self) -> AResult<PlainTermArray> {
        let len = self.inner_ref().len();
        let values = self.pretty_print()?;
        let datatypes = self.literal_data_types()?;
        PlainTermArray::try_new_literals(
            values,
            datatypes,
            StringArray::new_null(len),
            None,
        )
    }

    fn cast_to_sortable_bytes(&self) -> Result<BinaryArray, ArrowError> {
        let mut builder = GenericBinaryBuilder::<i32>::with_capacity(
            self.inner_ref().len(),
            self.inner_ref().len() * 18,
        );
        for i in 0..self.inner_ref().len() {
            if self.array.is_null(i) {
                builder.append_null();
            } else {
                let ts = self.get_timestamp(i);
                builder.append_value(ts.to_be_bytes());
            }
        }
        Ok(builder.finish())
    }
}

/// A builder for creating an array of the [`DateTimeFamily`].
pub struct DateTimeArrayBuilder {
    date_time_types: UInt8Array,
    timestamp_values: Decimal128Array,
    timestamp_offsets: Int16Array,
}

impl DateTimeArrayBuilder {
    /// Creates a new [`DateTimeArrayBuilder`].
    pub fn new(
        date_time_types: UInt8Array,
        timestamp_values: Decimal128Array,
        timestamp_offsets: Int16Array,
    ) -> Self {
        Self {
            date_time_types,
            timestamp_values,
            timestamp_offsets,
        }
    }

    /// Builds the array.
    pub fn finish(self) -> AResult<ArrayRef> {
        let nulls = self.date_time_types.nulls().cloned();
        let struct_array = StructArray::try_new(
            FIELDS_TIMESTAMP.clone(),
            vec![
                Arc::new(self.date_time_types) as ArrayRef,
                Arc::new(self.timestamp_values) as ArrayRef,
                Arc::new(self.timestamp_offsets) as ArrayRef,
            ],
            nulls,
        )?;
        Ok(Arc::new(struct_array) as ArrayRef)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::arrow::util::pretty::pretty_format_batches;

    #[test]
    fn test_date_time_family_pretty_print() {
        let ts =
            Timestamp::new(Decimal::from(1000), Some(TimezoneOffset::new_unchecked(60)));
        let dt = DateTime::new(ts);

        let array = DateTimeArrayBuilder::new(
            UInt8Array::from(vec![DateTimeFamily::DATE_TIME_TYPE_ID]),
            Decimal128Array::from_iter_values(vec![i128::from_be_bytes(
                dt.timestamp().value().to_be_bytes(),
            )])
            .with_precision_and_scale(Decimal::PRECISION, Decimal::SCALE)
            .unwrap(),
            Int16Array::from(vec![Some(60)]),
        )
        .finish()
        .unwrap();

        let family_array = DateTimeFamilyArray::from_array_unchecked(array);
        let pretty = family_array.pretty_print().unwrap();
        let batch =
            RecordBatch::try_from_iter(vec![("pretty", Arc::new(pretty) as ArrayRef)])
                .unwrap();
        let formatted = pretty_format_batches(&[batch]).unwrap().to_string();

        insta::assert_snapshot!(formatted, @r"
            +---------------------------+
            | pretty                    |
            +---------------------------+
            | 0001-01-01T01:16:40+01:00 |
            +---------------------------+
            ");
    }
}
