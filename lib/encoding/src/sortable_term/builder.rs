use crate::sortable_term::encoding::{SortableTermEncoding, SortableTermEncodingField};
use crate::sortable_term::term_type::SortableTermType;
use datafusion::arrow::array::{
    ArrayRef, BinaryBuilder, Float64Builder, Int8Builder, StructBuilder,
};
use rdf_fusion_common::{BlankNodeRef, LiteralRef, NamedNodeRef};
use rdf_fusion_common::{
    Boolean, Date, DateTime, DayTimeDuration, Double, Duration, Integer, Numeric, Time,
    YearMonthDuration,
};
use std::sync::Arc;

pub struct SortableTermArrayBuilder {
    builder: StructBuilder,
}

impl SortableTermArrayBuilder {
    pub fn new(capacity: usize) -> Self {
        Self {
            builder: StructBuilder::from_fields(SortableTermEncoding::fields(), capacity),
        }
    }

    pub fn append_null(&mut self) {
        self.append(SortableTermType::Null, None, &[])
    }

    pub fn append_boolean(&mut self, value: Boolean) {
        self.append(
            SortableTermType::Boolean,
            Some(value.into()),
            &value.to_be_bytes(),
        )
    }

    pub fn append_numeric(&mut self, value: Numeric, original_be_bytes: &[u8]) {
        let value = Double::from(value);
        self.append(SortableTermType::Numeric, Some(value), original_be_bytes)
    }

    pub fn append_blank_node(&mut self, value: BlankNodeRef<'_>) {
        self.append(
            SortableTermType::BlankNodes,
            None,
            value.as_str().as_bytes(),
        )
    }

    pub fn append_named_node(&mut self, value: NamedNodeRef<'_>) {
        self.append(SortableTermType::NamedNode, None, value.as_str().as_bytes())
    }

    pub fn append_string(&mut self, value: &str) {
        self.append(SortableTermType::String, None, value.as_bytes())
    }

    pub fn append_date_time(&mut self, value: DateTime) {
        self.append(
            SortableTermType::DateTime,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
        )
    }

    pub fn append_time(&mut self, value: Time) {
        self.append(
            SortableTermType::Time,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
        )
    }

    pub fn append_date(&mut self, value: Date) {
        self.append(
            SortableTermType::Date,
            Some(value.timestamp().value().into()),
            &value.to_be_bytes(),
        )
    }

    pub fn append_duration(&mut self, value: Duration) {
        self.append(
            SortableTermType::Duration,
            None, // Sort by bytes
            &value.to_be_bytes(),
        )
    }

    pub fn append_year_month_duration(&mut self, value: YearMonthDuration) {
        self.append(
            SortableTermType::YearMonthDuration,
            Some(Integer::from(value.as_i64()).into()),
            Duration::from(value).to_be_bytes().as_slice(),
        )
    }

    pub fn append_day_time_duration(&mut self, value: DayTimeDuration) {
        self.append(
            SortableTermType::DayTimeDuration,
            Some(value.as_seconds().into()),
            Duration::from(value).to_be_bytes().as_slice(),
        )
    }

    pub fn append_literal(&mut self, literal: LiteralRef<'_>) {
        self.append(
            SortableTermType::UnsupportedLiteral,
            None,
            literal.value().as_bytes(),
        )
    }

    fn append(
        &mut self,
        sort_type: SortableTermType,
        numeric: Option<Double>,
        bytes: &[u8],
    ) {
        self.builder
            .field_builder::<Int8Builder>(SortableTermEncodingField::Type.index())
            .unwrap()
            .append_value(sort_type.into());

        let numeric_builder = self
            .builder
            .field_builder::<Float64Builder>(SortableTermEncodingField::Numeric.index())
            .unwrap();
        match numeric {
            None => numeric_builder.append_null(),
            Some(numeric) => numeric_builder.append_value(numeric.into()),
        }

        let bytes_builder = self
            .builder
            .field_builder::<BinaryBuilder>(SortableTermEncodingField::Bytes.index())
            .unwrap();
        bytes_builder.append_value(bytes);

        self.builder.append(true)
    }

    pub fn finish(mut self) -> ArrayRef {
        Arc::new(self.builder.finish())
    }
}
