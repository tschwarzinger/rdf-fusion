use std::fmt::Display;
use thiserror::Error;

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub(super) enum SortableTermType {
    Null,
    BlankNodes,
    NamedNode,
    Boolean,
    Numeric,
    String,
    DateTime,
    Time,
    Date,
    Duration,
    YearMonthDuration,
    DayTimeDuration,
    UnsupportedLiteral,
}

#[derive(Debug, Clone, Copy, Default, Error, PartialEq, Eq, Hash)]
pub struct UnknownSortableTermTypeError;

impl Display for UnknownSortableTermTypeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Invalid value for SortableTermType")
    }
}

impl TryFrom<i8> for SortableTermType {
    type Error = UnknownSortableTermTypeError;

    fn try_from(value: i8) -> Result<Self, Self::Error> {
        let term_type = match value {
            0 => SortableTermType::Null,
            1 => SortableTermType::BlankNodes,
            2 => SortableTermType::NamedNode,
            3 => SortableTermType::Boolean,
            4 => SortableTermType::Numeric,
            5 => SortableTermType::String,
            6 => SortableTermType::DateTime,
            7 => SortableTermType::Time,
            8 => SortableTermType::Date,
            9 => SortableTermType::Duration,
            10 => SortableTermType::YearMonthDuration,
            11 => SortableTermType::DayTimeDuration,
            12 => SortableTermType::UnsupportedLiteral,
            _ => return Err(UnknownSortableTermTypeError),
        };
        Ok(term_type)
    }
}

impl From<SortableTermType> for i8 {
    fn from(value: SortableTermType) -> Self {
        match value {
            SortableTermType::Null => 0,
            SortableTermType::BlankNodes => 1,
            SortableTermType::NamedNode => 2,
            SortableTermType::Boolean => 3,
            SortableTermType::Numeric => 4,
            SortableTermType::String => 5,
            SortableTermType::DateTime => 6,
            SortableTermType::Time => 7,
            SortableTermType::Date => 8,
            SortableTermType::Duration => 9,
            SortableTermType::YearMonthDuration => 10,
            SortableTermType::DayTimeDuration => 11,
            SortableTermType::UnsupportedLiteral => 12,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_term_type_roundtrip() {
        test_roundtrip(SortableTermType::Null);
        test_roundtrip(SortableTermType::BlankNodes);
        test_roundtrip(SortableTermType::NamedNode);
        test_roundtrip(SortableTermType::Boolean);
        test_roundtrip(SortableTermType::Numeric);
        test_roundtrip(SortableTermType::String);
        test_roundtrip(SortableTermType::DateTime);
        test_roundtrip(SortableTermType::Time);
        test_roundtrip(SortableTermType::Date);
        test_roundtrip(SortableTermType::Duration);
        test_roundtrip(SortableTermType::YearMonthDuration);
        test_roundtrip(SortableTermType::DayTimeDuration);
        test_roundtrip(SortableTermType::UnsupportedLiteral);
    }

    fn test_roundtrip(term_field: SortableTermType) {
        let value: i8 = term_field.into();
        assert_eq!(term_field, value.try_into().unwrap());
    }
}
