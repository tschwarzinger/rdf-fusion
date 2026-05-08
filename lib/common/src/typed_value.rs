use crate::{
    Boolean, Date, DateTime, DayTimeDuration, Decimal, Double, Duration, Float, Int,
    Integer, LanguageString, LanguageStringRef, Numeric, ParseDateTimeError,
    ParseDecimalError, ParseDurationError, SimpleLiteral, SimpleLiteralRef, Term, Time,
    YearMonthDuration,
};
use oxrdf::vocab::xsd;
use oxrdf::{
    BlankNode, BlankNodeRef, Literal, LiteralRef, NamedNode, NamedNodeRef, TermRef,
};
use std::cmp::Ordering;
use std::num::{ParseFloatError, ParseIntError};
use std::str::ParseBoolError;
use thiserror::Error;

#[derive(Clone, PartialEq, Eq, Debug, Hash)]
pub enum TypedValue {
    /// An RDF named node (IRI).
    ///
    /// # Additional Resources
    /// - [RDF 1.1 Concepts - IRIs](https://www.w3.org/TR/rdf11-concepts/#section-IRIs)
    NamedNode(NamedNode),
    /// An RDF blank node.
    ///
    /// # Additional Resources
    /// - [RDF 1.1 Concepts - Blank Nodes](https://www.w3.org/TR/rdf11-concepts/#section-blank-nodes)
    BlankNode(BlankNode),
    /// A boolean literal with datatype xsd:boolean.
    ///
    /// # Additional Resources
    /// - [XSD Boolean Datatype](https://www.w3.org/TR/xmlschema11-2/#boolean)
    BooleanLiteral(Boolean),
    /// A numeric literal (integer, decimal, float, or double).
    ///
    /// # Additional Resources
    /// - [XSD Numeric Datatypes](https://www.w3.org/TR/xmlschema11-2/#built-in-datatypes)
    NumericLiteral(Numeric),
    /// A simple string literal without a language tag.
    ///
    /// # Additional Resources
    /// - [RDF 1.1 Concepts - Literals](https://www.w3.org/TR/rdf11-concepts/#section-Graph-Literal)
    SimpleLiteral(SimpleLiteral),
    /// A string literal with a language tag.
    ///
    /// # Additional Resources
    /// - [RDF 1.1 Concepts - Language-tagged Strings](https://www.w3.org/TR/rdf11-concepts/#dfn-language-tagged-string)
    LanguageStringLiteral(LanguageString),
    /// A dateTime literal with datatype xsd:dateTime.
    ///
    /// # Additional Resources
    /// - [XSD DateTime Datatype](https://www.w3.org/TR/xmlschema11-2/#dateTime)
    DateTimeLiteral(DateTime),
    /// A time literal with datatype xsd:time.
    ///
    /// # Additional Resources
    /// - [XSD Time Datatype](https://www.w3.org/TR/xmlschema11-2/#time)
    TimeLiteral(Time),
    /// A date literal with datatype xsd:date.
    ///
    /// # Additional Resources
    /// - [XSD Date Datatype](https://www.w3.org/TR/xmlschema11-2/#date)
    DateLiteral(Date),
    /// A duration literal with datatype xsd:duration.
    ///
    /// # Additional Resources
    /// - [XSD Duration Datatype](https://www.w3.org/TR/xmlschema11-2/#duration)
    DurationLiteral(Duration),
    /// A year-month duration literal with datatype xsd:yearMonthDuration.
    ///
    /// # Additional Resources
    /// - [XSD YearMonthDuration Datatype](https://www.w3.org/TR/xmlschema11-2/#yearMonthDuration)
    YearMonthDurationLiteral(YearMonthDuration),
    /// A day-time duration literal with datatype xsd:dayTimeDuration.
    ///
    /// # Additional Resources
    /// - [XSD DayTimeDuration Datatype](https://www.w3.org/TR/xmlschema11-2/#dayTimeDuration)
    DayTimeDurationLiteral(DayTimeDuration),
    /// A literal with a datatype not specifically handled by other variants.
    ///
    /// # Additional Resources
    /// - [RDF 1.1 Concepts - Datatypes](https://www.w3.org/TR/rdf11-concepts/#section-Datatypes)
    OtherLiteral(Literal),
}

impl TypedValue {
    pub fn as_ref(&self) -> TypedValueRef<'_> {
        match self {
            TypedValue::NamedNode(inner) => TypedValueRef::NamedNode(inner.as_ref()),
            TypedValue::BlankNode(inner) => TypedValueRef::BlankNode(inner.as_ref()),
            TypedValue::BooleanLiteral(inner) => TypedValueRef::BooleanLiteral(*inner),
            TypedValue::NumericLiteral(inner) => TypedValueRef::NumericLiteral(*inner),
            TypedValue::SimpleLiteral(inner) => {
                TypedValueRef::SimpleLiteral(inner.as_ref())
            }
            TypedValue::LanguageStringLiteral(inner) => {
                TypedValueRef::LanguageStringLiteral(inner.as_ref())
            }
            TypedValue::DateTimeLiteral(inner) => TypedValueRef::DateTimeLiteral(*inner),
            TypedValue::TimeLiteral(inner) => TypedValueRef::TimeLiteral(*inner),
            TypedValue::DateLiteral(inner) => TypedValueRef::DateLiteral(*inner),
            TypedValue::DurationLiteral(inner) => TypedValueRef::DurationLiteral(*inner),
            TypedValue::YearMonthDurationLiteral(inner) => {
                TypedValueRef::YearMonthDurationLiteral(*inner)
            }
            TypedValue::DayTimeDurationLiteral(inner) => {
                TypedValueRef::DayTimeDurationLiteral(*inner)
            }
            TypedValue::OtherLiteral(inner) => {
                TypedValueRef::OtherLiteral(inner.as_ref())
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Debug)]
pub enum TypedValueRef<'value> {
    NamedNode(NamedNodeRef<'value>),
    BlankNode(BlankNodeRef<'value>),
    BooleanLiteral(Boolean),
    NumericLiteral(Numeric),
    SimpleLiteral(SimpleLiteralRef<'value>),
    LanguageStringLiteral(LanguageStringRef<'value>),
    DateTimeLiteral(DateTime),
    TimeLiteral(Time),
    DateLiteral(Date),
    DurationLiteral(Duration),
    YearMonthDurationLiteral(YearMonthDuration),
    DayTimeDurationLiteral(DayTimeDuration),
    OtherLiteral(LiteralRef<'value>),
}

impl TypedValueRef<'_> {
    pub fn into_owned(self) -> TypedValue {
        match self {
            TypedValueRef::NamedNode(inner) => TypedValue::NamedNode(inner.into_owned()),
            TypedValueRef::BlankNode(inner) => TypedValue::BlankNode(inner.into_owned()),
            TypedValueRef::BooleanLiteral(inner) => TypedValue::BooleanLiteral(inner),
            TypedValueRef::NumericLiteral(inner) => TypedValue::NumericLiteral(inner),
            TypedValueRef::SimpleLiteral(inner) => {
                TypedValue::SimpleLiteral(inner.into_owned())
            }
            TypedValueRef::LanguageStringLiteral(inner) => {
                TypedValue::LanguageStringLiteral(inner.into_owned())
            }
            TypedValueRef::DateTimeLiteral(inner) => TypedValue::DateTimeLiteral(inner),
            TypedValueRef::TimeLiteral(inner) => TypedValue::TimeLiteral(inner),
            TypedValueRef::DateLiteral(inner) => TypedValue::DateLiteral(inner),
            TypedValueRef::DurationLiteral(inner) => TypedValue::DurationLiteral(inner),
            TypedValueRef::YearMonthDurationLiteral(inner) => {
                TypedValue::YearMonthDurationLiteral(inner)
            }
            TypedValueRef::DayTimeDurationLiteral(inner) => {
                TypedValue::DayTimeDurationLiteral(inner)
            }
            TypedValueRef::OtherLiteral(inner) => {
                TypedValue::OtherLiteral(inner.into_owned())
            }
        }
    }
}

impl PartialOrd for TypedValueRef<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match *self {
            TypedValueRef::BlankNode(a) => Some(match other {
                TypedValueRef::BlankNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            TypedValueRef::NamedNode(a) => Some(match other {
                TypedValueRef::BlankNode(_) => Ordering::Greater,
                TypedValueRef::NamedNode(b) => a.as_str().cmp(b.as_str()),
                _ => Ordering::Less,
            }),
            a => match other {
                TypedValueRef::NamedNode(_) | TypedValueRef::BlankNode(_) => {
                    Some(Ordering::Greater)
                }
                _ => partial_cmp_literals(a, *other),
            },
        }
    }
}

fn partial_cmp_literals(a: TypedValueRef<'_>, b: TypedValueRef<'_>) -> Option<Ordering> {
    match a {
        TypedValueRef::SimpleLiteral(a) => {
            if let TypedValueRef::SimpleLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::LanguageStringLiteral(a) => {
            if let TypedValueRef::LanguageStringLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::BooleanLiteral(a) => {
            if let TypedValueRef::BooleanLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::NumericLiteral(a) => {
            if let TypedValueRef::NumericLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::DateTimeLiteral(a) => {
            if let TypedValueRef::DateTimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::TimeLiteral(a) => {
            if let TypedValueRef::TimeLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::DateLiteral(a) => {
            if let TypedValueRef::DateLiteral(b) = b {
                a.partial_cmp(&b)
            } else {
                None
            }
        }
        TypedValueRef::DurationLiteral(a) => match b {
            TypedValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TypedValueRef::YearMonthDurationLiteral(a) => match b {
            TypedValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TypedValueRef::DayTimeDurationLiteral(a) => match b {
            TypedValueRef::DurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::YearMonthDurationLiteral(b) => a.partial_cmp(&b),
            TypedValueRef::DayTimeDurationLiteral(b) => a.partial_cmp(&b),
            _ => None,
        },
        TypedValueRef::OtherLiteral(a) => match b {
            TypedValueRef::OtherLiteral(b) if a.datatype() == b.datatype() => {
                (a.value() == b.value()).then_some(Ordering::Equal)
            }
            _ => None,
        },
        _ => None,
    }
}

macro_rules! impl_from {
    ($TYPE: ty, $VARIANT: path) => {
        impl<'data> From<$TYPE> for TypedValueRef<'data> {
            fn from(value: $TYPE) -> Self {
                $VARIANT(value)
            }
        }
    };
}

impl_from!(Boolean, TypedValueRef::BooleanLiteral);
impl_from!(Numeric, TypedValueRef::NumericLiteral);
impl_from!(SimpleLiteralRef<'data>, TypedValueRef::SimpleLiteral);
impl_from!(
    LanguageStringRef<'data>,
    TypedValueRef::LanguageStringLiteral
);
impl_from!(Duration, TypedValueRef::DurationLiteral);
impl_from!(YearMonthDuration, TypedValueRef::YearMonthDurationLiteral);
impl_from!(DayTimeDuration, TypedValueRef::DayTimeDurationLiteral);
impl_from!(Date, TypedValueRef::DateLiteral);
impl_from!(Time, TypedValueRef::TimeLiteral);
impl_from!(DateTime, TypedValueRef::DateTimeLiteral);

impl From<TypedValueRef<'_>> for Term {
    fn from(value: TypedValueRef<'_>) -> Self {
        match value {
            TypedValueRef::NamedNode(value) => Term::NamedNode(value.into_owned()),
            TypedValueRef::BlankNode(value) => Term::BlankNode(value.into_owned()),
            TypedValueRef::BooleanLiteral(value) => {
                Term::Literal(Literal::from(value.as_bool()))
            }
            TypedValueRef::NumericLiteral(value) => match value {
                Numeric::Int(value) => Term::Literal(Literal::from(i32::from(value))),
                Numeric::Integer(value) => Term::Literal(Literal::from(i64::from(value))),
                Numeric::Float(value) => Term::Literal(Literal::from(f32::from(value))),
                Numeric::Double(value) => Term::Literal(Literal::from(f64::from(value))),
                Numeric::Decimal(value) => Term::Literal(Literal::new_typed_literal(
                    value.to_string(),
                    xsd::DECIMAL,
                )),
            },
            TypedValueRef::SimpleLiteral(value) => {
                Term::Literal(Literal::from(value.value))
            }
            TypedValueRef::LanguageStringLiteral(value) => {
                Term::Literal(Literal::new_language_tagged_literal_unchecked(
                    value.value,
                    value.language,
                ))
            }
            TypedValueRef::DateTimeLiteral(value) => Term::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::DATE_TIME),
            ),
            TypedValueRef::TimeLiteral(value) => {
                Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::TIME))
            }
            TypedValueRef::DateLiteral(value) => {
                Term::Literal(Literal::new_typed_literal(value.to_string(), xsd::DATE))
            }
            TypedValueRef::DurationLiteral(value) => Term::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::DURATION),
            ),
            TypedValueRef::YearMonthDurationLiteral(value) => Term::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::YEAR_MONTH_DURATION),
            ),
            TypedValueRef::DayTimeDurationLiteral(value) => Term::Literal(
                Literal::new_typed_literal(value.to_string(), xsd::DAY_TIME_DURATION),
            ),
            TypedValueRef::OtherLiteral(value) => Term::Literal(value.into_owned()),
        }
    }
}

impl<'data> TryFrom<TermRef<'data>> for TypedValueRef<'data> {
    type Error = TermToTypedValueError;

    fn try_from(value: TermRef<'data>) -> Result<Self, Self::Error> {
        match value {
            TermRef::NamedNode(named_node) => Ok(Self::NamedNode(named_node)),
            TermRef::BlankNode(bnode) => Ok(Self::BlankNode(bnode)),
            TermRef::Literal(literal) => literal.try_into(),
        }
    }
}

impl<'data> TryFrom<LiteralRef<'data>> for TypedValueRef<'data> {
    type Error = TermToTypedValueError;

    fn try_from(value: LiteralRef<'data>) -> Result<Self, Self::Error> {
        if let Some(language) = value.language() {
            return Ok(LanguageStringRef::new(value.value(), language).into());
        }

        let result: TypedValueRef<'data> = match value.datatype() {
            // TODO: Other literals
            xsd::BOOLEAN => value.value().parse::<Boolean>().map(Into::into)?,
            xsd::FLOAT => value
                .value()
                .parse::<Float>()
                .map(Into::into)
                .map(Self::NumericLiteral)?,
            xsd::DOUBLE => value
                .value()
                .parse::<Double>()
                .map(Into::into)
                .map(Self::NumericLiteral)?,
            xsd::DECIMAL => value
                .value()
                .parse::<Decimal>()
                .map(Into::into)
                .map(Self::NumericLiteral)?,
            xsd::BYTE
            | xsd::SHORT
            | xsd::LONG
            | xsd::UNSIGNED_BYTE
            | xsd::UNSIGNED_SHORT
            | xsd::UNSIGNED_INT
            | xsd::UNSIGNED_LONG
            | xsd::POSITIVE_INTEGER
            | xsd::NEGATIVE_INTEGER
            | xsd::NON_POSITIVE_INTEGER
            | xsd::NON_NEGATIVE_INTEGER
            | xsd::INTEGER => value
                .value()
                .parse::<Integer>()
                .map(Into::into)
                .map(Self::NumericLiteral)?,
            xsd::INT => value
                .value()
                .parse::<Int>()
                .map(Into::into)
                .map(Self::NumericLiteral)?,
            xsd::DURATION => value.value().parse::<Duration>().map(Into::into)?,
            xsd::YEAR_MONTH_DURATION => {
                value.value().parse::<YearMonthDuration>().map(Into::into)?
            }
            xsd::DAY_TIME_DURATION => {
                value.value().parse::<DayTimeDuration>().map(Into::into)?
            }
            xsd::DATE_TIME => value.value().parse::<DateTime>().map(Into::into)?,
            xsd::TIME => value.value().parse::<Time>().map(Into::into)?,
            xsd::DATE => value.value().parse::<Date>().map(Into::into)?,
            xsd::STRING => SimpleLiteralRef::new(value.value()).into(),
            _ => TypedValueRef::OtherLiteral(value),
        };
        Ok(result)
    }
}

#[derive(Debug, Error)]
pub struct TermToTypedValueError;

impl Default for TermToTypedValueError {
    fn default() -> Self {
        Self::new()
    }
}

impl TermToTypedValueError {
    /// Creates a new [TermToTypedValueError].
    pub fn new() -> Self {
        Self {}
    }
}

impl std::fmt::Display for TermToTypedValueError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Could not convert term to typed value")
    }
}

macro_rules! impl_for_parsing_error {
    ($ERROR: ty) => {
        impl From<$ERROR> for TermToTypedValueError {
            fn from(_: $ERROR) -> Self {
                TermToTypedValueError::new()
            }
        }
    };
}

impl_for_parsing_error!(ParseIntError);
impl_for_parsing_error!(ParseFloatError);
impl_for_parsing_error!(ParseBoolError);
impl_for_parsing_error!(ParseDecimalError);
impl_for_parsing_error!(ParseDurationError);
impl_for_parsing_error!(ParseDateTimeError);
