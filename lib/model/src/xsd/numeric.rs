use crate::xsd::decimal::Decimal;
use crate::xsd::double::Double;
use crate::xsd::float::Float;
use crate::xsd::integer::Integer;
use crate::{Int, ThinResult};
use std::cmp::Ordering;
use std::fmt::{Display, Formatter};
use std::hash::Hash;
use std::ops::Neg;

#[derive(Copy, Clone, Debug)]
pub enum Numeric {
    Int(Int),
    Integer(Integer),
    Float(Float),
    Double(Double),
    Decimal(Decimal),
}

impl Numeric {
    pub fn checked_add(&self, rhs: Self) -> ThinResult<Numeric> {
        match NumericPair::with_casts_from(*self, rhs) {
            NumericPair::Int(lhs, rhs) => lhs.checked_add(rhs).map(Numeric::Int),
            NumericPair::Integer(lhs, rhs) => lhs.checked_add(rhs).map(Numeric::Integer),
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs + rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs + rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_add(rhs).map(Numeric::Decimal),
        }
    }

    pub fn checked_sub(&self, rhs: Self) -> ThinResult<Numeric> {
        match NumericPair::with_casts_from(*self, rhs) {
            NumericPair::Int(lhs, rhs) => lhs.checked_sub(rhs).map(Numeric::Int),
            NumericPair::Integer(lhs, rhs) => lhs.checked_sub(rhs).map(Numeric::Integer),
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs - rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs - rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_sub(rhs).map(Numeric::Decimal),
        }
    }

    pub fn checked_mul(&self, rhs: Self) -> ThinResult<Numeric> {
        match NumericPair::with_casts_from(*self, rhs) {
            NumericPair::Int(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Int),
            NumericPair::Integer(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Integer),
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs * rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs * rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_mul(rhs).map(Numeric::Decimal),
        }
    }

    pub fn div(&self, rhs: Self) -> ThinResult<Numeric> {
        match NumericPair::with_casts_from(*self, rhs) {
            NumericPair::Int(lhs, rhs) => Decimal::from(lhs)
                .checked_div(Decimal::from(rhs))
                .map(Numeric::Decimal),
            NumericPair::Integer(lhs, rhs) => Decimal::from(lhs)
                .checked_div(Decimal::from(rhs))
                .map(Numeric::Decimal),
            NumericPair::Float(lhs, rhs) => Ok(Numeric::Float(lhs / rhs)),
            NumericPair::Double(lhs, rhs) => Ok(Numeric::Double(lhs / rhs)),
            NumericPair::Decimal(lhs, rhs) => lhs.checked_div(rhs).map(Numeric::Decimal),
        }
    }

    pub fn abs(&self) -> ThinResult<Numeric> {
        match self {
            Numeric::Int(value) => value.checked_abs().map(Numeric::Int),
            Numeric::Integer(value) => Ok(Numeric::Integer(value.checked_abs()?)),
            Numeric::Float(value) => Ok(Numeric::Float(value.abs())),
            Numeric::Double(value) => Ok(Numeric::Double(value.abs())),
            Numeric::Decimal(value) => value.checked_abs().map(Numeric::Decimal),
        }
    }

    pub fn neg(&self) -> ThinResult<Numeric> {
        match self {
            Numeric::Int(value) => value.checked_neg().map(Numeric::Int),
            Numeric::Integer(value) => Ok(Numeric::Integer(value.checked_neg()?)),
            Numeric::Float(value) => Ok(Numeric::Float(value.neg())),
            Numeric::Double(value) => Ok(Numeric::Double(value.neg())),
            Numeric::Decimal(value) => value.checked_neg().map(Numeric::Decimal),
        }
    }

    #[must_use]
    pub fn format_value(&self) -> String {
        match self {
            Numeric::Int(value) => value.to_string(),
            Numeric::Integer(value) => value.to_string(),
            Numeric::Float(value) => value.to_string(),
            Numeric::Double(value) => value.to_string(),
            Numeric::Decimal(value) => value.to_string(),
        }
    }

    #[must_use]
    pub fn to_be_bytes(self) -> Box<[u8]> {
        match self {
            Numeric::Int(int) => int.to_be_bytes().into(),
            Numeric::Integer(int) => int.to_be_bytes().into(),
            Numeric::Float(float) => float.to_be_bytes().into(),
            Numeric::Double(double) => double.to_be_bytes().into(),
            Numeric::Decimal(decimal) => decimal.to_be_bytes().into(),
        }
    }

    #[must_use]
    pub fn is_zero(&self) -> bool {
        match self {
            Self::Int(v) => i32::from(*v) == 0,
            Self::Integer(v) => i64::from(*v) == 0,
            Self::Float(v) => f32::from(*v) == 0.0,
            Self::Double(v) => f64::from(*v) == 0.0,
            Self::Decimal(v) => v.is_zero(),
        }
    }
}

impl PartialEq for Numeric {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Int(lhs), Self::Int(rhs)) => lhs == rhs,
            (Self::Integer(lhs), Self::Integer(rhs)) => lhs == rhs,
            (Self::Float(lhs), Self::Float(rhs)) => lhs == rhs,
            (Self::Double(lhs), Self::Double(rhs)) => lhs == rhs,
            (Self::Decimal(lhs), Self::Decimal(rhs)) => lhs == rhs,
            _ => false,
        }
    }
}

impl Eq for Numeric {}

impl Hash for Numeric {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Numeric::Int(int) => int.hash(state),
            Numeric::Integer(int) => int.hash(state),
            Numeric::Float(float) => float.to_be_bytes().hash(state),
            Numeric::Double(double) => double.to_be_bytes().hash(state),
            Numeric::Decimal(decimal) => decimal.hash(state),
        }
    }
}

impl PartialOrd for Numeric {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match NumericPair::with_casts_from(*self, *other) {
            NumericPair::Int(lhs, rhs) => Some(lhs.cmp(&rhs)),
            NumericPair::Integer(lhs, rhs) => Some(lhs.cmp(&rhs)),
            NumericPair::Float(lhs, rhs) => lhs.partial_cmp(&rhs),
            NumericPair::Double(lhs, rhs) => lhs.partial_cmp(&rhs),
            NumericPair::Decimal(lhs, rhs) => Some(lhs.cmp(&rhs)),
        }
    }
}

impl Display for Numeric {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Numeric::Int(v) => write!(f, "{v}"),
            Numeric::Integer(v) => write!(f, "{v}"),
            Numeric::Float(v) => write!(f, "{v}"),
            Numeric::Double(v) => write!(f, "{v}"),
            Numeric::Decimal(v) => write!(f, "{v}"),
        }
    }
}

macro_rules! impl_from {
    ($TYPE: ty, $VARIANT: path) => {
        impl From<$TYPE> for Numeric {
            fn from(value: $TYPE) -> Self {
                $VARIANT(value)
            }
        }
    };
}

impl_from!(Int, Numeric::Int);
impl_from!(Integer, Numeric::Integer);
impl_from!(Float, Numeric::Float);
impl_from!(Double, Numeric::Double);
impl_from!(Decimal, Numeric::Decimal);

pub enum NumericPair {
    Int(Int, Int),
    Integer(Integer, Integer),
    Float(Float, Float),
    Double(Double, Double),
    Decimal(Decimal, Decimal),
}

impl NumericPair {
    pub fn with_casts_from(lhs: Numeric, rhs: Numeric) -> NumericPair {
        match (lhs, rhs) {
            (Numeric::Int(lhs), Numeric::Int(rhs)) => NumericPair::Int(lhs, rhs),
            (Numeric::Int(lhs), Numeric::Integer(rhs)) => {
                NumericPair::Integer(lhs.into(), rhs)
            }
            (Numeric::Int(lhs), Numeric::Float(rhs)) => {
                NumericPair::Float(lhs.into(), rhs)
            }
            (Numeric::Int(lhs), Numeric::Double(rhs)) => {
                NumericPair::Double(lhs.into(), rhs)
            }
            (Numeric::Int(lhs), Numeric::Decimal(rhs)) => {
                NumericPair::Decimal(Decimal::from(lhs), rhs)
            }

            (Numeric::Integer(lhs), Numeric::Int(rhs)) => {
                NumericPair::Integer(lhs, rhs.into())
            }
            (Numeric::Integer(lhs), Numeric::Integer(rhs)) => {
                NumericPair::Integer(lhs, rhs)
            }
            (Numeric::Integer(lhs), Numeric::Float(rhs)) => {
                NumericPair::Float(lhs.into(), rhs)
            }
            (Numeric::Integer(lhs), Numeric::Double(rhs)) => {
                NumericPair::Double(lhs.into(), rhs)
            }
            (Numeric::Integer(lhs), Numeric::Decimal(rhs)) => {
                NumericPair::Decimal(Decimal::from(lhs), rhs)
            }

            (Numeric::Float(lhs), Numeric::Int(rhs)) => {
                NumericPair::Float(lhs, rhs.into())
            }
            (Numeric::Float(lhs), Numeric::Integer(rhs)) => {
                NumericPair::Float(lhs, rhs.into())
            }
            (Numeric::Float(lhs), Numeric::Float(rhs)) => NumericPair::Float(lhs, rhs),
            (Numeric::Float(lhs), Numeric::Double(rhs)) => {
                NumericPair::Double(lhs.into(), rhs)
            }
            (Numeric::Float(lhs), Numeric::Decimal(rhs)) => {
                NumericPair::Float(lhs, rhs.into())
            }
            (Numeric::Double(lhs), Numeric::Int(rhs)) => {
                NumericPair::Double(lhs, Integer::from(rhs).into())
            }
            (Numeric::Double(lhs), Numeric::Integer(rhs)) => {
                NumericPair::Double(lhs, rhs.into())
            }
            (Numeric::Double(lhs), Numeric::Float(rhs)) => {
                NumericPair::Double(lhs, rhs.into())
            }
            (Numeric::Double(lhs), Numeric::Double(rhs)) => NumericPair::Double(lhs, rhs),
            (Numeric::Double(lhs), Numeric::Decimal(rhs)) => {
                NumericPair::Double(lhs, rhs.into())
            }
            (Numeric::Decimal(lhs), Numeric::Int(rhs)) => {
                NumericPair::Decimal(lhs, rhs.into())
            }
            (Numeric::Decimal(lhs), Numeric::Integer(rhs)) => {
                NumericPair::Decimal(lhs, rhs.into())
            }
            (Numeric::Decimal(lhs), Numeric::Float(rhs)) => {
                NumericPair::Float(lhs.into(), rhs)
            }
            (Numeric::Decimal(lhs), Numeric::Double(rhs)) => {
                NumericPair::Double(lhs.into(), rhs)
            }
            (Numeric::Decimal(lhs), Numeric::Decimal(rhs)) => {
                NumericPair::Decimal(lhs, rhs)
            }
        }
    }
}
