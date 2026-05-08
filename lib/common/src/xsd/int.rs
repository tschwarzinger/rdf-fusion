use crate::xsd::double::Double;
use crate::{Boolean, Decimal, Float, Integer, ThinError, ThinResult};
use std::fmt;
use std::num::ParseIntError;
use std::str::FromStr;

/// [XML Schema `integer` datatype](https://www.w3.org/TR/xmlschema11-2/#integer)
///
/// Uses internally a [`i32`].
#[derive(Debug, Clone, Copy, Default, Eq, PartialEq, Ord, PartialOrd, Hash)]
#[repr(transparent)]
pub struct Int {
    value: i32,
}

impl Int {
    pub const MAX: Self = Self { value: i32::MAX };
    pub const MIN: Self = Self { value: i32::MIN };

    pub fn new(value: i32) -> Self {
        Self { value }
    }

    #[inline]
    #[must_use]
    pub fn from_be_bytes(bytes: [u8; 4]) -> Self {
        Self {
            value: i32::from_be_bytes(bytes),
        }
    }

    #[inline]
    #[must_use]
    pub fn to_be_bytes(self) -> [u8; 4] {
        self.value.to_be_bytes()
    }

    /// [op:numeric-add](https://www.w3.org/TR/xpath-functions-31/#func-numeric-add)
    ///
    /// Returns `Err` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    pub fn checked_add(self, rhs: impl Into<Self>) -> ThinResult<Self> {
        Ok(Self {
            value: self
                .value
                .checked_add(rhs.into().value)
                .ok_or(ThinError::ExpectedError)?,
        })
    }

    /// [op:numeric-subtract](https://www.w3.org/TR/xpath-functions-31/#func-numeric-subtract)
    ///
    /// Returns `Err` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    pub fn checked_sub(self, rhs: impl Into<Self>) -> ThinResult<Self> {
        Ok(Self {
            value: self
                .value
                .checked_sub(rhs.into().value)
                .ok_or(ThinError::ExpectedError)?,
        })
    }

    /// [op:numeric-multiply](https://www.w3.org/TR/xpath-functions-31/#func-numeric-multiply)
    ///
    /// Returns `Err` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    pub fn checked_mul(self, rhs: impl Into<Self>) -> ThinResult<Self> {
        Ok(Self {
            value: self
                .value
                .checked_mul(rhs.into().value)
                .ok_or(ThinError::ExpectedError)?,
        })
    }

    /// [op:numeric-integer-divide](https://www.w3.org/TR/xpath-functions-31/#func-numeric-integer-divide)
    ///
    /// Returns `Err` in case of division by 0 ([FOAR0001](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0001)) or overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    pub fn checked_div(self, rhs: impl Into<Self>) -> ThinResult<Self> {
        Ok(Self {
            value: self
                .value
                .checked_div(rhs.into().value)
                .ok_or(ThinError::ExpectedError)?,
        })
    }

    /// [op:numeric-mod](https://www.w3.org/TR/xpath-functions-31/#func-numeric-mod)
    ///
    /// Returns `Err` in case of division by 0 ([FOAR0001](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0001)) or overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    pub fn checked_rem(self, rhs: impl Into<Self>) -> ThinResult<Self> {
        Ok(Self {
            value: self
                .value
                .checked_rem(rhs.into().value)
                .ok_or(ThinError::ExpectedError)?,
        })
    }

    /// Euclidean remainder
    ///
    /// Returns `Err` in case of division by 0 ([FOAR0001](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0001)) or overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    pub fn checked_rem_euclid(self, rhs: impl Into<Self>) -> ThinResult<Self> {
        Ok(Self {
            value: self
                .value
                .checked_rem_euclid(rhs.into().value)
                .ok_or(ThinError::ExpectedError)?,
        })
    }

    /// [op:numeric-unary-minus](https://www.w3.org/TR/xpath-functions-31/#func-numeric-unary-minus)
    ///
    /// Returns `Err` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    pub fn checked_neg(self) -> ThinResult<Self> {
        Ok(Self {
            value: self.value.checked_neg().ok_or(ThinError::ExpectedError)?,
        })
    }

    /// [fn:abs](https://www.w3.org/TR/xpath-functions-31/#func-abs)
    ///
    /// Returns `Err` in case of overflow ([FOAR0002](https://www.w3.org/TR/xpath-functions-31/#ERRFOAR0002)).
    #[inline]
    pub fn checked_abs(self) -> ThinResult<Self> {
        Ok(Self {
            value: self.value.checked_abs().ok_or(ThinError::ExpectedError)?,
        })
    }

    #[inline]
    #[must_use]
    pub const fn is_negative(self) -> bool {
        self.value < 0
    }

    #[inline]
    #[must_use]
    pub const fn is_positive(self) -> bool {
        self.value > 0
    }

    /// Checks if the two values are [identical](https://www.w3.org/TR/xmlschema11-2/#identity).
    #[inline]
    #[must_use]
    pub fn is_identical_with(self, other: Self) -> bool {
        self == other
    }
}

impl From<bool> for Int {
    #[inline]
    fn from(value: bool) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i8> for Int {
    #[inline]
    fn from(value: i8) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i16> for Int {
    #[inline]
    fn from(value: i16) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<i32> for Int {
    #[inline]
    fn from(value: i32) -> Self {
        Self { value }
    }
}

impl From<u8> for Int {
    #[inline]
    fn from(value: u8) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<u16> for Int {
    #[inline]
    fn from(value: u16) -> Self {
        Self {
            value: value.into(),
        }
    }
}

impl From<Boolean> for Int {
    #[inline]
    fn from(value: Boolean) -> Self {
        bool::from(value).into()
    }
}

impl From<Int> for i32 {
    #[inline]
    fn from(value: Int) -> Self {
        value.value
    }
}

impl FromStr for Int {
    type Err = ParseIntError;

    #[inline]
    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Ok(i32::from_str(input)?.into())
    }
}

impl fmt::Display for Int {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.value.fmt(f)
    }
}

impl TryFrom<Integer> for Int {
    type Error = TooLargeForIntError;

    #[inline]
    fn try_from(value: Integer) -> Result<Self, Self::Error> {
        Decimal::from(value).try_into()
    }
}

impl TryFrom<Float> for Int {
    type Error = TooLargeForIntError;

    #[inline]
    fn try_from(value: Float) -> Result<Self, Self::Error> {
        Decimal::try_from(value)
            .map_err(|_| TooLargeForIntError)?
            .try_into()
    }
}

impl TryFrom<Double> for Int {
    type Error = TooLargeForIntError;

    #[inline]
    fn try_from(value: Double) -> Result<Self, Self::Error> {
        Decimal::try_from(value)
            .map_err(|_| TooLargeForIntError)?
            .try_into()
    }
}

/// The input is too large to fit into an [`Int`].
///
/// Matches XPath [`FOCA0003` error](https://www.w3.org/TR/xpath-functions-31/#ERRFOCA0003).
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("Value too large for :integer internal representation")]
pub struct TooLargeForIntError;

#[cfg(test)]
#[allow(clippy::panic_in_result_fn)]
mod tests {
    use super::*;

    #[test]
    fn from_str() -> Result<(), ParseIntError> {
        assert_eq!(Int::from_str("0")?.to_string(), "0");
        assert_eq!(Int::from_str("-0")?.to_string(), "0");
        assert_eq!(Int::from_str("123")?.to_string(), "123");
        assert_eq!(Int::from_str("-123")?.to_string(), "-123");
        Int::from_str("123456789123456789123456789123456789123456789").unwrap_err();
        Ok(())
    }

    #[test]
    fn from_float() -> Result<(), ParseIntError> {
        assert_eq!(
            Int::try_from(Float::from(0.)).ok(),
            Some(Int::from_str("0")?)
        );
        assert_eq!(
            Int::try_from(Float::from(-0.)).ok(),
            Some(Int::from_str("0")?)
        );
        assert_eq!(
            Int::try_from(Float::from(-123.1)).ok(),
            Some(Int::from_str("-123")?)
        );
        Int::try_from(Float::from(f32::NAN)).unwrap_err();
        Int::try_from(Float::from(f32::INFINITY)).unwrap_err();
        Int::try_from(Float::from(f32::NEG_INFINITY)).unwrap_err();
        Int::try_from(Float::from(f32::MIN)).unwrap_err();
        Int::try_from(Float::from(f32::MAX)).unwrap_err();
        assert!(
            Int::try_from(Float::from(1_672_000.))
                .unwrap()
                .checked_sub(Int::from_str("1672000")?)
                .unwrap()
                .checked_abs()
                .unwrap()
                < Int::from(1_000_000)
        );
        Ok(())
    }

    #[test]
    fn from_double() -> Result<(), ParseIntError> {
        assert_eq!(
            Int::try_from(Double::from(0.0)).ok(),
            Some(Int::from_str("0")?)
        );
        assert_eq!(
            Int::try_from(Double::from(-0.0)).ok(),
            Some(Int::from_str("0")?)
        );
        assert_eq!(
            Int::try_from(Double::from(-123.1)).ok(),
            Some(Int::from_str("-123")?)
        );
        assert!(
            Int::try_from(Double::from(1_672_000.))
                .unwrap()
                .checked_sub(Int::from_str("1672000").unwrap())
                .unwrap()
                .checked_abs()
                .unwrap()
                < Int::from(10)
        );
        Int::try_from(Double::from(f64::NAN)).unwrap_err();
        Int::try_from(Double::from(f64::INFINITY)).unwrap_err();
        Int::try_from(Double::from(f64::NEG_INFINITY)).unwrap_err();
        Int::try_from(Double::from(f64::MIN)).unwrap_err();
        Int::try_from(Double::from(f64::MAX)).unwrap_err();
        Ok(())
    }

    #[test]
    fn from_decimal() -> Result<(), ParseIntError> {
        assert_eq!(
            Int::try_from(Decimal::from(0)).ok(),
            Some(Int::from_str("0")?)
        );
        assert_eq!(
            Int::try_from(Decimal::from_str("-123.1").unwrap()).ok(),
            Some(Int::from_str("-123")?)
        );
        Int::try_from(Decimal::MIN).unwrap_err();
        Int::try_from(Decimal::MAX).unwrap_err();
        Ok(())
    }

    #[test]
    fn add() {
        assert_eq!(Int::MIN.checked_add(1), Ok(Int::from(i32::MIN + 1)));
        assert_eq!(Int::MAX.checked_add(1), ThinError::expected());
    }

    #[test]
    fn sub() {
        assert_eq!(Int::MIN.checked_sub(1), ThinError::expected());
        assert_eq!(Int::MAX.checked_sub(1), Ok(Int::from(i32::MAX - 1)));
    }

    #[test]
    fn mul() {
        assert_eq!(Int::MIN.checked_mul(2), ThinError::expected());
        assert_eq!(Int::MAX.checked_mul(2), ThinError::expected());
    }

    #[test]
    fn div() {
        assert_eq!(Int::from(1).checked_div(0), ThinError::expected());
    }

    #[test]
    fn rem() {
        assert_eq!(Int::from(10).checked_rem(3), Ok(Int::from(1)));
        assert_eq!(Int::from(6).checked_rem(-2), Ok(Int::from(0)));
        assert_eq!(Int::from(1).checked_rem(0), ThinError::expected());
    }
}
