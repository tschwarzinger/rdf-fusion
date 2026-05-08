use crate::{
    DateTimeOverflowError, OppositeSignInDurationComponentsError, ParseDateTimeError,
    ParseDecimalError, TooLargeForDecimalError, TooLargeForIntError,
    TooLargeForIntegerError,
};
use datafusion::common::DataFusionError;
use oxiri::IriParseError;
use oxrdf::BlankNodeIdParseError;
use std::error::Error;
use std::fmt::Debug;
use std::io;
use std::num::{ParseFloatError, ParseIntError, TryFromIntError};
use std::str::ParseBoolError;
use std::string::FromUtf8Error;
use thiserror::Error;

/// A light-weight result, mainly used for SPARQL operations.
pub type ThinResult<T> = Result<T, ThinError>;

/// A thin error type that indicates an *expected* failure without any reason.
///
/// In SPARQL, many operations can fail. For example, because the input value had a different data
/// type. However, these errors are expected and are part of the query evaluation. As all of these
/// "expected" errors are treated equally in the query evaluation, we do not need to store a reason.
#[derive(Clone, Copy, Debug, Error, PartialEq, Eq)]
pub enum ThinError {
    #[error("An expected error occurred.")]
    ExpectedError,
}

impl ThinError {
    /// Creates a result with a [ThinError].
    pub fn expected<T>() -> ThinResult<T> {
        Err(ThinError::ExpectedError)
    }
}

macro_rules! implement_from {
    ($t:ty) => {
        impl From<$t> for ThinError {
            fn from(_: $t) -> Self {
                ThinError::ExpectedError
            }
        }
    };
}

implement_from!(TooLargeForDecimalError);
implement_from!(TooLargeForIntegerError);
implement_from!(TooLargeForIntError);
implement_from!(ParseBoolError);
implement_from!(ParseIntError);
implement_from!(ParseFloatError);
implement_from!(ParseDecimalError);
implement_from!(ParseDateTimeError);
implement_from!(BlankNodeIdParseError);
implement_from!(IriParseError);
implement_from!(TryFromIntError);
implement_from!(DateTimeOverflowError);
implement_from!(OppositeSignInDurationComponentsError);
implement_from!(FromUtf8Error);

/// An error related to storage operations (reads, writes...).
///
/// TODO improve this
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum StorageError {
    /// Error from the OS I/O layer.
    #[error(transparent)]
    Io(#[from] io::Error),
    /// Error related to data corruption.
    #[error(transparent)]
    Corruption(#[from] CorruptionError),
    #[error("{0}")]
    Other(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl From<StorageError> for io::Error {
    #[inline]
    fn from(error: StorageError) -> Self {
        match error {
            StorageError::Io(error) => error,
            StorageError::Corruption(error) => error.into(),
            StorageError::Other(error) => Self::other(error),
        }
    }
}

// TODO: Improve when implementing proper error handling
impl From<DataFusionError> for StorageError {
    #[inline]
    fn from(error: DataFusionError) -> Self {
        Self::Other(Box::new(error))
    }
}

/// An error return if some content in the database is corrupted.
#[derive(Debug, thiserror::Error)]
#[error(transparent)]
pub struct CorruptionError(#[from] CorruptionErrorKind);

/// An error return if some content in the database is corrupted.
#[derive(Debug, thiserror::Error)]
enum CorruptionErrorKind {
    #[error("{0}")]
    Msg(String),
    #[error("{0}")]
    Other(#[source] Box<dyn Error + Send + Sync + 'static>),
}

impl CorruptionError {
    /// Builds an error from a printable error message.
    #[inline]
    pub fn new(error: impl Into<Box<dyn Error + Send + Sync + 'static>>) -> Self {
        Self(CorruptionErrorKind::Other(error.into()))
    }

    /// Builds an error from a printable error message.
    #[inline]
    pub fn msg(msg: impl Into<String>) -> Self {
        Self(CorruptionErrorKind::Msg(msg.into()))
    }
}

impl From<CorruptionError> for io::Error {
    #[inline]
    fn from(error: CorruptionError) -> Self {
        Self::new(io::ErrorKind::InvalidData, error)
    }
}
