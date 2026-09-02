//! Errors reported by the saga machinery.
//!
//! Python signals failures with exceptions: [`InvalidOperationError`][py] for an
//! operation that does not fit the current state, and `KeyError` for a missing
//! dictionary key or an unregistered activity type. Rust reports them as values,
//! so all of those cases are collapsed into a single [`SagaError`] enum.
//!
//! [py]: https://github.com/krew-solutions/ascetic-ddd-python

use std::fmt;

/// Boxed error carrying an arbitrary failure raised by an activity.
///
/// The counterpart of a bare `Exception` propagating out of `do_work()`.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Result alias used throughout the crate.
pub type Result<T> = std::result::Result<T, SagaError>;

/// Everything that can go wrong while running a saga.
#[derive(Debug)]
#[non_exhaustive]
pub enum SagaError {
    /// The requested operation is invalid for the current state.
    ///
    /// Counterpart of `InvalidOperationError`: processing a completed routing
    /// slip, or undoing one that has no completed work.
    InvalidOperation(String),

    /// No activity type is registered under this name (or for this type).
    ///
    /// Counterpart of the `KeyError` raised by `ActivityTypeResolver`.
    ActivityTypeNotRegistered(String),

    /// A required argument (or result) key is absent.
    ///
    /// Counterpart of the `KeyError` raised by `arguments["missing"]`.
    MissingKey(String),

    /// A key holds a value of an unexpected type.
    ///
    /// Rust-specific: Python's dynamic typing has no equivalent failure mode.
    UnexpectedType {
        /// The key that was looked up.
        key: String,
        /// The type the caller asked for.
        expected: &'static str,
    },

    /// An activity failed with an arbitrary error.
    ///
    /// Counterpart of an exception propagating out of `do_work()` or
    /// `compensate()`.
    Activity(BoxError),
}

impl SagaError {
    /// Wraps an arbitrary error raised by an activity.
    pub fn activity<E: Into<BoxError>>(error: E) -> Self {
        SagaError::Activity(error.into())
    }

    /// Reports an operation that does not fit the current state.
    pub fn invalid_operation(message: impl Into<String>) -> Self {
        SagaError::InvalidOperation(message.into())
    }

    /// Reports a missing argument or result key.
    pub fn missing_key(key: impl Into<String>) -> Self {
        SagaError::MissingKey(key.into())
    }
}

impl fmt::Display for SagaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SagaError::InvalidOperation(message) => write!(f, "{message}"),
            SagaError::ActivityTypeNotRegistered(name) => {
                write!(f, "activity type not registered: {name}")
            }
            SagaError::MissingKey(key) => write!(f, "missing key: {key}"),
            SagaError::UnexpectedType { key, expected } => {
                write!(f, "key {key} does not hold a value of type {expected}")
            }
            SagaError::Activity(error) => write!(f, "activity failed: {error}"),
        }
    }
}

impl std::error::Error for SagaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SagaError::Activity(error) => Some(&**error),
            _ => None,
        }
    }
}

impl From<BoxError> for SagaError {
    fn from(error: BoxError) -> Self {
        SagaError::Activity(error)
    }
}
