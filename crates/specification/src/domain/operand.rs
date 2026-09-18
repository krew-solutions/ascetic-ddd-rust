//! What the evaluator needs of a value.
//!
//! The Go source keeps a registry of operator implementations keyed by the
//! dynamic types of the operands, with a fallback to interfaces a Value
//! Object may implement (`EqualOperand`, `LessThanOperand`, …); the Python
//! source leans on duck typing. Both are how an open set of value types is
//! served when a value is `any`. Here the value type is a parameter of the
//! tree, so the set is open the static way: a type is usable in an evaluated
//! specification if it implements [`Operand`]. [`Value`] does; a domain that
//! wants its Value Objects as constants wraps `Value` and its own types in
//! an enum and implements the trait for that, delegating the scalars.
//!
//! The trait holds only what depends on the value type. Null propagation,
//! three-valued `AND`/`OR`/`NOT`, `IS`, `IS NULL` are the same for every
//! value type, and the evaluator does them once; a method here is never
//! called with a null.
//!
//! [`Value`]: crate::domain::value::Value

use std::cmp::Ordering;
use std::fmt;

use super::operator::Arithmetic;

/// A value a specification can be evaluated over.
pub trait Operand: Sized {
    /// The null.
    fn null() -> Self;

    /// Whether this is the null.
    fn is_null(&self) -> bool;

    /// The truth value `value`.
    fn from_bool(value: bool) -> Self;

    /// The truth value this is, if it is one.
    fn as_bool(&self) -> Option<bool>;

    /// What kind of value this is, for an error to name: `"integer"`.
    fn kind(&self) -> &'static str;

    /// Whether the two are equal. Go's `EqualOperand`.
    fn equals(&self, other: &Self) -> Result<bool, OperandError>;

    /// How the two are ordered. Go's `LessThanOperand` and its kin.
    fn compare(&self, other: &Self) -> Result<Ordering, OperandError>;

    /// `-self`.
    fn negate(&self) -> Result<Self, OperandError>;

    /// `self op other`.
    fn compute(&self, op: Arithmetic, other: &Self) -> Result<Self, OperandError>;
}

/// An operator could not be applied.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OperandError {
    /// The operator is not defined for operands of these kinds.
    Unsupported {
        /// The operator, as [`Display`](fmt::Display) prints it.
        operator: String,
        /// The kind of the left, or only, operand.
        left: &'static str,
        /// The kind of the right operand, if there is one.
        right: Option<&'static str>,
    },
    /// A division, or a remainder, by zero.
    DivisionByZero,
    /// The result does not fit the type.
    OutOfRange,
}

impl OperandError {
    /// `operator` is not defined between a `left` and a `right`.
    pub fn unsupported(
        operator: impl fmt::Display,
        left: &'static str,
        right: &'static str,
    ) -> Self {
        OperandError::Unsupported {
            operator: operator.to_string(),
            left,
            right: Some(right),
        }
    }

    /// `operator` is not defined for an `operand`.
    pub fn unsupported_unary(operator: impl fmt::Display, operand: &'static str) -> Self {
        OperandError::Unsupported {
            operator: operator.to_string(),
            left: operand,
            right: None,
        }
    }
}

impl fmt::Display for OperandError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            OperandError::Unsupported {
                operator,
                left,
                right: Some(right),
            } => write!(
                f,
                "operator \"{operator}\" is not supported for {left} and {right}"
            ),
            OperandError::Unsupported {
                operator,
                left,
                right: None,
            } => write!(f, "operator \"{operator}\" is not supported for {left}"),
            OperandError::DivisionByZero => f.write_str("division by zero"),
            OperandError::OutOfRange => f.write_str("the result is out of range"),
        }
    }
}

impl std::error::Error for OperandError {}
