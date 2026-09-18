//! The scalar values every part of the crate speaks: what a JSONPath
//! template's literals and parameters are, what the typed DSL builds with,
//! what `#[specification]` generates, and what goes to PostgreSQL as a
//! query parameter.
//!
//! The operators follow PostgreSQL, because a specification has two readers
//! that must agree — the evaluator here and the database reading the
//! compiled SQL: integer division truncates, a division by zero and an
//! integer overflow are errors rather than wrapped results, an integer
//! meeting a float is promoted, a shift counts modulo 64, `NaN` equals
//! itself and is greater than any other number. Text is the exception the
//! crate cannot remove: here it is ordered by code point, in the database by
//! the column's collation.

use std::cmp::Ordering;

use super::operand::{Operand, OperandError};
use super::operator::{Arithmetic, Comparison, Prefix};

/// A point in time: microseconds since the Unix epoch, PostgreSQL's
/// resolution. A type of the crate's own, so that the core depends on no
/// calendar library; the application converts at its edge.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(i64);

impl Timestamp {
    /// The point `micros` microseconds after the Unix epoch.
    pub const fn from_micros(micros: i64) -> Self {
        Timestamp(micros)
    }

    /// Microseconds since the Unix epoch.
    pub const fn as_micros(self) -> i64 {
        self.0
    }
}

/// A span of time, in microseconds: what two [`Timestamp`]s differ by.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Interval(i64);

impl Interval {
    /// The span of `micros` microseconds.
    pub const fn from_micros(micros: i64) -> Self {
        Interval(micros)
    }

    /// The span in microseconds.
    pub const fn as_micros(self) -> i64 {
        self.0
    }
}

/// A scalar value.
///
/// `==` on `Value` is identity of representation, which is what comparing
/// trees in a test wants: `Int(1) != Float(1.0)`. Equality of the values
/// meant is [`Operand::equals`].
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// The null: a value that is not known.
    Null,
    /// A truth value.
    Bool(bool),
    /// An integer: PostgreSQL's `bigint`.
    Int(i64),
    /// A float: PostgreSQL's `double precision`.
    Float(f64),
    /// A text.
    Text(String),
    /// A point in time.
    Timestamp(Timestamp),
    /// A span of time.
    Interval(Interval),
}

impl Operand for Value {
    fn null() -> Self {
        Value::Null
    }

    fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    fn from_bool(value: bool) -> Self {
        Value::Bool(value)
    }

    fn as_bool(&self) -> Option<bool> {
        match self {
            Value::Bool(value) => Some(*value),
            _ => None,
        }
    }

    fn kind(&self) -> &'static str {
        match self {
            Value::Null => "null",
            Value::Bool(_) => "boolean",
            Value::Int(_) => "integer",
            Value::Float(_) => "float",
            Value::Text(_) => "text",
            Value::Timestamp(_) => "timestamp",
            Value::Interval(_) => "interval",
        }
    }

    fn equals(&self, other: &Self) -> Result<bool, OperandError> {
        self.compare(other).map(Ordering::is_eq)
    }

    fn compare(&self, other: &Self) -> Result<Ordering, OperandError> {
        match (self, other) {
            // False before true: PostgreSQL's order, Rust's and Python's. The
            // Go source compares truth values for equality only.
            (Value::Bool(left), Value::Bool(right)) => Ok(left.cmp(right)),
            (Value::Int(left), Value::Int(right)) => Ok(left.cmp(right)),
            (Value::Float(left), Value::Float(right)) => Ok(float_order(*left, *right)),
            (Value::Int(left), Value::Float(right)) => Ok(float_order(*left as f64, *right)),
            (Value::Float(left), Value::Int(right)) => Ok(float_order(*left, *right as f64)),
            (Value::Text(left), Value::Text(right)) => Ok(left.cmp(right)),
            (Value::Timestamp(left), Value::Timestamp(right)) => Ok(left.cmp(right)),
            (Value::Interval(left), Value::Interval(right)) => Ok(left.cmp(right)),
            _ => Err(OperandError::unsupported(
                Comparison::Lt,
                self.kind(),
                other.kind(),
            )),
        }
    }

    fn negate(&self) -> Result<Self, OperandError> {
        match self {
            Value::Int(value) => value
                .checked_neg()
                .map(Value::Int)
                .ok_or(OperandError::OutOfRange),
            Value::Float(value) => Ok(Value::Float(-value)),
            Value::Interval(Interval(value)) => value
                .checked_neg()
                .map(|micros| Value::Interval(Interval(micros)))
                .ok_or(OperandError::OutOfRange),
            _ => Err(OperandError::unsupported_unary(Prefix::Neg, self.kind())),
        }
    }

    fn compute(&self, op: Arithmetic, other: &Self) -> Result<Self, OperandError> {
        let defined = match (self, other) {
            (Value::Int(left), Value::Int(right)) => {
                Some(integer(op, *left, *right).map(Value::Int))
            }
            (Value::Float(left), Value::Float(right)) => float(op, *left, *right),
            (Value::Int(left), Value::Float(right)) => float(op, *left as f64, *right),
            (Value::Float(left), Value::Int(right)) => float(op, *left, *right as f64),
            _ => temporal(op, self, other),
        };
        defined.unwrap_or_else(|| Err(OperandError::unsupported(op, self.kind(), other.kind())))
    }
}

/// Every operator is defined for two integers.
fn integer(op: Arithmetic, left: i64, right: i64) -> Result<i64, OperandError> {
    let checked = |result: Option<i64>| result.ok_or(OperandError::OutOfRange);
    match op {
        Arithmetic::Add => checked(left.checked_add(right)),
        Arithmetic::Sub => checked(left.checked_sub(right)),
        Arithmetic::Mul => checked(left.checked_mul(right)),
        Arithmetic::Div | Arithmetic::Mod if right == 0 => Err(OperandError::DivisionByZero),
        Arithmetic::Div => checked(left.checked_div(right)),
        // `i64::MIN % -1` overflows in Rust and is 0 in PostgreSQL.
        Arithmetic::Mod => Ok(left.checked_rem(right).unwrap_or(0)),
        // PostgreSQL takes the count modulo 64, a negative count included.
        Arithmetic::Shl => Ok(left.wrapping_shl(right as u32)),
        Arithmetic::Shr => Ok(left.wrapping_shr(right as u32)),
    }
}

/// `None` if the operator is not one of floats.
fn float(op: Arithmetic, left: f64, right: f64) -> Option<Result<Value, OperandError>> {
    let result = match op {
        Arithmetic::Add => left + right,
        Arithmetic::Sub => left - right,
        Arithmetic::Mul => left * right,
        Arithmetic::Div if right == 0.0 => return Some(Err(OperandError::DivisionByZero)),
        Arithmetic::Div => left / right,
        Arithmetic::Mod | Arithmetic::Shl | Arithmetic::Shr => return None,
    };
    Some(
        if result.is_infinite() && left.is_finite() && right.is_finite() {
            Err(OperandError::OutOfRange)
        } else {
            Ok(Value::Float(result))
        },
    )
}

/// `None` if the operator is not one of these two. A point less a point is
/// a span; a point and a span make a point; spans add up.
fn temporal(op: Arithmetic, left: &Value, right: &Value) -> Option<Result<Value, OperandError>> {
    let checked = |result: Option<i64>, wrap: fn(i64) -> Value| {
        Some(result.map(wrap).ok_or(OperandError::OutOfRange))
    };
    let point: fn(i64) -> Value = |micros| Value::Timestamp(Timestamp(micros));
    let span: fn(i64) -> Value = |micros| Value::Interval(Interval(micros));
    match (left, op, right) {
        (Value::Timestamp(l), Arithmetic::Sub, Value::Timestamp(r)) => {
            checked(l.0.checked_sub(r.0), span)
        }
        (Value::Timestamp(l), Arithmetic::Add, Value::Interval(r))
        | (Value::Interval(r), Arithmetic::Add, Value::Timestamp(l)) => {
            checked(l.0.checked_add(r.0), point)
        }
        (Value::Timestamp(l), Arithmetic::Sub, Value::Interval(r)) => {
            checked(l.0.checked_sub(r.0), point)
        }
        (Value::Interval(l), Arithmetic::Add, Value::Interval(r)) => {
            checked(l.0.checked_add(r.0), span)
        }
        (Value::Interval(l), Arithmetic::Sub, Value::Interval(r)) => {
            checked(l.0.checked_sub(r.0), span)
        }
        _ => None,
    }
}

/// PostgreSQL's order of floats: `NaN` equals itself and is greater than
/// everything else; `-0` equals `0`.
fn float_order(left: f64, right: f64) -> Ordering {
    left.partial_cmp(&right)
        .unwrap_or_else(|| left.is_nan().cmp(&right.is_nan()))
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Bool(value)
    }
}

macro_rules! from_integer {
    ($($integer:ty),*) => {$(
        impl From<$integer> for Value {
            fn from(value: $integer) -> Self {
                Value::Int(i64::from(value))
            }
        }
    )*};
}

from_integer!(i8, i16, i32, i64, u8, u16, u32);

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Value::Float(f64::from(value))
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Text(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Text(value)
    }
}

impl From<&String> for Value {
    fn from(value: &String) -> Self {
        Value::Text(value.clone())
    }
}

impl From<Timestamp> for Value {
    fn from(value: Timestamp) -> Self {
        Value::Timestamp(value)
    }
}

impl From<Interval> for Value {
    fn from(value: Interval) -> Self {
        Value::Interval(value)
    }
}

/// `None` is the null.
impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(value: Option<T>) -> Self {
        value.map_or(Value::Null, Into::into)
    }
}
