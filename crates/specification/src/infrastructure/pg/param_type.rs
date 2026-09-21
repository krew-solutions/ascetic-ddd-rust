//! The type PostgreSQL is told a constant has, where nothing else tells it.
//!
//! A constant is a numbered parameter, and the server finds its type from
//! what stands beside it: `"age" >= $1` makes `$1` whatever `age` is, and
//! the driver writes the value as that. Where every operand of an operator
//! is a constant there is nothing beside it: `$1 + $2` is "operator is not
//! unique: unknown + unknown", `-$1` likewise, and of `$1 IS NULL` the server
//! "could not determine data type". So it is to Rust's and Go's drivers,
//! which ask the server; Python's sends a type with each value, an integer's
//! by its size, and the server computed `1 << 63` in sixteen bits: 0.
//!
//! So there, and only there, the text says the type: `$1::bigint + $2::bigint`.
//! Not everywhere. A value adapts to the column it meets — an integer is
//! written as `int2`, `int4`, `int8` or a float, a point in time as a
//! timestamp with or without zone — and a type said beside a column takes
//! that away: `"at" = $1::timestamptz` of a column without zone is compared
//! in the session's time zone, and selects other rows than `"at" = $1` does.

use crate::domain::ast::Expr;
use crate::domain::operator::{Arithmetic, Infix, Prefix};
use crate::domain::value::Value;

/// What PostgreSQL calls the type of a value, by its kind.
pub trait ParamType {
    /// The name of the type, or none for a value of no kind: the null.
    fn param_type(&self) -> Option<&'static str>;
}

impl ParamType for Value {
    fn param_type(&self) -> Option<&'static str> {
        match self {
            Value::Null => None,
            Value::Bool(_) => Some("boolean"),
            Value::Int(_) => Some("bigint"),
            Value::Float(_) => Some("double precision"),
            Value::Text(_) => Some("text"),
            Value::Timestamp(_) => Some("timestamptz"),
            Value::Interval(_) => Some("interval"),
        }
    }
}

/// The type to say of the operand of a prefix operator, if it is a constant:
/// it has nothing beside it to take a type from. A null has no kind, and
/// takes what the operator is of: a number under `-`; under `NOT` the server
/// finds `boolean` by itself.
pub(super) fn under_prefix<V: ParamType>(op: Prefix, operand: &Expr<V>) -> Option<&'static str> {
    let Expr::Value(value) = operand else {
        return None;
    };
    value.param_type().or(match op {
        Prefix::Neg => Some("bigint"),
        Prefix::Not => None,
    })
}

/// The type to say of the operand of a null test, if it is a constant. Of
/// what type a null is tested does not matter, and the server must be told
/// one: "could not determine data type of parameter".
pub(super) fn under_postfix<V: ParamType>(operand: &Expr<V>) -> Option<&'static str> {
    match operand {
        Expr::Value(value) => value.param_type().or(Some("text")),
        _ => None,
    }
}

/// The types to say of the operands of `op`, if both are constants: neither
/// has anything beside it to take a type from. PostgreSQL shifts a `bigint`
/// by an `integer`, so the count of a shift is that. Two nulls take what the
/// operator is of: numbers under arithmetic; compared, the server takes them
/// for texts by itself.
pub(super) fn of_both<V: ParamType>(
    left: &Expr<V>,
    op: Infix,
    right: &Expr<V>,
) -> (Option<&'static str>, Option<&'static str>) {
    let (Expr::Value(left), Expr::Value(right)) = (left, right) else {
        return (None, None);
    };
    let shift = matches!(op, Infix::Arithmetic(Arithmetic::Shl | Arithmetic::Shr));
    let count = |said: Option<&'static str>| match said {
        Some("bigint") if shift => Some("integer"),
        other => other,
    };
    match (left.param_type(), right.param_type(), op) {
        (None, None, Infix::Arithmetic(_)) => (Some("bigint"), count(Some("bigint"))),
        (left, right, _) => (left, count(right)),
    }
}
