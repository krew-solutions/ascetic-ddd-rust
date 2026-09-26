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
//!
//! The one column that is cast is the count of a shift, `"a" << "b"::integer`:
//! PostgreSQL shifts by an `integer` and by nothing else, and a column there,
//! a `bigint` more often than not, is "operator does not exist: bigint <<
//! bigint". A cast of the count takes nothing away — the operator has it an
//! integer already — and turns a column of any integer type into the one the
//! operator has.

use crate::domain::ast::Expr;
use crate::domain::operator::{Arithmetic, Infix, Prefix};
use crate::domain::value::Value;

/// What PostgreSQL calls the type of a value, by its kind.
pub trait ParamType {
    /// The name of the type, or none for a value of no kind: the null.
    fn param_type(&self) -> Option<&'static str>;

    /// Whether the value is a text with a NUL in it: no text PostgreSQL has,
    /// `text` holds none. The compiler refuses such a value where it meets
    /// the server, rather than let the driver or the server fail the query.
    fn nul_in_text(&self) -> bool {
        false
    }
}

impl ParamType for Value {
    fn nul_in_text(&self) -> bool {
        matches!(self, Value::Text(text) if text.contains('\0'))
    }

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

/// Whether `right` is the count of a shift that must be said an integer. A
/// constant there is inferred, or was said an integer already where nothing
/// stands beside it; a column or an expression has a type of its own, which
/// the server will not convert, so it is cast.
pub(super) fn is_a_count_to_cast<V>(op: Infix, right: &Expr<V>) -> bool {
    matches!(op, Infix::Arithmetic(Arithmetic::Shl | Arithmetic::Shr))
        && !matches!(right, Expr::Value(_))
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
