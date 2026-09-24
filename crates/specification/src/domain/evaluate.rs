//! The reader that decides, in memory, whether a candidate satisfies a
//! specification: the sources' `EvaluateVisitor`.
//!
//! The logic is PostgreSQL's, so that this reader and the database reading
//! the compiled SQL agree on every candidate, nulls included:
//!
//! * a comparison or a computation with a null operand is null;
//! * `AND`, `OR`, `NOT` are three-valued: `NULL AND FALSE` is false,
//!   `NULL OR TRUE` is true, `NOT NULL` is null;
//! * `IS`, `IS NULL`, `IS NOT NULL` and `any` are true or false, never null;
//! * a candidate satisfies a specification whose value is true — a null
//!   does not, as a row with a null `WHERE` is not selected.
//!
//! `AND` and `OR` do not evaluate their right side once the left has decided
//! the result, and `any` stops at its first witness, as the same operators do
//! in Rust, Python and Go. The sources evaluate everything; the difference
//! shows only where the part not evaluated would have failed.

use std::cmp::Ordering;
use std::fmt;

use super::ast::{Expr, Path, Root};
use super::operand::{Operand, OperandError};
use super::operator::{Comparison, Infix, Logical, Postfix, Prefix};

/// What a specification is evaluated against: the candidate, an object
/// nested in it, an item of one of its collections.
///
/// The sources have one method, `get(key)`, and look at what came back —
/// a value, a context, a list of contexts. The tree already knows which of
/// the three it is after, so here it asks for that, and the answer is typed.
pub trait Context<V> {
    /// The value of the member `name`.
    fn field(&self, name: &str) -> Result<V, ContextError>;

    /// The object that is the member `name`.
    fn object(&self, name: &str) -> Result<&dyn Context<V>, ContextError>;

    /// The items of the collection that is the member `name`.
    fn collection(&self, name: &str) -> Result<Vec<&dyn Context<V>>, ContextError>;
}

/// A context has no such member, or has it as something else.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ContextError {
    /// No member of this name.
    Missing(String),
    /// The member is not a value.
    NotAValue(String),
    /// The member is not an object.
    NotAnObject(String),
    /// The member is not a collection.
    NotACollection(String),
}

impl fmt::Display for ContextError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ContextError::Missing(name) => write!(f, "key '{name}' not found"),
            ContextError::NotAValue(name) => write!(f, "'{name}' is not a value"),
            ContextError::NotAnObject(name) => write!(f, "'{name}' is not an object"),
            ContextError::NotACollection(name) => write!(f, "'{name}' is not a collection"),
        }
    }
}

impl std::error::Error for ContextError {}

/// A specification could not be evaluated.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EvalError {
    /// The candidate has no such member.
    Context(ContextError),
    /// A path from the item under test, outside any collection.
    NoCurrentItem,
    /// A truth value was needed — by `NOT`, `AND`, `OR`, as the predicate of
    /// `any`, as the result — and a value of this kind was found.
    NotBoolean(&'static str),
    /// An operator could not be applied to its operands.
    Operand(OperandError),
}

impl fmt::Display for EvalError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EvalError::Context(error) => error.fmt(f),
            EvalError::NoCurrentItem => f.write_str("no current item in context"),
            EvalError::NotBoolean(kind) => write!(f, "a boolean was expected, got {kind}"),
            EvalError::Operand(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for EvalError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            EvalError::Context(error) => Some(error),
            EvalError::Operand(error) => Some(error),
            EvalError::NoCurrentItem | EvalError::NotBoolean(_) => None,
        }
    }
}

impl From<ContextError> for EvalError {
    fn from(error: ContextError) -> Self {
        EvalError::Context(error)
    }
}

/// Whether `candidate` satisfies `specification`: whether its value is
/// true. False and null both say no.
pub fn is_satisfied_by<V: Operand + Clone>(
    specification: &Expr<V>,
    candidate: &dyn Context<V>,
) -> Result<bool, EvalError> {
    evaluate(specification, candidate).and_then(|value| Ok(truth(&value)? == Some(true)))
}

/// The value of `expr` for `candidate`.
pub fn evaluate<V: Operand + Clone>(
    expr: &Expr<V>,
    candidate: &dyn Context<V>,
) -> Result<V, EvalError> {
    eval(
        expr,
        Scope {
            global: candidate,
            item: None,
        },
    )
}

/// What the roots of a path stand for at this point of the tree. Entering a
/// collection makes a new scope rather than changing this one: the sources'
/// `_with_item`.
struct Scope<'a, V> {
    global: &'a dyn Context<V>,
    /// The item under test, and behind it the items of the enclosing
    /// collections: `Root::Item(up)` is the one `up` steps out.
    item: Option<&'a Frame<'a, V>>,
}

/// The item of one collection's predicate, inside the item of the enclosing
/// collection's.
struct Frame<'a, V> {
    context: &'a dyn Context<V>,
    outer: Option<&'a Frame<'a, V>>,
}

impl<V> Clone for Scope<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for Scope<'_, V> {}

fn eval<V: Operand + Clone>(expr: &Expr<V>, scope: Scope<'_, V>) -> Result<V, EvalError> {
    match expr {
        Expr::Value(value) => Ok(value.clone()),
        Expr::Field(path) => Ok(owner(path, scope)?.field(path.name())?),
        Expr::Prefix(Prefix::Not, operand) => Ok(from_truth(
            truth(&eval(operand, scope)?)?.map(|value| !value),
        )),
        Expr::Prefix(Prefix::Neg, operand) => {
            let operand = eval(operand, scope)?;
            if operand.is_null() {
                Ok(V::null())
            } else {
                operand.negate().map_err(EvalError::Operand)
            }
        }
        Expr::Postfix(operand, Postfix::IsNull) => Ok(V::from_bool(is_null(operand, scope)?)),
        Expr::Postfix(operand, Postfix::IsNotNull) => Ok(V::from_bool(!is_null(operand, scope)?)),
        Expr::Infix(left, Infix::Logical(op), right) => {
            connective(*op, truth(&eval(left, scope)?)?, || {
                truth(&eval(right, scope)?)
            })
            .map(from_truth)
        }
        Expr::Infix(left, Infix::Is, right) => {
            let (left, right) = (eval(left, scope)?, eval(right, scope)?);
            if left.is_null() || right.is_null() {
                Ok(V::from_bool(left.is_null() && right.is_null()))
            } else {
                left.equals(&right)
                    .map(V::from_bool)
                    .map_err(|error| named(error, Infix::Is))
            }
        }
        Expr::Infix(left, Infix::Comparison(op), right) => {
            strict(eval(left, scope)?, eval(right, scope)?, |left, right| {
                compare(left, *op, right).map(V::from_bool)
            })
            .map_err(|error| named(error, op))
        }
        Expr::Infix(left, Infix::Arithmetic(op), right) => {
            strict(eval(left, scope)?, eval(right, scope)?, |left, right| {
                left.compute(*op, right)
            })
            .map_err(|error| named(error, op))
        }
        Expr::Any(source, predicate) => {
            let witness = |item| {
                let frame = Frame {
                    context: item,
                    outer: scope.item,
                };
                let scope = Scope {
                    item: Some(&frame),
                    ..scope
                };
                Ok(truth(&eval(predicate, scope)?)? == Some(true))
            };
            owner(source, scope)?
                .collection(source.name())?
                .into_iter()
                .map(witness)
                .find(|outcome| !matches!(outcome, Ok(false)))
                .unwrap_or(Ok(false))
                .map(V::from_bool)
        }
    }
}

/// The context that has the member the path names.
/// Whether `operand` is null. A member is asked about whatever it is: an
/// object that is there is not null, though it is no value — the guard the
/// macro writes for `is_some_and` over a Value Object, `discount IS NOT NULL`,
/// asks that of an object. An object that is not there is a null value in the
/// context, and null. Anything else is evaluated, and is null or is not.
fn is_null<V: Operand + Clone>(operand: &Expr<V>, scope: Scope<'_, V>) -> Result<bool, EvalError> {
    let Expr::Field(path) = operand else {
        return Ok(eval(operand, scope)?.is_null());
    };
    let owner = owner(path, scope)?;
    match owner.field(path.name()) {
        Ok(value) => Ok(value.is_null()),
        Err(ContextError::NotAValue(_)) => owner
            .object(path.name())
            .map(|_| false)
            .map_err(EvalError::Context),
        Err(error) => Err(error.into()),
    }
}

fn owner<'a, V>(path: &Path, scope: Scope<'a, V>) -> Result<&'a dyn Context<V>, EvalError> {
    let root = match path.root() {
        Root::Global => scope.global,
        Root::Item(up) => {
            let mut frame = scope.item;
            for _ in 0..up {
                frame = frame.and_then(|frame| frame.outer);
            }
            frame.ok_or(EvalError::NoCurrentItem)?.context
        }
    };
    path.objects()
        .iter()
        .try_fold(root, |context, name| context.object(name))
        .map_err(EvalError::Context)
}

/// True, false, or — `None` — unknown.
fn truth<V: Operand>(value: &V) -> Result<Option<bool>, EvalError> {
    if value.is_null() {
        Ok(None)
    } else {
        value
            .as_bool()
            .map(Some)
            .ok_or(EvalError::NotBoolean(value.kind()))
    }
}

fn from_truth<V: Operand>(truth: Option<bool>) -> V {
    truth.map_or_else(V::null, V::from_bool)
}

/// Three-valued `AND` and `OR`. `deciding` is the value that settles the
/// result whatever the other side is: false for `AND`, true for `OR`.
fn connective(
    op: Logical,
    left: Option<bool>,
    right: impl FnOnce() -> Result<Option<bool>, EvalError>,
) -> Result<Option<bool>, EvalError> {
    let deciding = match op {
        Logical::And => false,
        Logical::Or => true,
    };
    if left == Some(deciding) {
        return Ok(left);
    }
    Ok(match (left, right()?) {
        (_, Some(right)) if right == deciding => Some(deciding),
        (Some(_), Some(_)) => Some(!deciding),
        _ => None,
    })
}

/// A null operand makes a null result; `f` sees only values.
fn strict<V: Operand>(
    left: V,
    right: V,
    f: impl FnOnce(&V, &V) -> Result<V, OperandError>,
) -> Result<V, OperandError> {
    if left.is_null() || right.is_null() {
        Ok(V::null())
    } else {
        f(&left, &right)
    }
}

fn compare<V: Operand>(left: &V, op: Comparison, right: &V) -> Result<bool, OperandError> {
    match op {
        Comparison::Eq => left.equals(right),
        Comparison::Ne => left.equals(right).map(|equal| !equal),
        Comparison::Gt => left.compare(right).map(Ordering::is_gt),
        Comparison::Lt => left.compare(right).map(Ordering::is_lt),
        Comparison::Ge => left.compare(right).map(Ordering::is_ge),
        Comparison::Le => left.compare(right).map(Ordering::is_le),
    }
}

/// An operand type reports the operator it implements the asked one with —
/// `<` for every ordering; the error names the one the tree has.
fn named(error: OperandError, op: impl fmt::Display) -> EvalError {
    EvalError::Operand(match error {
        OperandError::Unsupported { left, right, .. } => OperandError::Unsupported {
            operator: op.to_string(),
            left,
            right,
        },
        other => other,
    })
}
