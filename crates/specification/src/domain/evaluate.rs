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

    /// The items of the collection that is the member `name`: objects, or
    /// values - the identifiers of the groups a policy targets.
    fn collection(&self, name: &str) -> Result<Vec<Element<'_, V>>, ContextError>;
}

/// An item of a collection: an object, which has members, or a value, which
/// is one - as [`Context::field`] gives it, by value: a domain's element is
/// often a type of its own that no `Context` can be implemented for.
pub enum Element<'a, V> {
    /// An object, whose members a path names: `Path::item("price")`.
    Object(&'a dyn Context<V>),
    /// A value, which the tree names as a whole: [`Expr::Item`].
    Value(V),
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
/// collection's; the collection by its name, for an error to say.
struct Frame<'a, V> {
    element: &'a Element<'a, V>,
    collection: &'a str,
    outer: Option<&'a Frame<'a, V>>,
}

impl<V> Clone for Scope<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for Scope<'_, V> {}

/// The value of `expr` in `scope`. A dispatch and nothing else: the frame
/// of this function is what the height of a tree costs in stack at every
/// level of it, so each case is a function of its own, not inlined, whose
/// locals are on the stack only where that case is - in a build without
/// optimisation the frame of a `match` holds the locals of every arm at once.
fn eval<V: Operand + Clone>(expr: &Expr<V>, scope: Scope<'_, V>) -> Result<V, EvalError> {
    match expr {
        Expr::Value(value) => Ok(value.clone()),
        Expr::Field(path) => member(path, scope),
        Expr::Item(up) => itself(*up, scope),
        Expr::Prefix(op, operand) => prefix(*op, operand, scope),
        Expr::Postfix(operand, op) => postfix(operand, *op, scope),
        Expr::Infix(left, Infix::Logical(op), right) => logical(left, *op, right, scope),
        Expr::Infix(left, op, right) => infix(left, *op, right, scope),
        Expr::Any(source, predicate) => some(source, predicate, scope),
    }
}

#[inline(never)]
fn member<V: Operand + Clone>(path: &Path, scope: Scope<'_, V>) -> Result<V, EvalError> {
    Ok(owner(path, scope)?.field(path.name())?)
}

/// The item `up` collections out itself: a value, or no value to give.
#[inline(never)]
fn itself<V: Operand + Clone>(up: usize, scope: Scope<'_, V>) -> Result<V, EvalError> {
    match frame(up, scope)? {
        Frame {
            element: Element::Value(value),
            ..
        } => Ok(value.clone()),
        Frame { collection, .. } => Err(ContextError::NotAValue(format!("{collection}[*]")).into()),
    }
}

#[inline(never)]
fn prefix<V: Operand + Clone>(
    op: Prefix,
    operand: &Expr<V>,
    scope: Scope<'_, V>,
) -> Result<V, EvalError> {
    let operand = eval(operand, scope)?;
    match op {
        Prefix::Not => Ok(from_truth(truth(&operand)?.map(|value| !value))),
        Prefix::Neg if operand.is_null() => Ok(V::null()),
        Prefix::Neg => operand.negate().map_err(EvalError::Operand),
    }
}

#[inline(never)]
fn postfix<V: Operand + Clone>(
    operand: &Expr<V>,
    op: Postfix,
    scope: Scope<'_, V>,
) -> Result<V, EvalError> {
    let null = is_null(operand, scope)?;
    Ok(V::from_bool(match op {
        Postfix::IsNull => null,
        Postfix::IsNotNull => !null,
    }))
}

#[inline(never)]
fn logical<V: Operand + Clone>(
    left: &Expr<V>,
    op: Logical,
    right: &Expr<V>,
    scope: Scope<'_, V>,
) -> Result<V, EvalError> {
    connective(op, truth(&eval(left, scope)?)?, || {
        truth(&eval(right, scope)?)
    })
    .map(from_truth)
}

/// An operator of values, `IS` and the comparisons among them, applied to
/// both operands.
#[inline(never)]
fn infix<V: Operand + Clone>(
    left_expr: &Expr<V>,
    op: Infix,
    right_expr: &Expr<V>,
    scope: Scope<'_, V>,
) -> Result<V, EvalError> {
    let (left, right) = (eval(left_expr, scope)?, eval(right_expr, scope)?);
    match op {
        Infix::Is => {
            let (left, right) = read_constants(left_expr, right_expr, left, right)
                .map_err(|error| named(error, op))?;
            if left.is_null() || right.is_null() {
                Ok(V::from_bool(left.is_null() && right.is_null()))
            } else {
                left.equals(&right)
                    .map(V::from_bool)
                    .map_err(|error| named(error, op))
            }
        }
        Infix::Comparison(comparison) => {
            let (left, right) = read_constants(left_expr, right_expr, left, right)
                .map_err(|error| named(error, op))?;
            strict(left, right, |left, right| {
                compare(left, comparison, right).map(V::from_bool)
            })
            .map_err(|error| named(error, op))
        }
        Infix::Arithmetic(arithmetic) => {
            strict(left, right, |left, right| left.compute(arithmetic, right))
                .map_err(|error| named(error, op))
        }
        Infix::Logical(logical) => self::logical(left_expr, logical, right_expr, scope),
    }
}

/// Whether some item of the collection at `source` satisfies `predicate`.
#[inline(never)]
fn some<V: Operand + Clone>(
    source: &Path,
    predicate: &Expr<V>,
    scope: Scope<'_, V>,
) -> Result<V, EvalError> {
    let witness = |element: &Element<'_, V>| {
        let frame = Frame {
            element,
            collection: source.name(),
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
        .iter()
        .map(witness)
        .find(|outcome| !matches!(outcome, Ok(false)))
        .unwrap_or(Ok(false))
        .map(V::from_bool)
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

/// The operands of a comparison, a string constant among them read as the
/// kind of the other: a constant's, not a member's, since the server refuses
/// two columns of those kinds; under a comparison, not `+`, under which the
/// server reads the string as an interval — another reading. ADR-0015.
fn read_constants<V: Operand>(
    left_expr: &Expr<V>,
    right_expr: &Expr<V>,
    left: V,
    right: V,
) -> Result<(V, V), OperandError> {
    if matches!(left_expr, Expr::Value(_)) {
        if let Some(read) = left.read_beside(&right)? {
            return Ok((read, right));
        }
    }
    if matches!(right_expr, Expr::Value(_)) {
        if let Some(read) = right.read_beside(&left)? {
            return Ok((left, read));
        }
    }
    Ok((left, right))
}

fn owner<'a, V>(path: &Path, scope: Scope<'a, V>) -> Result<&'a dyn Context<V>, EvalError> {
    let root = match path.root() {
        Root::Global => scope.global,
        Root::Item(up) => match frame(up, scope)? {
            Frame {
                element: Element::Object(context),
                ..
            } => *context,
            Frame { collection, .. } => {
                return Err(ContextError::NotAnObject(format!("{collection}[*]")).into());
            }
        },
    };
    path.objects()
        .iter()
        .try_fold(root, |context, name| context.object(name))
        .map_err(EvalError::Context)
}

/// The item `up` collections out from the item under test.
fn frame<'a, V>(up: usize, scope: Scope<'a, V>) -> Result<&'a Frame<'a, V>, EvalError> {
    let mut frame = scope.item;
    for _ in 0..up {
        frame = frame.and_then(|frame| frame.outer);
    }
    frame.ok_or(EvalError::NoCurrentItem)
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
