//! From a specification in the domain's terms to the same in the storage's:
//! the sources' `TransformVisitor` and `CompositeExpression`.
//!
//! A member of the domain model need not be a column, nor a domain value a
//! column's value. The case the sources are built around is a composite
//! identity: `something.id = MemberSomethingId(…)` in the domain is three
//! columns compared with three scalars in the table. A [`Mapping`] says what
//! each member and each value becomes — one expression, or a [composite] of
//! them — and [`transform`] rewrites the tree, turning an equality of two
//! composites into the conjunction of the equalities of their parts.
//!
//! The sources make the composite a node of the tree, of a class the
//! visitor does not know: its `accept` raises, and the transformer must
//! catch every composite before anything else visits it. Here a composite is
//! not a tree. It is the other case of what a mapping returns, [`Mapped`],
//! and the result of `transform` is an [`Expr`], which has no such case: a
//! composite left over — under `<`, under `+`, as the whole specification —
//! is an error of `transform`, and nothing downstream can meet one.
//!
//! The value type changes on the way, `Expr<D>` to `Expr<S>`, so a tree
//! that was not transformed does not pass for one that was.
//!
//! [composite]: Mapped::Composite

use std::fmt;

use crate::domain::ast::{self, Expr, Path};
use crate::domain::operator::{Comparison, Infix};

/// What a member or a value of the domain is in the storage.
#[derive(Clone, Debug, PartialEq)]
pub enum Mapped<S> {
    /// One expression: a column, a scalar.
    Scalar(Expr<S>),
    /// Several, compared part by part; a part may be composite itself.
    Composite(Vec<Mapped<S>>),
}

/// What the storage has for the domain's members and values: the sources'
/// `ITransformContext`.
pub trait Mapping<D, S> {
    /// What can go wrong.
    type Error;

    /// What the member at `path` is. The sources pass the names alone; the
    /// path says as well whether they start at the candidate or at the item
    /// of a collection, which a mapping of both must know.
    fn field(&self, path: &Path) -> Result<Mapped<S>, Self::Error>;

    /// What `value` is.
    fn value(&self, value: &D) -> Result<Mapped<S>, Self::Error>;

    /// Where the collection at `path` is. The same place, unless said
    /// otherwise.
    fn collection(&self, path: &Path) -> Result<Path, Self::Error> {
        Ok(path.clone())
    }
}

/// A specification could not be transformed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransformError<E> {
    /// The mapping failed.
    Mapping(E),
    /// Two composites compared are not of one shape: the sources'
    /// `CompositeExpressionsDifferentLengthError`.
    ShapeMismatch,
    /// A composite compared with one expression.
    NotComposite,
    /// A composite without parts.
    EmptyComposite,
    /// A composite under this operator. Only `=` and `!=` take composites.
    UnsupportedOperator(Infix),
    /// A composite where one expression is needed: under a prefix or
    /// postfix operator, as a predicate, as the whole specification.
    UnexpectedComposite,
}

impl<E: fmt::Display> fmt::Display for TransformError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransformError::Mapping(error) => error.fmt(f),
            TransformError::ShapeMismatch => {
                f.write_str("composite expressions have different length")
            }
            TransformError::NotComposite => f.write_str("not enough composite expressions"),
            TransformError::EmptyComposite => f.write_str("a composite expression has no parts"),
            TransformError::UnsupportedOperator(op) => write!(
                f,
                "operator \"{op}\" is not supported for composite expressions"
            ),
            TransformError::UnexpectedComposite => {
                f.write_str("a composite expression where a single one is needed")
            }
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for TransformError<E> {}

/// `expr` in the terms of `mapping`.
pub fn transform<D, S, M: Mapping<D, S>>(
    expr: &Expr<D>,
    mapping: &M,
) -> Result<Expr<S>, TransformError<M::Error>> {
    scalar(lower(expr, mapping)?)
}

fn lower<D, S, M: Mapping<D, S>>(
    expr: &Expr<D>,
    mapping: &M,
) -> Result<Mapped<S>, TransformError<M::Error>> {
    match expr {
        Expr::Value(value) => mapping.value(value).map_err(TransformError::Mapping),
        Expr::Field(path) => mapping.field(path).map_err(TransformError::Mapping),
        Expr::Prefix(op, operand) => Ok(Mapped::Scalar(Expr::Prefix(
            *op,
            Box::new(transform(operand, mapping)?),
        ))),
        Expr::Postfix(operand, op) => Ok(Mapped::Scalar(Expr::Postfix(
            Box::new(transform(operand, mapping)?),
            *op,
        ))),
        Expr::Infix(left, op, right) => {
            match (lower(left, mapping)?, lower(right, mapping)?) {
                (Mapped::Scalar(left), Mapped::Scalar(right)) => {
                    Ok(Mapped::Scalar(ast::infix(left, *op, right)))
                }
                (Mapped::Composite(left), Mapped::Composite(right)) => match op {
                    Infix::Comparison(Comparison::Eq) => equal(left, right).map(Mapped::Scalar),
                    // Unequal is "not equal in every part", which is not
                    // "unequal in every part": (1, 2) and (1, 3) differ.
                    Infix::Comparison(Comparison::Ne) => {
                        equal(left, right).map(ast::not).map(Mapped::Scalar)
                    }
                    _ => Err(TransformError::UnsupportedOperator(*op)),
                },
                (Mapped::Composite(_), Mapped::Scalar(_))
                | (Mapped::Scalar(_), Mapped::Composite(_)) => Err(TransformError::NotComposite),
            }
        }
        Expr::Any(source, predicate) => Ok(Mapped::Scalar(ast::any(
            mapping
                .collection(source)
                .map_err(TransformError::Mapping)?,
            transform(predicate, mapping)?,
        ))),
    }
}

fn scalar<S, E>(mapped: Mapped<S>) -> Result<Expr<S>, TransformError<E>> {
    match mapped {
        Mapped::Scalar(expr) => Ok(expr),
        Mapped::Composite(_) => Err(TransformError::UnexpectedComposite),
    }
}

/// `left = right`, part by part: `l1 = r1 AND l2 = r2 AND …`.
fn equal<S, E>(left: Vec<Mapped<S>>, right: Vec<Mapped<S>>) -> Result<Expr<S>, TransformError<E>> {
    if left.len() != right.len() {
        return Err(TransformError::ShapeMismatch);
    }
    let mut parts = left.into_iter().zip(right).map(|pair| match pair {
        (Mapped::Scalar(left), Mapped::Scalar(right)) => Ok(ast::equal(left, right)),
        (Mapped::Composite(left), Mapped::Composite(right)) => equal(left, right),
        (Mapped::Composite(_), Mapped::Scalar(_)) | (Mapped::Scalar(_), Mapped::Composite(_)) => {
            Err(TransformError::ShapeMismatch)
        }
    });
    let first = parts.next().ok_or(TransformError::EmptyComposite)??;
    parts.try_fold(first, |all, part| Ok(ast::and(all, part?)))
}
