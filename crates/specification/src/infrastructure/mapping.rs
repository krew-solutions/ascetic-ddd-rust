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

use crate::domain::ast::{self, Expr, Path, Root};
use crate::domain::operator::{Comparison, Infix};

/// What a member or a value of the domain is in the storage.
#[derive(Clone, Debug, PartialEq)]
pub enum Mapped<S> {
    /// One expression: a column, a scalar.
    Scalar(Expr<S>),
    /// Several, compared part by part; a part may be composite itself.
    Composite(Vec<Mapped<S>>),
    /// The storage's null, for a value of the domain that is one there: a
    /// special case that answers for itself in the domain - "no discount",
    /// equal to itself - and is a column's null in the storage. Compared for
    /// equality it is tested for, `IS NULL`: `= $1` with a null is true of
    /// nothing. Anywhere else it is the null it carries.
    ///
    /// A null that was one in the domain already is a [`Scalar`] like any
    /// constant, and stays compared: it is the mapping that knows which of
    /// the two a null is, so it is the mapping that says.
    ///
    /// [`Scalar`]: Mapped::Scalar
    Null(S),
}

/// What the storage has for the domain's members and values: the sources'
/// `ITransformContext`.
///
/// A mapping is of the aggregate's members, and knows nothing of any
/// query: it is asked about a member by its whole path from the candidate,
/// `categories.products.price` for the price of a product of a category,
/// and answers what that is in the storage, `categories.products.price_cents`,
/// as a path from the candidate's row. A collection is a member like any
/// other, `categories.products`. Where a query stands when it asks - inside
/// which collection's predicate, how far out an item is - is the tree's, and
/// [`transform`] puts the answer there: the member of an item is the answer
/// less the collection's, from the item.
///
/// A mapping is a guard as well. A specification may arrive as data - a
/// template bound from a request, a tree from another service - and the
/// mapping names every member such a specification may filter by, and
/// refuses any other: a member the mapping does not know is its error, not
/// a column that happens to exist. So what reaches the database is a query
/// over the columns the repository chose to expose, through the relations
/// it declared in the [`Schema`](crate::pg::Schema), and not whatever a
/// caller composed to read another table or to scan a column without an
/// index. [`transform`] is the one place for that: the compiler writes the
/// names it is given. The size of a tree is bounded by the parsers - its
/// height and its nesting - and its members by the mapping.
pub trait Mapping<D, S> {
    /// What can go wrong.
    type Error;

    /// What the member at `path` is: a path from the candidate.
    fn field(&self, path: &Path) -> Result<Mapped<S>, Self::Error>;

    /// What `value` is.
    fn value(&self, value: &D) -> Result<Mapped<S>, Self::Error>;
}

/// A specification could not be transformed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransformError<E> {
    /// The mapping failed.
    Mapping(E),
    /// Two composites compared are not of one shape: the sources'
    /// `CompositeExpressionsDifferentLengthError`.
    ShapeMismatch,
    /// A path from an item outside any collection's predicate, or from an
    /// item further out than there are collections.
    NoCurrentItem,
    /// The mapping put a member of an item, or a collection of one,
    /// somewhere else than in its collection: `items.price` answered with a
    /// path that does not start with what `items` was answered with.
    OutsideItsCollection,
    /// The mapping answered for a collection with something that is not a
    /// place: a value, a composite, a null.
    CollectionNotAPlace,
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
            TransformError::NoCurrentItem => f.write_str("no current item in context"),
            TransformError::OutsideItsCollection => f.write_str(
                "the mapping put a member of an item outside its collection: the storage's \
                 path of a member of an item starts with the collection's",
            ),
            TransformError::CollectionNotAPlace => {
                f.write_str("the mapping answered for a collection with what is not a place")
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
    scalar(lower(expr, mapping, &[])?)
}

/// A collection whose predicate is being transformed: its whole path from
/// the candidate, in the domain's names and in the storage's.
struct Collection {
    domain: Vec<String>,
    storage: Vec<String>,
}

/// The collections the expression is inside of, the nearest last: the item
/// `up` collections out is of `collections[len - 1 - up]`.
type Inside<'a> = [&'a Collection];

/// The path `names` from the candidate.
fn from_candidate<'a>(names: impl IntoIterator<Item = &'a str>) -> Option<Path> {
    let mut names = names.into_iter();
    let first = names.next()?;
    Some(names.fold(Path::global(first), Path::child))
}

/// The whole path of `path` from the candidate: from an item, the path of
/// the item's collection and then the names.
fn whole<E>(path: &Path, inside: &Inside<'_>) -> Result<Path, TransformError<E>> {
    let collection = match path.root() {
        Root::Global => None,
        Root::Item(up) => Some(
            inside
                .len()
                .checked_sub(up + 1)
                .map(|at| inside[at])
                .ok_or(TransformError::NoCurrentItem)?,
        ),
    };
    let names = collection
        .into_iter()
        .flat_map(|collection| collection.domain.iter().map(String::as_str))
        .chain(path.names());
    from_candidate(names).ok_or(TransformError::NoCurrentItem)
}

/// The storage's path of a member, put where the member was: of an item,
/// the mapping's answer less the collection's, from the item.
fn placed<S, E>(
    mapped: Mapped<S>,
    root: Root,
    inside: &Inside<'_>,
) -> Result<Mapped<S>, TransformError<E>> {
    let Root::Item(up) = root else {
        return Ok(mapped);
    };
    let collection = inside[inside.len() - 1 - up];
    Ok(match mapped {
        Mapped::Scalar(expr) => Mapped::Scalar(placed_expr(expr, &collection.storage, up)?),
        Mapped::Composite(parts) => Mapped::Composite(
            parts
                .into_iter()
                .map(|part| placed(part, root, inside))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        null @ Mapped::Null(_) => null,
    })
}

/// Every path from the candidate in `expr` starts with `prefix`, the
/// collection's, and is made a path from the item `up` collections out.
fn placed_expr<S, E>(
    expr: Expr<S>,
    prefix: &[String],
    up: usize,
) -> Result<Expr<S>, TransformError<E>> {
    let place = |expr: Box<Expr<S>>| placed_expr(*expr, prefix, up).map(Box::new);
    Ok(match expr {
        Expr::Field(path) if path.root() == Root::Global => {
            let names: Vec<&str> = path.names().collect();
            let rest = names
                .strip_prefix(
                    prefix
                        .iter()
                        .map(String::as_str)
                        .collect::<Vec<_>>()
                        .as_slice(),
                )
                .ok_or(TransformError::OutsideItsCollection)?;
            let mut rest = rest.iter().copied();
            let first = rest.next().ok_or(TransformError::OutsideItsCollection)?;
            Expr::Field(rest.fold(Path::outer(up, first), Path::child))
        }
        Expr::Field(_) | Expr::Value(_) => expr,
        Expr::Prefix(op, operand) => Expr::Prefix(op, place(operand)?),
        Expr::Postfix(operand, op) => Expr::Postfix(place(operand)?, op),
        Expr::Infix(left, op, right) => Expr::Infix(place(left)?, op, place(right)?),
        Expr::Any(source, predicate) => Expr::Any(source, place(predicate)?),
    })
}

fn lower<D, S, M: Mapping<D, S>>(
    expr: &Expr<D>,
    mapping: &M,
    inside: &Inside<'_>,
) -> Result<Mapped<S>, TransformError<M::Error>> {
    match expr {
        Expr::Value(value) => mapping.value(value).map_err(TransformError::Mapping),
        Expr::Field(path) => member(path, mapping, inside),
        Expr::Prefix(op, operand) => Ok(Mapped::Scalar(Expr::Prefix(
            *op,
            Box::new(scalar(lower(operand, mapping, inside)?)?),
        ))),
        Expr::Postfix(operand, op) => Ok(Mapped::Scalar(Expr::Postfix(
            Box::new(scalar(lower(operand, mapping, inside)?)?),
            *op,
        ))),
        Expr::Infix(left, op, right) => {
            let (left, right) = (
                lower(left, mapping, inside)?,
                lower(right, mapping, inside)?,
            );
            // Equality with what the mapping says is the storage's null is
            // the null test of the other operand.
            let test: Option<fn(Expr<S>) -> Expr<S>> = match op {
                Infix::Comparison(Comparison::Eq) => Some(ast::is_null),
                Infix::Comparison(Comparison::Ne) => Some(ast::is_not_null),
                _ => None,
            };
            match (left, right, test) {
                (tested, Mapped::Null(_), Some(test)) | (Mapped::Null(_), tested, Some(test)) => {
                    Ok(Mapped::Scalar(test(scalar(tested)?)))
                }
                (left, right, _) => match (written(left), written(right)) {
                    (Written::One(left), Written::One(right)) => {
                        Ok(Mapped::Scalar(ast::infix(left, *op, right)))
                    }
                    (Written::Several(left), Written::Several(right)) => match op {
                        Infix::Comparison(Comparison::Eq) => equal(left, right).map(Mapped::Scalar),
                        // Unequal is "not equal in every part", which is not
                        // "unequal in every part": (1, 2) and (1, 3) differ.
                        Infix::Comparison(Comparison::Ne) => {
                            equal(left, right).map(ast::not).map(Mapped::Scalar)
                        }
                        _ => Err(TransformError::UnsupportedOperator(*op)),
                    },
                    (Written::Several(_), Written::One(_))
                    | (Written::One(_), Written::Several(_)) => Err(TransformError::NotComposite),
                },
            }
        }
        Expr::Any(source, predicate) => collection(source, predicate, mapping, inside),
    }
}

/// The member at `path`, as the mapping has it and where the member was.
///
/// Not inlined: the frame of `lower` is what the height of a tree costs in
/// stack, and this arm's locals would be in it at every node.
#[inline(never)]
fn member<D, S, M: Mapping<D, S>>(
    path: &Path,
    mapping: &M,
    inside: &Inside<'_>,
) -> Result<Mapped<S>, TransformError<M::Error>> {
    let mapped = mapping
        .field(&whole(path, inside)?)
        .map_err(TransformError::Mapping)?;
    placed(mapped, path.root(), inside)
}

/// `any` over the collection at `source`: a collection is a member, the
/// mapping says where it is, by a path from the candidate, and the
/// predicate is transformed inside it. Not inlined, as [`member`] is not.
#[inline(never)]
fn collection<D, S, M: Mapping<D, S>>(
    source: &Path,
    predicate: &Expr<D>,
    mapping: &M,
    inside: &Inside<'_>,
) -> Result<Mapped<S>, TransformError<M::Error>> {
    let domain = whole(source, inside)?;
    let storage = match mapping.field(&domain).map_err(TransformError::Mapping)? {
        Mapped::Scalar(Expr::Field(path)) if path.root() == Root::Global => path,
        _ => return Err(TransformError::CollectionNotAPlace),
    };
    let source = match placed::<S, M::Error>(
        Mapped::Scalar(Expr::Field(storage.clone())),
        source.root(),
        inside,
    )? {
        Mapped::Scalar(Expr::Field(source)) => source,
        _ => return Err(TransformError::CollectionNotAPlace),
    };
    let collection = Collection {
        domain: domain.names().map(str::to_owned).collect(),
        storage: storage.names().map(str::to_owned).collect(),
    };
    let inside: Vec<&Collection> = inside.iter().copied().chain([&collection]).collect();
    Ok(Mapped::Scalar(ast::any(
        source,
        scalar(lower(predicate, mapping, &inside)?)?,
    )))
}

fn scalar<S, E>(mapped: Mapped<S>) -> Result<Expr<S>, TransformError<E>> {
    match mapped {
        Mapped::Scalar(expr) => Ok(expr),
        Mapped::Null(null) => Ok(Expr::Value(null)),
        Mapped::Composite(_) => Err(TransformError::UnexpectedComposite),
    }
}

/// An operand where it is not tested for: one expression, or several. The
/// storage's null is the constant it carries, under any operator but the
/// equality that tests for it.
enum Written<S> {
    One(Expr<S>),
    Several(Vec<Mapped<S>>),
}

fn written<S>(mapped: Mapped<S>) -> Written<S> {
    match mapped {
        Mapped::Scalar(expr) => Written::One(expr),
        Mapped::Null(null) => Written::One(Expr::Value(null)),
        Mapped::Composite(parts) => Written::Several(parts),
    }
}

/// `left = right`, part by part: `l1 = r1 AND l2 = r2 AND …`.
fn equal<S, E>(left: Vec<Mapped<S>>, right: Vec<Mapped<S>>) -> Result<Expr<S>, TransformError<E>> {
    if left.len() != right.len() {
        return Err(TransformError::ShapeMismatch);
    }
    let mut parts = left.into_iter().zip(right).map(|pair| match pair {
        // A part that is the storage's null is tested for, as a whole is.
        (tested, Mapped::Null(_)) | (Mapped::Null(_), tested) => scalar(tested).map(ast::is_null),
        (Mapped::Scalar(left), Mapped::Scalar(right)) => Ok(ast::equal(left, right)),
        (Mapped::Composite(left), Mapped::Composite(right)) => equal(left, right),
        (Mapped::Composite(_), Mapped::Scalar(_)) | (Mapped::Scalar(_), Mapped::Composite(_)) => {
            Err(TransformError::ShapeMismatch)
        }
    });
    let first = parts.next().ok_or(TransformError::EmptyComposite)??;
    parts.try_fold(first, |all, part| Ok(ast::and(all, part?)))
}
