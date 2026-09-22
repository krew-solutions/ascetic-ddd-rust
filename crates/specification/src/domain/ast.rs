//! The tree of a specification, and the functions that build it.
//!
//! The Python and Go sources describe the tree as classes with `accept` and
//! read it through a `Visitor`. A visitor is how a language without sum
//! types spells a `match`; here the tree is a sum type, [`Expr`], and a
//! reader is a function that matches on it. New readers need no change
//! here, which is what the visitor was for.
//!
//! The tree is generic in its values. `Expr<V>` says nothing of `V`; the
//! readers ask for what they need — the evaluator an [`Operand`], the
//! PostgreSQL compiler a `Clone`. So one tree type serves every stage a
//! specification goes through, and the stage is in the type: a template with
//! unbound placeholders is an `Expr<Slot>`, a bound one an `Expr<Value>`; a
//! specification over domain values is an `Expr<D>`, the same over column
//! values an `Expr<S>`. [`Expr::try_map_values`] goes from one to the next.
//!
//! [`Operand`]: crate::domain::operand::Operand

use super::operator::{Infix, Postfix, Prefix};

/// Where a path starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Root {
    /// The candidate itself: JSONPath's `$`.
    Global,
    /// The item under test in an enclosing [`Expr::Any`], by how far out
    /// that one is: `Item(0)` is the item of the nearest, JSONPath's `@`;
    /// `Item(1)` the item of the collection enclosing that one, which the
    /// text of JSONPath cannot name and a predicate written in the host
    /// language can - the category, from the predicate of its products.
    Item(usize),
}

/// A member reached from a root through nested objects:
/// `user.profile.age`. Never empty — it names at least the member.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Path {
    root: Root,
    objects: Vec<String>,
    name: String,
}

impl Path {
    /// The member `name` of `root`.
    pub fn new(root: Root, name: impl Into<String>) -> Self {
        Path {
            root,
            objects: Vec::new(),
            name: name.into(),
        }
    }

    /// The member `name` of the candidate.
    pub fn global(name: impl Into<String>) -> Self {
        Path::new(Root::Global, name)
    }

    /// The member `name` of the item under test.
    pub fn item(name: impl Into<String>) -> Self {
        Path::new(Root::Item(0), name)
    }

    /// The member `name` of the item `up` collections out: `outer(1, ..)` is
    /// the item of the collection enclosing the nearest, `outer(0, ..)` is
    /// [`Path::item`].
    pub fn outer(up: usize, name: impl Into<String>) -> Self {
        Path::new(Root::Item(up), name)
    }

    /// One step down: what this path named is an object, and the path now
    /// names its member `name`.
    pub fn child(self, name: impl Into<String>) -> Self {
        let Path {
            root,
            mut objects,
            name: object,
        } = self;
        objects.push(object);
        Path {
            root,
            objects,
            name: name.into(),
        }
    }

    /// The member `name` of the same object: `user.profile.age` to
    /// `user.profile.name`. What a mapping needs when one member of the
    /// domain is several columns of one table.
    pub fn sibling(&self, name: impl Into<String>) -> Self {
        Path {
            name: name.into(),
            ..self.clone()
        }
    }

    /// The dotted `names` from `root`: `"user.profile.age"`. The text is
    /// split at the dots and nothing else is done to it: whether a name is
    /// one the storage accepts is the storage's to say.
    pub fn dotted(root: Root, names: &str) -> Self {
        let mut names = names.split('.');
        let first = names.next().unwrap_or_default();
        names.fold(Path::new(root, first), Path::child)
    }

    /// Where the path starts.
    pub fn root(&self) -> Root {
        self.root
    }

    /// The objects walked through, outermost first.
    pub fn objects(&self) -> &[String] {
        &self.objects
    }

    /// The member named.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Every name of the path in order: the objects, then the member.
    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.objects
            .iter()
            .map(String::as_str)
            .chain(std::iter::once(self.name.as_str()))
    }
}

/// A dotted path from the candidate.
impl From<&str> for Path {
    fn from(names: &str) -> Self {
        Path::dotted(Root::Global, names)
    }
}

/// A specification: a tree over values of type `V`.
#[derive(Clone, Debug, PartialEq)]
pub enum Expr<V> {
    /// A constant.
    Value(V),
    /// The value of a member.
    Field(Path),
    /// An operator before its operand.
    Prefix(Prefix, Box<Expr<V>>),
    /// An operator between its operands.
    Infix(Box<Expr<V>>, Infix, Box<Expr<V>>),
    /// An operator after its operand.
    Postfix(Box<Expr<V>>, Postfix),
    /// At least one item of the collection at the path satisfies the
    /// predicate, in which [`Root::Item`] is that item. True or false, never
    /// unknown: an item whose predicate is unknown is not a witness. The
    /// sources call this node `Wildcard`, after JSONPath's `[*]`.
    Any(Path, Box<Expr<V>>),
}

impl<V> Expr<V> {
    /// The same tree over other values: each constant goes through `f`, in
    /// order, left to right; the first failure is the result.
    pub fn try_map_values<W, E>(&self, f: &impl Fn(&V) -> Result<W, E>) -> Result<Expr<W>, E> {
        Ok(match self {
            Expr::Value(value) => Expr::Value(f(value)?),
            Expr::Field(path) => Expr::Field(path.clone()),
            Expr::Prefix(op, operand) => Expr::Prefix(*op, Box::new(operand.try_map_values(f)?)),
            Expr::Infix(left, op, right) => Expr::Infix(
                Box::new(left.try_map_values(f)?),
                *op,
                Box::new(right.try_map_values(f)?),
            ),
            Expr::Postfix(operand, op) => Expr::Postfix(Box::new(operand.try_map_values(f)?), *op),
            Expr::Any(source, predicate) => {
                Expr::Any(source.clone(), Box::new(predicate.try_map_values(f)?))
            }
        })
    }
}

/// The constant `value`.
pub fn value<V>(value: impl Into<V>) -> Expr<V> {
    Expr::Value(value.into())
}

/// The value of the member at `path`.
pub fn field<V>(path: impl Into<Path>) -> Expr<V> {
    Expr::Field(path.into())
}

/// `NOT operand`.
pub fn not<V>(operand: Expr<V>) -> Expr<V> {
    Expr::Prefix(Prefix::Not, Box::new(operand))
}

/// `-operand`.
pub fn neg<V>(operand: Expr<V>) -> Expr<V> {
    Expr::Prefix(Prefix::Neg, Box::new(operand))
}

/// `left op right`.
pub fn infix<V>(left: Expr<V>, op: Infix, right: Expr<V>) -> Expr<V> {
    Expr::Infix(Box::new(left), op, Box::new(right))
}

/// `left = right`.
pub fn equal<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::EQ, right)
}

/// `left != right`.
pub fn not_equal<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::NE, right)
}

/// `left > right`.
pub fn greater_than<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::GT, right)
}

/// `left < right`.
pub fn less_than<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::LT, right)
}

/// `left >= right`.
pub fn greater_than_equal<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::GE, right)
}

/// `left <= right`.
pub fn less_than_equal<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::LE, right)
}

/// `left IS right`: equality in which null is a value.
pub fn is<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::IS, right)
}

/// `left AND right`.
pub fn and<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::AND, right)
}

/// `left OR right`.
pub fn or<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::OR, right)
}

/// `first AND second AND …`, nested to the left, as the sources' variadic
/// `And` nests it; `first` alone if there is no other.
pub fn and_all<V>(first: Expr<V>, rest: impl IntoIterator<Item = Expr<V>>) -> Expr<V> {
    rest.into_iter().fold(first, and)
}

/// `first OR second OR …`, nested to the left; `first` alone if there is no
/// other.
pub fn or_all<V>(first: Expr<V>, rest: impl IntoIterator<Item = Expr<V>>) -> Expr<V> {
    rest.into_iter().fold(first, or)
}

/// `left + right`.
pub fn add<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::ADD, right)
}

/// `left - right`.
pub fn sub<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::SUB, right)
}

/// `left * right`.
pub fn mul<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::MUL, right)
}

/// `left / right`.
pub fn div<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::DIV, right)
}

/// `left % right`.
pub fn modulo<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::MOD, right)
}

/// `left << right`.
pub fn left_shift<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::SHL, right)
}

/// `left >> right`.
pub fn right_shift<V>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    infix(left, Infix::SHR, right)
}

/// `operand IS NULL`.
pub fn is_null<V>(operand: Expr<V>) -> Expr<V> {
    Expr::Postfix(Box::new(operand), Postfix::IsNull)
}

/// `operand IS NOT NULL`.
pub fn is_not_null<V>(operand: Expr<V>) -> Expr<V> {
    Expr::Postfix(Box::new(operand), Postfix::IsNotNull)
}

/// Some item of the collection at `source` satisfies `predicate`.
pub fn any<V>(source: impl Into<Path>, predicate: Expr<V>) -> Expr<V> {
    Expr::Any(source.into(), Box::new(predicate))
}

/// No item of the collection at `source` fails `predicate`: written as
/// `NOT any(source, NOT predicate)`, which is what it means, so that the
/// tree needs no second quantifier and no reader a second case. An item
/// whose predicate is unknown does not fail it — the same reading SQL gives
/// `NOT EXISTS (… WHERE NOT predicate)`.
pub fn all<V>(source: impl Into<Path>, predicate: Expr<V>) -> Expr<V> {
    not(any(source, not(predicate)))
}
