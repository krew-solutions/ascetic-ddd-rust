//! Equalities on members of a document as what the document contains:
//! `"p"."document" @> {"status": "active"}`, which a GIN index on the
//! document serves, where `("p"."document" ->> 'status') = $1` is served by
//! no index of the document as a whole.
//!
//! The two agree where only truth counts. Of a member that is null, or not
//! there, the equality is null and containment false; at the root of a
//! condition, under `AND` and `OR` there, and as the predicate of `any`,
//! null and false select alike. Under `NOT` they do not, and nothing there is
//! written as containment. They agree for a kind with one spelling in JSON
//! alone: a text, an integer, a boolean. A number with a fraction has
//! several (`0.10000000000000001` is `0.1` as a `double precision` and not as
//! JSON), a point in time has its offsets and its fractions, a UUID its case.
//! And they agree for a constant that is not null: `= NULL` is null, and
//! `{"status": null}` is contained in a document whose status is null.
//!
//! The pattern is built of functions the server folds into a constant,
//! `jsonb_object` and `jsonb_set`, which are immutable: `jsonb_build_object`
//! is stable, and is called again for every row a sequential scan reads -
//! three times the time of the scan, measured on PostgreSQL 14.

use super::layout::{Kind, Layout, Member};
use super::param_type::ParamType;
use super::{CompileError, Next, key};
use crate::domain::ast::{Expr, Path, Root};
use crate::domain::operator::{Comparison, Infix, Logical};

/// What a document is asked to contain: an object of members, each a
/// constant of a kind, an object, or an array that contains objects.
pub(super) struct Pattern<V> {
    members: Vec<(String, Contained<V>)>,
}

enum Contained<V> {
    Constant(Spelled, V),
    Object(Pattern<V>),
    Array(Vec<Pattern<V>>),
}

/// A kind with one spelling in JSON, and the type its constant is said.
#[derive(Clone, Copy)]
enum Spelled {
    Text,
    Int,
    Bool,
}

impl<V: Clone + ParamType> Contained<V> {
    /// What contains both: see [`Pattern::and`].
    fn and(self, other: Self) -> Self {
        match (self, other) {
            (Contained::Object(mine), Contained::Object(yours)) => {
                Contained::Object(mine.and(yours))
            }
            (Contained::Array(mut mine), Contained::Array(yours)) => {
                mine.extend(yours);
                Contained::Array(mine)
            }
            (mine, _) => mine,
        }
    }
}

impl<V: Clone + ParamType> Pattern<V> {
    /// The member at `names` of an object that holds what `layout` says,
    /// equal to `value`: none if the member is not a value of a kind with
    /// one spelling, or `value` is null.
    pub(super) fn equal(layout: &Layout, names: &[&str], value: &V) -> Option<Self> {
        let (name, objects) = names.split_last()?;
        let mut layout = layout;
        for object in objects {
            let Some(Member::Object(inner)) = layout.member(object) else {
                return None;
            };
            layout = inner;
        }
        let spelled = match layout.member(name)? {
            Member::Value(Kind::Text) => Spelled::Text,
            Member::Value(Kind::Int) => Spelled::Int,
            Member::Value(Kind::Bool) => Spelled::Bool,
            Member::Value(Kind::Float | Kind::Timestamp | Kind::Date | Kind::Uuid)
            | Member::Object(_)
            | Member::Collection(_) => return None,
        };
        value.param_type()?;
        let constant = Contained::Constant(spelled, value.clone());
        Some(Self::nested(objects, Pattern::of(name, constant)))
    }

    /// What an object that holds what `layout` contains when `predicate` is
    /// true of it as an item: the equalities of its members with constants
    /// that `AND` joins, and those of its own collections' items under
    /// `any`. Anything else is left out: what the pattern says follows from
    /// the predicate, not the other way.
    pub(super) fn of_item(predicate: &Expr<V>, layout: &Layout) -> Self {
        match predicate {
            Expr::Infix(left, Infix::Logical(Logical::And), right) => {
                Self::of_item(left, layout).and(Self::of_item(right, layout))
            }
            Expr::Infix(left, Infix::Comparison(Comparison::Eq), right) => {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Field(path), Expr::Value(value))
                    | (Expr::Value(value), Expr::Field(path))
                        if path.root() == Root::Item(0) =>
                    {
                        let names: Vec<&str> = path.names().collect();
                        Self::equal(layout, &names, value).unwrap_or_else(Self::none)
                    }
                    _ => Self::none(),
                }
            }
            Expr::Any(source, predicate) if source.root() == Root::Item(0) => {
                Self::of_collection(layout, source, predicate)
            }
            _ => Self::none(),
        }
    }

    /// What an object that holds what `layout` contains when an item of its
    /// collection at `source` satisfies `predicate`.
    fn of_collection(layout: &Layout, source: &Path, predicate: &Expr<V>) -> Self {
        let names: Vec<&str> = source.names().collect();
        let Some((name, objects)) = names.split_last() else {
            return Self::none();
        };
        let mut layout = layout;
        for object in objects {
            let Some(Member::Object(inner)) = layout.member(object) else {
                return Self::none();
            };
            layout = inner;
        }
        let Some(Member::Collection(items)) = layout.member(name) else {
            return Self::none();
        };
        Self::of_item(predicate, items)
            .in_array(name)
            .map_or_else(Self::none, |pattern| Self::nested(objects, pattern))
    }

    /// An object that contains, at `names`, an array that contains an
    /// object that contains `item`: none if `item` asks for nothing.
    pub(super) fn of_array(names: &[&str], item: Self) -> Option<Self> {
        let (name, objects) = names.split_last()?;
        item.in_array(name)
            .map(|pattern| Self::nested(objects, pattern))
    }

    /// Whether the pattern asks for nothing: every document contains it.
    pub(super) fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// The pattern as the text of a query, its constants as parameters
    /// numbered from `next`.
    pub(super) fn written(self, next: Next) -> Result<(String, Vec<V>, Next), CompileError> {
        let mut params = Vec::new();
        let (sql, next) = self.object(&mut params, next)?;
        Ok((sql, params, next))
    }

    fn none() -> Self {
        Pattern {
            members: Vec::new(),
        }
    }

    fn of(name: &str, member: Contained<V>) -> Self {
        Pattern {
            members: vec![(name.to_owned(), member)],
        }
    }

    /// `pattern` at the end of the objects `objects`.
    fn nested(objects: &[&str], pattern: Self) -> Self {
        objects.iter().rev().fold(pattern, |inner, object| {
            Self::of(object, Contained::Object(inner))
        })
    }

    /// An object whose member `name` is an array that contains this.
    fn in_array(self, name: &str) -> Option<Self> {
        (!self.is_empty()).then(|| Self::of(name, Contained::Array(vec![self])))
    }

    /// What contains both: the members of objects at one name merged, the
    /// items of arrays at one name both asked for. Of two constants at one
    /// name the first is kept, as of a constant and an object: the pattern
    /// then asks for less, and still follows from what it is made of.
    fn and(self, other: Self) -> Self {
        let mut members = self.members;
        for (name, theirs) in other.members {
            match members.iter().position(|(ours, _)| *ours == name) {
                None => members.push((name, theirs)),
                Some(at) => {
                    let (name, ours) = members.remove(at);
                    members.insert(at, (name, ours.and(theirs)));
                }
            }
        }
        Pattern { members }
    }

    /// The object: `jsonb_object` of a text member, `jsonb_set` of `{}` with
    /// any other, and `||` between them, parenthesised where there are
    /// several - `||` and `@>` bind alike, from the left.
    fn object(self, params: &mut Vec<V>, next: Next) -> Result<(String, Next), CompileError> {
        let several = self.members.len() > 1;
        let mut next = next;
        let mut entries = Vec::new();
        for (name, member) in self.members {
            let key = key(&name)?;
            let (entry, after) = match member {
                Contained::Constant(spelled, value) => {
                    if value.nul_in_text() {
                        return Err(CompileError::NulInText);
                    }
                    let param = next.param + 1;
                    params.push(value);
                    let entry = match spelled {
                        Spelled::Text => {
                            format!("jsonb_object(ARRAY[{key}], ARRAY[${param}::text])")
                        }
                        Spelled::Int => format!(
                            "jsonb_set('{{}}'::jsonb, ARRAY[{key}], ${param}::bigint::text::jsonb)"
                        ),
                        Spelled::Bool => format!(
                            "jsonb_set('{{}}'::jsonb, ARRAY[{key}], ${param}::boolean::text::jsonb)"
                        ),
                    };
                    (entry, Next { param, ..next })
                }
                Contained::Object(object) => {
                    let (object, next) = object.object(params, next)?;
                    (
                        format!("jsonb_set('{{}}'::jsonb, ARRAY[{key}], {object})"),
                        next,
                    )
                }
                Contained::Array(items) => {
                    let mut written = vec!["'[]'::jsonb".to_owned()];
                    let mut next = next;
                    for item in items {
                        let (item, after) = item.object(params, next)?;
                        written.push(item);
                        next = after;
                    }
                    (
                        format!(
                            "jsonb_set('{{}}'::jsonb, ARRAY[{key}], {})",
                            written.join(" || ")
                        ),
                        next,
                    )
                }
            };
            entries.push(entry);
            next = after;
        }
        let sql = entries.join(" || ");
        Ok((if several { format!("({sql})") } else { sql }, next))
    }
}
