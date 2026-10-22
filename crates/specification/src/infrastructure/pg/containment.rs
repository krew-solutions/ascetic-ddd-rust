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
//! three times the time of the scan, measured on PostgreSQL 14. The fold is
//! a custom plan's, where the parameters are known; under a generic plan the
//! pattern is evaluated for every row of a sequential scan, three times the
//! time of the equality it replaced, and once for an index scan, where it
//! costs nothing (ADR-0027).

use super::layout::{Items, Kind, Layout, Member};
use super::param_type::ParamType;
use super::{CompileError, Next, key};
use crate::domain::ast::{Expr, Path, Root};
use crate::domain::operator::{Comparison, Infix, Logical};

/// What a document is asked to contain: an object of members, each a
/// constant of a kind, an object, or an array that contains objects or
/// constants.
pub(super) struct Pattern<V> {
    members: Vec<(String, Contained<V>)>,
}

pub(super) enum Contained<V> {
    Constant(Spelled, V),
    Object(Pattern<V>),
    Array(Vec<Contained<V>>),
}

/// A kind with one spelling in JSON, and the type its constant is said.
#[derive(Clone, Copy)]
pub(super) enum Spelled {
    Text,
    Int,
    Bool,
}

impl Spelled {
    /// The kind, if it has one spelling in JSON.
    fn of(kind: Kind) -> Option<Self> {
        match kind {
            Kind::Text => Some(Spelled::Text),
            Kind::Int => Some(Spelled::Int),
            Kind::Bool => Some(Spelled::Bool),
            Kind::Float | Kind::Timestamp | Kind::Date | Kind::Uuid => None,
        }
    }
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
        let Member::Value(kind) = layout.member(name)? else {
            return None;
        };
        let spelled = Spelled::of(*kind)?;
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
        let Some(items) = layout.member(name).and_then(Member::items) else {
            return Self::none();
        };
        Self::of_array(&names, Self::elements(predicate, items)).unwrap_or_else(Self::none)
    }

    /// What an array whose items are `items` contains when one of them
    /// satisfies `predicate`: an object of what the predicate says of its
    /// members, or the constants it says the value is equal to.
    pub(super) fn elements(predicate: &Expr<V>, items: Items<'_>) -> Vec<Contained<V>> {
        match items {
            Items::Objects(layout) => {
                let object = Self::of_item(predicate, layout);
                if object.is_empty() {
                    Vec::new()
                } else {
                    vec![Contained::Object(object)]
                }
            }
            Items::Values(kind) => match Spelled::of(kind) {
                Some(spelled) => Self::constants(predicate, spelled),
                None => Vec::new(),
            },
        }
    }

    /// The constants that `predicate`, by `AND`, says the item itself is
    /// equal to. An item equal to two is equal to both, so the array
    /// contains both.
    fn constants(predicate: &Expr<V>, spelled: Spelled) -> Vec<Contained<V>> {
        match predicate {
            Expr::Infix(left, Infix::Logical(Logical::And), right) => {
                let mut constants = Self::constants(left, spelled);
                constants.extend(Self::constants(right, spelled));
                constants
            }
            Expr::Infix(left, Infix::Comparison(Comparison::Eq), right) => {
                match (left.as_ref(), right.as_ref()) {
                    (Expr::Item(0), Expr::Value(value)) | (Expr::Value(value), Expr::Item(0))
                        if value.param_type().is_some() =>
                    {
                        vec![Contained::Constant(spelled, value.clone())]
                    }
                    _ => Vec::new(),
                }
            }
            _ => Vec::new(),
        }
    }

    /// An object that contains, at `names`, an array that contains
    /// `elements`: none if there are none.
    pub(super) fn of_array(names: &[&str], elements: Vec<Contained<V>>) -> Option<Self> {
        let (name, objects) = names.split_last()?;
        (!elements.is_empty())
            .then(|| Self::nested(objects, Self::of(name, Contained::Array(elements))))
    }

    /// Whether the pattern asks for nothing: every document contains it.
    pub(super) fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// The condition that `document` contains the pattern, its constants
    /// as parameters numbered from `next`: of the document itself, or of the
    /// value at `at` in it, where a GIN index is - `("g"."document" ->
    /// 'subgroups') @> …`, the expression the index is on. None if the pattern
    /// is not a path to that value alone.
    pub(super) fn containing(
        self,
        document: &str,
        at: &[String],
        next: Next,
    ) -> Result<Option<(String, Vec<V>, Next)>, CompileError> {
        let mut params = Vec::new();
        if at.is_empty() {
            let (pattern, next) = self.object(&mut params, next)?;
            return Ok(Some((format!("{document} @> {pattern}"), params, next)));
        }
        let Some(contained) = self.at(at) else {
            return Ok(None);
        };
        let (json, next) = contained.json(&mut params, next)?;
        let path = at
            .iter()
            .map(|name| key(name))
            .collect::<Result<Vec<_>, _>>()?
            .join(" -> ");
        // `->`, `@>` and `||` bind alike, from the left.
        Ok(Some((
            format!("({document} -> {path}) @> ({json})"),
            params,
            next,
        )))
    }

    /// What the pattern asks of the value at `path`, where it asks nothing
    /// beside it on the way there.
    fn at(self, path: &[String]) -> Option<Contained<V>> {
        let (name, rest) = path.split_first()?;
        let mut members = self.members;
        let (member, contained) = members.pop()?;
        if !members.is_empty() || member != *name {
            return None;
        }
        match (rest.is_empty(), contained) {
            (true, contained) => Some(contained),
            (false, Contained::Object(inner)) => inner.at(rest),
            (false, Contained::Constant(..) | Contained::Array(_)) => None,
        }
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
                Contained::Constant(Spelled::Text, value) => {
                    let (param, next) = parameter(value, params, next)?;
                    (
                        format!("jsonb_object(ARRAY[{key}], ARRAY[${param}::text])"),
                        next,
                    )
                }
                member => {
                    let (value, next) = member.json(params, next)?;
                    (
                        format!("jsonb_set('{{}}'::jsonb, ARRAY[{key}], {value})"),
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

impl<V: Clone + ParamType> Contained<V> {
    /// This as a `jsonb` value: a constant cast to its type and read as
    /// JSON - a text through `jsonb_object`, which writes it as a JSON
    /// string - an object, or an array, `'[]'::jsonb ||` each of its items.
    fn json(self, params: &mut Vec<V>, next: Next) -> Result<(String, Next), CompileError> {
        match self {
            Contained::Constant(spelled, value) => {
                let (param, next) = parameter(value, params, next)?;
                let json = match spelled {
                    Spelled::Text => {
                        format!("(jsonb_object(ARRAY['v'], ARRAY[${param}::text]) -> 'v')")
                    }
                    Spelled::Int => format!("${param}::bigint::text::jsonb"),
                    Spelled::Bool => format!("${param}::boolean::text::jsonb"),
                };
                Ok((json, next))
            }
            Contained::Object(object) => object.object(params, next),
            Contained::Array(items) => {
                let mut written = vec!["'[]'::jsonb".to_owned()];
                let mut next = next;
                for item in items {
                    let (item, after) = item.json(params, next)?;
                    written.push(item);
                    next = after;
                }
                Ok((written.join(" || "), next))
            }
        }
    }
}

/// `value` as the next parameter.
fn parameter<V: ParamType>(
    value: V,
    params: &mut Vec<V>,
    next: Next,
) -> Result<(usize, Next), CompileError> {
    if value.nul_in_text() {
        return Err(CompileError::NulInText);
    }
    let param = next.param + 1;
    params.push(value);
    Ok((param, Next { param, ..next }))
}
