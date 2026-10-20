//! What a `jsonb` document of the storage holds: the members of each object
//! in it, and the kind of each value.
//!
//! A column says its type to the server, and the compiler leaves the rest to
//! it: a parameter beside a column takes the column's type. A member of a
//! `jsonb` document says nothing of the kind. JSON has numbers, strings and
//! booleans, so `3.0` equals `3` there, and a point in time, a date and a UUID
//! are strings. The server reads such a member as text, `->>`, and compares
//! it as text. A string ordered as text is not ordered as the point in time
//! it spells: `"2026-01-10T00:00:00.000000Z"` is greater than `"2026-01-10"`,
//! which means the same instant. So the compiler is told the kind, and says
//! it in the query: `("s"."document" ->> 'priority')::bigint`, which is a
//! value of that type as a column is, and the rest is the server's as it is
//! for a column.
//!
//! A layout is of the storage, as a [`Schema`](super::Schema) is: it says
//! what the encoding of a document writes, and nothing of any aggregate. The
//! [`Mapping`](crate::Mapping) says which members of the aggregate are read
//! from the document and which from the columns beside it. A member the
//! layout does not declare is refused, as a member the mapping does not know
//! is.

/// The kind of a value in a document, and the type it is read as.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    /// `true` or `false`: `boolean`.
    Bool,
    /// An integer: `bigint`. A number with a fraction there is an error of
    /// the server.
    Int,
    /// A number: `double precision`.
    Float,
    /// A string, ordered by code point as the evaluator orders it:
    /// `COLLATE "C"`. The database's collation orders `'B'` after `'a'`.
    Text,
    /// A point in time, written in ISO 8601 with its offset: `timestamptz`.
    /// Written at microseconds: the server rounds what is finer, and the
    /// evaluator's [`Timestamp`](crate::Timestamp) has none of it.
    Timestamp,
    /// A date, `YYYY-MM-DD`: `date`.
    Date,
    /// A UUID, in either case: `uuid`.
    Uuid,
}

impl Kind {
    /// `text`, a member read as text, read as this kind: cast, or ordered by
    /// code point. A cast binds tighter than any operator, so the cast text
    /// is an operand as it stands; `COLLATE` binds looser than unary minus,
    /// so the collated text is parenthesised.
    pub(super) fn read(self, text: &str) -> String {
        match self {
            Kind::Text => format!("({text} COLLATE \"C\")"),
            Kind::Bool => format!("{text}::boolean"),
            Kind::Int => format!("{text}::bigint"),
            Kind::Float => format!("{text}::double precision"),
            Kind::Timestamp => format!("{text}::timestamptz"),
            Kind::Date => format!("{text}::date"),
            Kind::Uuid => format!("{text}::uuid"),
        }
    }
}

/// A member of an object in a document.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Member {
    /// A value of this kind, or null.
    Value(Kind),
    /// An object of these members, or null.
    Object(Layout),
    /// An array of objects of these members.
    Collection(Layout),
}

/// The members of an object in a document: the document itself, an object
/// in it, an item of an array in it.
///
/// ```
/// use ascetic_ddd_specification::pg::{Kind, Layout};
///
/// let policy = Layout::new()
///     .value("status", Kind::Text)
///     .value("priority", Kind::Int)
///     .object("targeting", Layout::new().value("mode", Kind::Text))
///     .collection(
///         "versions",
///         Layout::new()
///             .value("number", Kind::Int)
///             .value("published_at", Kind::Timestamp),
///     );
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Layout {
    members: Vec<(String, Member)>,
}

impl Layout {
    /// An object of no members yet.
    pub fn new() -> Self {
        Layout::default()
    }

    /// The member `name` is a value of the kind `kind`, or null.
    pub fn value(self, name: impl Into<String>, kind: Kind) -> Self {
        self.with(name.into(), Member::Value(kind))
    }

    /// The member `name` is an object of the members `layout`, or null.
    pub fn object(self, name: impl Into<String>, layout: Layout) -> Self {
        self.with(name.into(), Member::Object(layout))
    }

    /// The member `name` is an array of objects of the members `layout`.
    pub fn collection(self, name: impl Into<String>, layout: Layout) -> Self {
        self.with(name.into(), Member::Collection(layout))
    }

    /// What the member `name` is: the last declaration of that name.
    pub(super) fn member(&self, name: &str) -> Option<&Member> {
        self.members
            .iter()
            .rev()
            .find(|(declared, _)| declared == name)
            .map(|(_, member)| member)
    }

    fn with(self, name: String, member: Member) -> Self {
        let mut members = self.members;
        members.push((name, member));
        Layout { members }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_declared_again_is_what_it_was_declared_last() {
        let layout = Layout::new()
            .value("at", Kind::Text)
            .value("at", Kind::Timestamp);
        assert_eq!(layout.member("at"), Some(&Member::Value(Kind::Timestamp)));
        assert_eq!(layout.member("by"), None);
    }
}
