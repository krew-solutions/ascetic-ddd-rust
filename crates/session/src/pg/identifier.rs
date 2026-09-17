//! A name that may be spliced into SQL.
//!
//! Table and sequence names go into statements as text, not as parameters:
//! PostgreSQL takes no parameter where an identifier goes. So a name must be
//! known safe before it is used, and it is known safe by being an
//! [`Identifier`]: lower-case letters, digits and underscores, not starting
//! with a digit, at most forty characters — room for the suffixes the
//! adapters append, `_meta`, `_slots`, `__waiting_since_idx`, within the
//! sixty-three PostgreSQL keeps. Unquoted, so the name is what the catalogue
//! shows; unqualified, since the indexes named after a table cannot carry a
//! schema — the `search_path` decides.

use std::fmt;

/// The longest name accepted, leaving room for the adapters' suffixes.
pub const MAX_IDENTIFIER: usize = 40;

/// A table or sequence name safe to splice into SQL.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Identifier(String);

/// A name refused by [`Identifier::new`], and why.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidIdentifier {
    /// The name as given.
    pub name: String,
    /// What is wrong with it.
    pub reason: &'static str,
}

impl fmt::Display for InvalidIdentifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "`{}` is not an identifier: {}", self.name, self.reason)
    }
}

impl std::error::Error for InvalidIdentifier {}

impl Identifier {
    /// Accepts `[a-z_][a-z0-9_]*` of at most [`MAX_IDENTIFIER`] characters.
    pub fn new(name: impl Into<String>) -> Result<Self, InvalidIdentifier> {
        let name = name.into();
        let refuse = |reason| InvalidIdentifier {
            name: name.clone(),
            reason,
        };
        let mut chars = name.chars();
        match chars.next() {
            None => return Err(refuse("it is empty")),
            Some(first) if !(first.is_ascii_lowercase() || first == '_') => {
                return Err(refuse(
                    "it must start with a lower-case letter or an underscore",
                ));
            }
            Some(_) => {}
        }
        if !chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') {
            return Err(refuse(
                "only lower-case letters, digits and underscores are allowed; a schema \
                 qualification is not, the search_path decides",
            ));
        }
        if name.len() > MAX_IDENTIFIER {
            return Err(refuse("it is longer than forty characters"));
        }
        Ok(Identifier(name))
    }

    /// The name.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Identifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<&str> for Identifier {
    type Error = InvalidIdentifier;

    fn try_from(name: &str) -> Result<Self, Self::Error> {
        Identifier::new(name)
    }
}

impl TryFrom<String> for Identifier {
    type Error = InvalidIdentifier;

    fn try_from(name: String) -> Result<Self, Self::Error> {
        Identifier::new(name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_that_may_go_into_sql() {
        for name in ["inbox", "inbox_2", "_x", "outbox_orders_offsets"] {
            assert_eq!(
                Identifier::new(name).map(|i| i.to_string()),
                Ok(name.to_owned())
            );
        }
    }

    #[test]
    fn names_that_may_not() {
        for name in [
            "",
            "Inbox",
            "2inbox",
            "public.inbox",
            "inbox; drop table inbox; --",
            "inbox-orders",
            "a_very_long_table_name_of_forty_one_charac",
        ] {
            assert!(Identifier::new(name).is_err(), "{name:?} was accepted");
        }
    }
}
