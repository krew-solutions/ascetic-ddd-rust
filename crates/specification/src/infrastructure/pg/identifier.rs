//! A name, as the text of a query has it: between double quotes.
//!
//! A word PostgreSQL knows is read as what PostgreSQL knows, and a member of
//! an aggregate may be called anything: `user` without quotes is the
//! session's user, so `user = $1` parses and selects other rows than were
//! asked for, and `order` does not parse. Which words these are depends on
//! the server's version, which a library does not know; so no name is
//! looked up in a list, and every name is quoted.
//!
//! Between quotes a name is the column's name to the letter: `"createdAt"` is
//! the column created as `"createdAt"`, which `createdAt` without quotes is
//! not — PostgreSQL folds that to `createdat`. What a member of the domain is
//! called in the storage is for a [`Mapping`](crate::Mapping) to
//! say.
//!
//! Two things keep SQL of a tree's own out of the text, and neither rests on
//! the other: a name is of ASCII letters, digits and `_` or it is refused,
//! and a double quote inside a name is doubled, as PostgreSQL reads it.

use super::CompileError;

/// `name`, checked and quoted.
pub(super) fn identifier(name: &str) -> Result<String, CompileError> {
    plain(name).map(quoted)
}

/// A table's name, which may carry its schema: `public.items` is
/// `"public"."items"`.
pub(super) fn qualified(name: &str) -> Result<String, CompileError> {
    name.split('.')
        .map(identifier)
        .collect::<Result<Vec<_>, _>>()
        .map(|parts| parts.join("."))
}

/// `name`, if it is of ASCII letters, digits and `_` and does not start with
/// a digit.
pub(super) fn plain(name: &str) -> Result<&str, CompileError> {
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|first| first.is_ascii_alphabetic() || first == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_');
    if valid {
        Ok(name)
    } else {
        Err(CompileError::InvalidIdentifier(name.to_owned()))
    }
}

/// `name` between double quotes, a double quote of its own written twice.
pub(super) fn quoted(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_name_is_quoted_as_it_is_written() {
        assert_eq!(identifier("user").as_deref(), Ok(r#""user""#));
        assert_eq!(identifier("createdAt").as_deref(), Ok(r#""createdAt""#));
        assert_eq!(
            qualified("public.items").as_deref(),
            Ok(r#""public"."items""#)
        );
    }

    #[test]
    fn a_quote_inside_a_name_does_not_end_it() {
        assert_eq!(quoted(r#"a" OR "b"#), r#""a"" OR ""b""#);
        assert_eq!(quoted(r#"""#), r#""""""#);
    }

    #[test]
    fn a_name_outside_the_alphabet_is_refused() {
        for name in ["", "1st", "a b", r#"a"b"#, "a.b", "имя", "a\0"] {
            assert_eq!(
                plain(name),
                Err(CompileError::InvalidIdentifier(name.to_owned()))
            );
        }
    }
}
