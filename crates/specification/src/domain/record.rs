//! A candidate made of plain data: what the sources' `DictContext`,
//! `NestedDictContext` and `CollectionContext` are between them. For tests,
//! for documents, and for a candidate that arrives as data rather than as a
//! domain object; a domain object implements [`Context`] itself.
//!
//! An object that may be absent — an `Option` of a Value Object — is a null
//! value where it is absent, `Record::value(Value::Null)`: null to a null
//! test, and no object to go into. A member left out is missing, which is
//! another thing.

use std::collections::BTreeMap;

use super::evaluate::{Context, ContextError};

/// A value, an object of named members, or a collection of items.
#[derive(Clone, Debug, PartialEq)]
pub enum Record<V> {
    /// A value.
    Value(V),
    /// Named members.
    Object(BTreeMap<String, Record<V>>),
    /// Items, in order.
    Collection(Vec<Record<V>>),
}

impl<V> Record<V> {
    /// The value `value`.
    pub fn value(value: impl Into<V>) -> Self {
        Record::Value(value.into())
    }

    /// An object of `members`.
    pub fn object<N: Into<String>>(members: impl IntoIterator<Item = (N, Record<V>)>) -> Self {
        Record::Object(
            members
                .into_iter()
                .map(|(name, member)| (name.into(), member))
                .collect(),
        )
    }

    /// A collection of `items`.
    pub fn collection(items: impl IntoIterator<Item = Record<V>>) -> Self {
        Record::Collection(items.into_iter().collect())
    }

    fn member(&self, name: &str) -> Result<&Record<V>, ContextError> {
        match self {
            Record::Object(members) => members
                .get(name)
                .ok_or_else(|| ContextError::Missing(name.to_owned())),
            // A value and a collection have no members to miss.
            Record::Value(_) | Record::Collection(_) => Err(ContextError::Missing(name.to_owned())),
        }
    }
}

impl<V: Clone> Context<V> for Record<V> {
    fn field(&self, name: &str) -> Result<V, ContextError> {
        match self.member(name)? {
            Record::Value(value) => Ok(value.clone()),
            Record::Object(_) | Record::Collection(_) => {
                Err(ContextError::NotAValue(name.to_owned()))
            }
        }
    }

    fn object(&self, name: &str) -> Result<&dyn Context<V>, ContextError> {
        match self.member(name)? {
            object @ Record::Object(_) => Ok(object),
            Record::Value(_) | Record::Collection(_) => {
                Err(ContextError::NotAnObject(name.to_owned()))
            }
        }
    }

    fn collection(&self, name: &str) -> Result<Vec<&dyn Context<V>>, ContextError> {
        match self.member(name)? {
            Record::Collection(items) => {
                Ok(items.iter().map(|item| item as &dyn Context<V>).collect())
            }
            Record::Value(_) | Record::Object(_) => {
                Err(ContextError::NotACollection(name.to_owned()))
            }
        }
    }
}
