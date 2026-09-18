//! How the collections of an aggregate are stored: the sources'
//! `SchemaRegistry`.
//!
//! A collection is either embedded in its parent's row, as an array, and
//! read with `unnest`; or kept in a table of its own, whose rows point at
//! the parent by a foreign key, simple or composite.

use std::collections::BTreeMap;

/// A collection kept in a table of its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Relation {
    pub(super) table: String,
    pub(super) keys: Vec<(String, String)>,
    pub(super) alias: Option<String>,
}

impl Relation {
    /// The rows of `table` whose column `child` equals the parent's column
    /// `parent`. A relation has at least this one pair: without any, every
    /// row of the table would belong to every parent.
    pub fn new(
        table: impl Into<String>,
        child: impl Into<String>,
        parent: impl Into<String>,
    ) -> Self {
        Relation {
            table: table.into(),
            keys: vec![(child.into(), parent.into())],
            alias: None,
        }
    }

    /// One more pair of a composite key.
    pub fn and(self, child: impl Into<String>, parent: impl Into<String>) -> Self {
        let mut keys = self.keys;
        keys.push((child.into(), parent.into()));
        Relation { keys, ..self }
    }

    /// What to call a row in the subquery, instead of the singular of the
    /// collection's name. A number is appended either way.
    pub fn alias(self, alias: impl Into<String>) -> Self {
        Relation {
            alias: Some(alias.into()),
            ..self
        }
    }
}

/// How a collection is stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Storage {
    /// In the parent's row, as an array.
    Embedded,
    /// In a table of its own.
    Relational(Relation),
}

/// How the collections of one aggregate are stored.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    table: String,
    alias: Option<String>,
    collections: BTreeMap<String, Storage>,
}

impl Schema {
    /// Of the aggregate whose root is a row of `table`.
    pub fn new(table: impl Into<String>) -> Self {
        Schema {
            table: table.into(),
            alias: None,
            collections: BTreeMap::new(),
        }
    }

    /// The alias the query gives the root's table: `s` of `FROM stores s`.
    pub fn alias(self, alias: impl Into<String>) -> Self {
        Schema {
            alias: Some(alias.into()),
            ..self
        }
    }

    /// The collection at `path` is embedded — which is what a collection
    /// not mentioned is taken to be.
    pub fn embedded(self, path: impl Into<String>) -> Self {
        self.collection(path, Storage::Embedded)
    }

    /// The collection at `path` is in a table of its own.
    pub fn relational(self, path: impl Into<String>, relation: Relation) -> Self {
        self.collection(path, Storage::Relational(relation))
    }

    /// The collection at `path` is stored as `storage`.
    ///
    /// `path` is the names from the candidate down, through the collections
    /// on the way: `"categories"`, and `"categories.items"` for the items of
    /// a category. The sources key a collection by its last name alone, so
    /// two collections of one name under different parents share a mapping.
    pub fn collection(self, path: impl Into<String>, storage: Storage) -> Self {
        let mut collections = self.collections;
        collections.insert(path.into(), storage);
        Schema {
            collections,
            ..self
        }
    }

    pub(super) fn storage(&self, path: &str) -> &Storage {
        self.collections.get(path).unwrap_or(&Storage::Embedded)
    }

    /// What a query calls the root's row: the alias, or the table.
    pub(super) fn parent(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}
