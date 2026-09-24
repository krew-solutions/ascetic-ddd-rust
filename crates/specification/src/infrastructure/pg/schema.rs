//! How the rows of a storage point at one another: the sources'
//! `SchemaRegistry`.
//!
//! A schema is the foreign keys of a storage, as `\d` shows them, and
//! nothing of any aggregate or query: a key is on a table, of columns, and
//! references a table's columns. What the compiler calls a row in a query -
//! an alias - is the compiler's own, made as it goes. The one thing of the
//! query in a schema is the table the query is of, `FROM stores s`: the row
//! the compiler starts from, and what it qualifies that row's columns with.
//!
//! A tree names a collection by the table its rows are in, `store_items`,
//! or, where two keys of that table reference the same row, by the key's
//! name; and an object kept in a table of its own by the key's column,
//! `owner_id`. A key has a name as it has in PostgreSQL: the one it is given,
//! or `<table>_<columns>_fkey`. A Value Object kept in the query's row as a
//! column of a composite type is declared as one, `composite("stores",
//! "address")`, and a path through it from the candidate is a member of it,
//! `("s"."address")."city"`: from the candidate an undeclared name is a
//! table's alias, `"s"."price"`.

/// A foreign key: `table (columns) REFERENCES referenced_table
/// (referenced_columns)`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ForeignKey {
    name: Option<String>,
    pub(super) table: String,
    pub(super) columns: Vec<String>,
    pub(super) referenced_table: String,
    pub(super) referenced_columns: Vec<String>,
}

impl ForeignKey {
    /// The key on `table` of `column`, referencing `referenced_column` of
    /// `referenced_table`. A key has at least this one column: without any,
    /// every row of the table would belong to every row it references.
    /// `table` is a table; or, for a key on a row of an array in a
    /// composite, which has no table, the array's column by its table,
    /// `stores.items`.
    pub fn new(
        table: impl Into<String>,
        column: impl Into<String>,
        referenced_table: impl Into<String>,
        referenced_column: impl Into<String>,
    ) -> Self {
        ForeignKey {
            name: None,
            table: table.into(),
            columns: vec![column.into()],
            referenced_table: referenced_table.into(),
            referenced_columns: vec![referenced_column.into()],
        }
    }

    /// One more column of a composite key, and the column it references.
    pub fn and(self, column: impl Into<String>, referenced_column: impl Into<String>) -> Self {
        let mut columns = self.columns;
        let mut referenced_columns = self.referenced_columns;
        columns.push(column.into());
        referenced_columns.push(referenced_column.into());
        ForeignKey {
            columns,
            referenced_columns,
            ..self
        }
    }

    /// The key's name, where the storage gives it one: `CONSTRAINT name`.
    pub fn named(self, name: impl Into<String>) -> Self {
        ForeignKey {
            name: Some(name.into()),
            ..self
        }
    }

    /// The key's name: the one it was given, or the one PostgreSQL gives a
    /// key that was not, `<table>_<columns>_fkey`.
    pub fn name(&self) -> String {
        self.name.clone().unwrap_or_else(|| {
            let table = self.table.rsplit('.').next().unwrap_or(&self.table);
            format!("{table}_{}_fkey", self.columns.join("_"))
        })
    }
}

/// The foreign keys of a storage, for the queries of one table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Schema {
    table: String,
    alias: Option<String>,
    keys: Vec<ForeignKey>,
    composites: Vec<(String, String)>,
}

impl Schema {
    /// For the queries of `table`: the row the compiler starts from.
    pub fn new(table: impl Into<String>) -> Self {
        Schema {
            table: table.into(),
            alias: None,
            keys: Vec::new(),
            composites: Vec::new(),
        }
    }

    /// The alias the query gives its table: `s` of `FROM stores s`.
    pub fn alias(self, alias: impl Into<String>) -> Self {
        Schema {
            alias: Some(alias.into()),
            ..self
        }
    }

    /// A key of one column: [`ForeignKey::new`].
    pub fn foreign_key(
        self,
        table: impl Into<String>,
        column: impl Into<String>,
        referenced_table: impl Into<String>,
        referenced_column: impl Into<String>,
    ) -> Self {
        self.key(ForeignKey::new(
            table,
            column,
            referenced_table,
            referenced_column,
        ))
    }

    /// A key as built: composite, or named.
    pub fn key(self, key: ForeignKey) -> Self {
        let mut keys = self.keys;
        keys.push(key);
        Schema { keys, ..self }
    }

    /// The column `column` of `table` is of a composite type: a Value Object
    /// kept in the row. From the candidate a path through it is a member of
    /// the composite, `("s"."address")."city"`, where a name not declared is
    /// a table's alias, `"s"."price"`.
    pub fn composite(self, table: impl Into<String>, column: impl Into<String>) -> Self {
        let mut composites = self.composites;
        composites.push((table.into(), column.into()));
        Schema { composites, ..self }
    }

    /// Whether `column` of `table` is declared a composite.
    pub(super) fn is_composite(&self, table: &str, column: &str) -> bool {
        self.composites
            .iter()
            .any(|(of, name)| of == table && name == column)
    }

    /// The key called `name`, if there is one.
    pub(super) fn key_named(&self, name: &str) -> Option<&ForeignKey> {
        self.keys.iter().find(|key| key.name() == name)
    }

    /// The keys on `table` that reference `referenced_table`.
    pub(super) fn keys_referencing(&self, table: &str, referenced_table: &str) -> Vec<&ForeignKey> {
        self.keys
            .iter()
            .filter(|key| key.table == table && key.referenced_table == referenced_table)
            .collect()
    }

    /// The keys on `table` that `column` is a column of.
    pub(super) fn keys_on(&self, table: &str, column: &str) -> Vec<&ForeignKey> {
        self.keys
            .iter()
            .filter(|key| key.table == table && key.columns.iter().any(|c| c == column))
            .collect()
    }

    /// The query's table, as given: what its row is to a key.
    pub(super) fn table(&self) -> &str {
        &self.table
    }

    /// What the query calls its table's row: the alias, or the table.
    pub(super) fn row(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_key_is_named_as_postgresql_names_it_unless_named() {
        assert_eq!(
            ForeignKey::new("transfers", "from_account_id", "accounts", "id").name(),
            "transfers_from_account_id_fkey"
        );
        assert_eq!(
            ForeignKey::new("public.orders", "tenant_id", "tenants", "tenant_id")
                .and("customer_id", "id")
                .name(),
            "orders_tenant_id_customer_id_fkey"
        );
        assert_eq!(
            ForeignKey::new("transfers", "from_account_id", "accounts", "id")
                .named("outgoing")
                .name(),
            "outgoing"
        );
    }
}
