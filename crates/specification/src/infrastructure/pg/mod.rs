//! The reader that writes a specification as the condition of a PostgreSQL
//! query: the sources' `PostgresqlVisitor`.
//!
//! ```
//! use ascetic_ddd_specification::ast::{and, any, field, greater_than, value};
//! use ascetic_ddd_specification::{Expr, Path, Value, pg};
//!
//! let specification: Expr<Value> = and(
//!     field("active"),
//!     any("items", greater_than(field(Path::item("price")), value(500))),
//! );
//! let query = pg::compile(&specification)?;
//! assert_eq!(
//!     query.sql,
//!     r#""active" AND EXISTS (SELECT 1 FROM unnest("items") AS "item_1" WHERE "item_1"."price" > $1)"#,
//! );
//! assert_eq!(query.params, [Value::Int(500)]);
//! # Ok::<(), pg::CompileError>(())
//! ```
//!
//! Every constant becomes a numbered parameter; the text holds names and
//! operators only. A name is written between double quotes, as it is: it is
//! the column's name, to the letter, and nothing else PostgreSQL knows by
//! that word. A name that is not of ASCII letters, digits and `_` is
//! refused, and a quote inside one would be doubled, so that no tree,
//! whatever it was built from, can put SQL of its own into the query.
//!
//! A path of more than one name goes through objects. From the candidate an
//! object is a qualifier of the name, `"s"."price"`. From the item of a
//! collection it is a Value Object, a composite kept in the item's row:
//! `("item_1"."maker")."name"`. Either is what it is unless the [`Schema`]
//! says the object is kept in a table of its own, as it says of a
//! collection: then its member is read through the key, by a subquery in the
//! column's place. The sources write dots in every case, which PostgreSQL
//! reads as a table and a column; and from an item they drop its alias.
//!
//! The sources number parameters and aliases with counters that every
//! visitor shares and changes. Here the count so far goes into each step and
//! the count after comes out of it, so compiling changes nothing anywhere.
//!
//! # Where the text differs from the sources'
//!
//! * A constant with nothing but constants beside it has its type said,
//!   `$1::bigint + $2::bigint`: the server has nothing to find it by, and
//!   the sources' `$1 + $2` is "operator is not unique: unknown + unknown"
//!   to a driver that asks the server for the types. [`ParamType`] is how a
//!   value says its kind.
//! * Parentheses follow associativity as well as precedence. The sources
//!   compare precedences alone and write `a - (b - c)` as `a - b - c`, and
//!   `(a = b) = c` as `a = b = c`, which PostgreSQL does not parse.
//! * `IS` is written `IS NOT DISTINCT FROM`. PostgreSQL's `IS` takes a
//!   keyword — `TRUE`, `NULL` — and not a parameter: the sources' `x IS $1`
//!   is a syntax error.
//! * The predicate of a relational collection is parenthesised where it
//!   must be. The sources write `fk AND p OR q`, which selects rows of other
//!   parents through `q`.
//! * A collection named from the candidate inside another collection's
//!   predicate joins to the root row, not to the enclosing item.
//!
//! The Python source, besides, writes no parentheses at all: it looks a
//! precedence up by `"%s %s" % (operator, associativity)`, which for its
//! `str`-and-`Enum` operators is `"OPERATOR.AND ASSOCIATIVITY.LEFT_ASSOCIATIVE"`
//! and is in no table, so `(a OR b) AND c` comes out `a OR b AND c`.

mod identifier;
mod param_type;
mod precedence;
mod schema;
mod singular;
#[cfg(feature = "pg")]
mod to_sql;

use std::fmt;

use self::identifier::{identifier, plain, qualified, quoted};
pub use self::param_type::ParamType;
use self::precedence::{ATOM, Associativity};
pub use self::schema::{ForeignKey, Schema};
use crate::domain::ast::{Expr, Path, Root};
use crate::domain::operator::{Infix, Logical, Postfix, Prefix};

/// A condition and its parameters: `$1` is `params[0]`, unless the
/// compiler was given an [offset](Compiler::offset).
#[derive(Clone, Debug, PartialEq)]
pub struct Query<V> {
    /// The condition: what goes after `WHERE`.
    pub sql: String,
    /// The parameters, in the order of their numbers.
    pub params: Vec<V>,
}

/// A specification could not be compiled.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompileError {
    /// A path from the item under test, outside any collection.
    NoCurrentItem,
    /// A member of the candidate inside a collection's predicate, with no
    /// [`Schema`] to say what the candidate's row is called there.
    NoTable,
    /// A name that fits more than one key: a table with two keys to the row
    /// it is named from, or a column of two keys. The message names them;
    /// the tree names the key it means.
    AmbiguousKey(String),
    /// A key named in the tree that does not go where the tree stands: a
    /// collection's key not referencing the row, an object's key not on it.
    WrongKey(String),
    /// A name that is not one: anything but ASCII letters, digits and `_`,
    /// not starting with a digit.
    InvalidIdentifier(String),
    /// A text with a NUL in it: no text PostgreSQL has, so no query.
    NulInText,
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompileError::NoCurrentItem => f.write_str("no current item in context"),
            CompileError::NoTable => f.write_str(
                "a member of the candidate inside a collection's predicate needs the \
                 candidate's table: compile with a schema",
            ),
            CompileError::AmbiguousKey(message) | CompileError::WrongKey(message) => {
                f.write_str(message)
            }
            CompileError::NulInText => {
                f.write_str("a text with a NUL (U+0000) in it is no text PostgreSQL has")
            }
            CompileError::InvalidIdentifier(name) => {
                write!(f, "'{name}' is not a valid identifier")
            }
        }
    }
}

impl std::error::Error for CompileError {}

/// `specification` as a condition, its collections embedded, its parameters
/// numbered from `$1`: the sources' `compile_to_sql`.
pub fn compile<V: Clone + ParamType>(specification: &Expr<V>) -> Result<Query<V>, CompileError> {
    Compiler::new().compile(specification)
}

/// What a compilation can be told beforehand.
#[derive(Clone, Copy, Debug, Default)]
pub struct Compiler<'s> {
    schema: Option<&'s Schema>,
    offset: usize,
}

impl<'s> Compiler<'s> {
    /// Collections embedded, parameters from `$1`.
    pub fn new() -> Self {
        Compiler::default()
    }

    /// With the collections stored as `schema` says.
    pub fn schema(self, schema: &'s Schema) -> Self {
        Compiler {
            schema: Some(schema),
            ..self
        }
    }

    /// With `offset` parameters of the query already taken: the first one
    /// here is `$offset+1`. For a condition that is a part of a larger query.
    pub fn offset(self, offset: usize) -> Self {
        Compiler { offset, ..self }
    }

    /// `specification` as a condition.
    pub fn compile<V: Clone + ParamType>(
        &self,
        specification: &Expr<V>,
    ) -> Result<Query<V>, CompileError> {
        let start = Next {
            param: self.offset,
            alias: 0,
        };
        let (fragment, _) = self.render(specification, None, start)?;
        Ok(Query {
            sql: fragment.sql,
            params: fragment.params,
        })
    }

    fn render<V: Clone + ParamType>(
        &self,
        expr: &Expr<V>,
        item: Option<&Item>,
        next: Next,
    ) -> Result<(Fragment<V>, Next), CompileError> {
        match expr {
            Expr::Value(value) => {
                // In memory such a text is a string like any other; here it
                // meets the server, which has no such text.
                if value.nul_in_text() {
                    return Err(CompileError::NulInText);
                }
                let param = next.param + 1;
                let fragment = Fragment {
                    sql: format!("${param}"),
                    params: vec![value.clone()],
                    precedence: ATOM,
                };
                Ok((fragment, Next { param, ..next }))
            }
            Expr::Field(path) => {
                let (sql, next) = self.member(path, item, next)?;
                Ok((Fragment::text(sql), next))
            }
            Expr::Prefix(op, operand) => {
                let precedence = precedence::prefix(*op);
                let alone = param_type::under_prefix(*op, operand);
                let (operand, next) = self.render(operand, item, next)?;
                let operand = operand.of_type(alone);
                // `NOT NOT a` reads as it should; `--a` reads as a comment.
                let doubled = *op == Prefix::Neg && operand.precedence == precedence;
                let operand = operand.within(precedence, doubled);
                let sql = match op {
                    Prefix::Not => format!("NOT {}", operand.sql),
                    Prefix::Neg => format!("-{}", operand.sql),
                };
                Ok((operand.with(sql, precedence), next))
            }
            Expr::Postfix(operand, op) => {
                let precedence = precedence::postfix(*op);
                let alone = param_type::under_postfix(operand);
                let test = self.null_test(*op, operand, item)?;
                let (operand, next) = self.render(operand, item, next)?;
                let operand = operand.of_type(alone);
                let operand = operand.within(precedence, true);
                let sql = format!("{} {test}", operand.sql);
                Ok((operand.with(sql, precedence), next))
            }
            Expr::Infix(left, op, right) => {
                let (precedence, associativity) = precedence::infix(*op);
                let apart = |side| associativity != side && !precedence::regroups(*op);
                let (of_left, of_right) = param_type::of_both(left, *op, right);
                let count = param_type::is_a_count_to_cast(*op, right);
                let (left, next) = self.render(left, item, next)?;
                let (right, next) = self.render(right, item, next)?;
                let left = left.of_type(of_left);
                // A cast binds tighter than any operator: the count of a
                // shift is parenthesised against the cast, if it is not an
                // atom, before its type is said: `("b" + $1)::integer`.
                let right = if count {
                    right
                        .within(precedence::CAST, false)
                        .of_type(Some("integer"))
                } else {
                    right.of_type(of_right)
                };
                let left = left.within(precedence, apart(Associativity::Left));
                let right = right.within(precedence, apart(Associativity::Right));
                let sql = format!("{} {} {}", left.sql, spelling(*op), right.sql);
                Ok((left.with(sql, precedence).and(right.params), next))
            }
            Expr::Any(source, predicate) => self.exists(source, predicate, item, next),
        }
    }
    /// The words of a null test: of the value as a whole, for a column the
    /// schema declares a composite.
    ///
    /// Of a composite `IS NULL` is true when all its members are null and
    /// `IS NOT NULL` when none is — the standard's null predicate over a row
    /// value — so a row with a null member is neither. An `Option` of a
    /// Value Object is `Some` or `None` whatever its members hold, and so is
    /// the column: null, or a row. `IS DISTINCT FROM NULL` tests that, as
    /// the manual advises; a `None` is written as a null column, not as a
    /// row of nulls.
    fn null_test<V>(
        &self,
        op: Postfix,
        operand: &Expr<V>,
        item: Option<&Item>,
    ) -> Result<String, CompileError> {
        if !self.is_composite_column(operand, item)? {
            return Ok(op.to_string());
        }
        Ok(match op {
            Postfix::IsNull => "IS NOT DISTINCT FROM NULL",
            Postfix::IsNotNull => "IS DISTINCT FROM NULL",
        }
        .to_owned())
    }

    /// Whether `operand` is a column the schema declares a composite. The
    /// column is named as a key names it: by its table, or by the array it
    /// is a row of, `stores.items`; a composite inside a composite by the
    /// column, `stores.discount`.
    fn is_composite_column<V>(
        &self,
        operand: &Expr<V>,
        item: Option<&Item>,
    ) -> Result<bool, CompileError> {
        let (Expr::Field(path), Some(schema)) = (operand, self.schema) else {
            return Ok(false);
        };
        let names: Vec<&str> = path.names().collect();
        let Some((column, owners)) = names.split_last() else {
            return Ok(false);
        };
        let of = match path.root() {
            Root::Item(up) => item_out(item, up)?.row.clone(),
            Root::Global => schema.table().to_owned(),
        };
        let of = std::iter::once(of.as_str())
            .chain(owners.iter().copied())
            .collect::<Vec<_>>()
            .join(".");
        Ok(schema.is_composite(&of, column))
    }

    /// The key a collection named `name` in a row of `row` is joined by:
    /// the one of that name, if it references the row; else the one key on
    /// the table `name` that does. None: the name is an array in the row.
    fn key_of_collection(
        &self,
        row: &str,
        name: &str,
    ) -> Result<Option<&'s ForeignKey>, CompileError> {
        let Some(schema) = self.schema else {
            return Ok(None);
        };
        if let Some(key) = schema.key_named(name) {
            if key.referenced_table != row {
                return Err(CompileError::WrongKey(format!(
                    "the key {name} references {}, not {row}",
                    key.referenced_table
                )));
            }
            return Ok(Some(key));
        }
        match schema.keys_referencing(name, row).as_slice() {
            [] => Ok(None),
            [key] => Ok(Some(key)),
            keys => Err(CompileError::AmbiguousKey(format!(
                "{name} has {} keys to {row}: {}; name the key",
                keys.len(),
                keys.iter()
                    .map(|key| key.name())
                    .collect::<Vec<_>>()
                    .join(", "),
            ))),
        }
    }

    /// The key an object named `name` in a row of `row` is read through:
    /// the one of that name, if it is on the row; else the one key on the
    /// row that `name` is a column of. None: the name is a composite in the
    /// row.
    fn key_of_object(&self, row: &str, name: &str) -> Result<Option<&'s ForeignKey>, CompileError> {
        let Some(schema) = self.schema else {
            return Ok(None);
        };
        if let Some(key) = schema.key_named(name) {
            if key.table != row {
                return Err(CompileError::WrongKey(format!(
                    "the key {name} is on {}, not {row}",
                    key.table
                )));
            }
            return Ok(Some(key));
        }
        match schema.keys_on(row, name).as_slice() {
            [] => Ok(None),
            [key] => Ok(Some(key)),
            keys => Err(CompileError::AmbiguousKey(format!(
                "{name} is a column of {} keys of {row}: {}; name the key",
                keys.len(),
                keys.iter()
                    .map(|key| key.name())
                    .collect::<Vec<_>>()
                    .join(", "),
            ))),
        }
    }
    /// The value of the member at `path`, as the storage has it.
    ///
    /// An object on the way to the member is looked up in the schema, as a
    /// collection is, by the names that lead to it. Kept in a table of its
    /// own, it is reached through its key. Not mentioned, it is what the
    /// dots have meant so far: from the item under test a composite kept in
    /// the item's row — a Value Object; from the candidate a qualifier of
    /// the name, `"s"."price"`.
    fn member(
        &self,
        path: &Path,
        item: Option<&Item>,
        next: Next,
    ) -> Result<(String, Next), CompileError> {
        let names: Vec<&str> = path.names().collect();
        match path.root() {
            Root::Item(up) => {
                let item = item_out(item, up)?;
                self.member_of_row(quoted(&item.alias), item.row.clone(), &names, next)
            }
            // An object of the candidate kept in a table of its own, or a
            // composite column of its row - a Value Object - that the schema
            // says is one: the dots of an undeclared name are a qualifier.
            Root::Global => match (names.first(), self.schema) {
                (Some(object), Some(schema))
                    if names.len() > 1
                        && (self.key_of_object(schema.table(), object)?.is_some()
                            || schema.is_composite(schema.table(), object)) =>
                {
                    self.member_of_row(
                        qualified(schema.row())?,
                        schema.table().to_owned(),
                        &names,
                        next,
                    )
                }
                _ => Ok((self.column(path, item)?, next)),
            },
        }
    }

    /// The member at `names` of the row written `row`, which is a row of
    /// `of` to the schema: a table, or the composite at a column of one.
    ///
    /// An object kept in a table of its own is read by a subquery in the
    /// column's place: it has at most the one row the key names, and is null
    /// if there is none, as a member of a composite that is null is. An
    /// object not mentioned is a composite in its row, and the parentheses
    /// are what makes it that: with dots alone PostgreSQL reads a schema, a
    /// table and a column, and there is no such table.
    fn member_of_row(
        &self,
        row: String,
        of: String,
        names: &[&str],
        next: Next,
    ) -> Result<(String, Next), CompileError> {
        match names {
            [] => Ok((row, next)),
            [name] => Ok((format!("{row}.{}", identifier(name)?), next)),
            [object, rest @ ..] => {
                let Some(key) = self.key_of_object(&of, object)? else {
                    let composite = format!("({row}.{})", identifier(object)?);
                    return self.member_of_row(composite, format!("{of}.{object}"), rest, next);
                };
                // The row read is one of the referenced table, and its alias
                // says so.
                let number = next.alias + 1;
                let alias = quoted(&format!("{}_{number}", singular_of(&key.referenced_table)?));
                let keys = key
                    .columns
                    .iter()
                    .zip(&key.referenced_columns)
                    .map(|(column, referenced)| {
                        Ok(format!(
                            "{alias}.{} = {row}.{}",
                            identifier(referenced)?,
                            identifier(column)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join(" AND ");
                let next = Next {
                    alias: number,
                    ..next
                };
                let (member, next) =
                    self.member_of_row(alias.clone(), key.referenced_table.clone(), rest, next)?;
                let sql = format!(
                    "(SELECT {member} FROM {} AS {alias} WHERE {keys})",
                    qualified(&key.referenced_table)?,
                );
                Ok((sql, next))
            }
        }
    }

    /// `EXISTS (SELECT 1 FROM … AS alias WHERE …)` over an array of the
    /// parent's row, or over the rows of a table that point at the parent.
    fn exists<V: Clone + ParamType>(
        &self,
        source: &Path,
        predicate: &Expr<V>,
        item: Option<&Item>,
        next: Next,
    ) -> Result<(Fragment<V>, Next), CompileError> {
        let enclosing = match source.root() {
            Root::Global => None,
            Root::Item(up) => Some(item_out(item, up)?),
        };
        // The row the collection is of, to the schema: the enclosing item's,
        // or the root's. Without a schema there is no relation to look for.
        let of: Option<String> = match (enclosing, self.schema) {
            (Some(item), _) => Some(item.row.clone()),
            (None, Some(schema)) => Some(schema.table().to_owned()),
            (None, None) => None,
        };
        let name: String = source.names().collect::<Vec<_>>().join(".");
        // A key is what a schema says, so it comes with its schema.
        let key = match of.as_deref() {
            Some(of) => self.key_of_collection(of, &name)?,
            None => None,
        };
        let relation = key.zip(self.schema);
        let number = next.alias + 1;
        // The alias is the singular of the row's table, or of the array's
        // name: the compiler's own.
        let alias = match key {
            Some(key) => singular_of(&key.table)?,
            None => singular_of(source.name())?,
        };
        let inner = Item {
            alias: format!("{alias}_{number}"),
            row: match key {
                Some(key) => key.table.clone(),
                None => format!("{}.{name}", of.unwrap_or_default()),
            },
            outer: item,
        };
        let next = Next {
            alias: number,
            ..next
        };
        let (predicate, next) = self.render(predicate, Some(&inner), next)?;
        let alias = quoted(&inner.alias);
        let fragment = match relation {
            None => {
                let sql = format!(
                    "EXISTS (SELECT 1 FROM unnest({}) AS {alias} WHERE {})",
                    self.column(source, item)?,
                    predicate.sql,
                );
                predicate.with(sql, ATOM)
            }
            Some((key, schema)) => {
                let parent = match enclosing {
                    Some(item) => quoted(&item.alias),
                    None => qualified(schema.row())?,
                };
                let keys = key
                    .columns
                    .iter()
                    .zip(&key.referenced_columns)
                    .map(|(column, referenced)| {
                        Ok(format!(
                            "{alias}.{} = {parent}.{}",
                            identifier(column)?,
                            identifier(referenced)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join(" AND ");
                // The predicate is an operand of the `AND` after the keys.
                let (and, _) = precedence::infix(Infix::Logical(Logical::And));
                let predicate = predicate.within(and, false);
                let sql = format!(
                    "EXISTS (SELECT 1 FROM {} AS {alias} WHERE {keys} AND {})",
                    qualified(&key.table)?,
                    predicate.sql,
                );
                predicate.with(sql, ATOM)
            }
        };
        Ok((fragment, next))
    }

    /// The path as a column reference, no object on its way looked up: the
    /// place of a collection, and a name from the candidate.
    ///
    /// From the candidate the names are written with dots, which PostgreSQL
    /// reads as a qualified name: `"s"."price"` is the column `price` of `s`.
    /// From the item under test the first name is a column of the item's row,
    /// under its alias, and what follows a member of a composite kept there.
    fn column(&self, path: &Path, item: Option<&Item>) -> Result<String, CompileError> {
        let names = path
            .names()
            .map(identifier)
            .collect::<Result<Vec<_>, _>>()?;
        match path.root() {
            // Inside a collection's predicate the candidate's column is
            // qualified with its row: unqualified, PostgreSQL reads it from
            // the innermost row that has a column of that name, and a category
            // with a `limit` of its own hid the shop's. A name of several parts
            // the author qualified.
            Root::Global if names.len() == 1 && item.is_some() => {
                let schema = self.schema.ok_or(CompileError::NoTable)?;
                Ok(format!("{}.{}", qualified(schema.row())?, names[0]))
            }
            Root::Global => Ok(names.join(".")),
            Root::Item(up) => {
                let alias = quoted(&item_out(item, up)?.alias);
                Ok(match names.split_first() {
                    Some((column, members)) if !members.is_empty() => {
                        format!("({alias}.{column}).{}", members.join("."))
                    }
                    _ => format!("{alias}.{}", names.join(".")),
                })
            }
        }
    }
}

/// How many parameters and aliases have been numbered so far.
#[derive(Clone, Copy)]
struct Next {
    param: usize,
    alias: usize,
}

/// The item under test, inside a collection's predicate: what its row is
/// called in the query, what it is a row of to the schema - a table, or the
/// composite at a column of one - and the item of the enclosing collection's
/// predicate, if there is one.
struct Item<'a> {
    alias: String,
    row: String,
    outer: Option<&'a Item<'a>>,
}

/// The singular of the last name of `table`, in lower case: what an alias
/// is made of.
fn singular_of(table: &str) -> Result<String, CompileError> {
    let name = table.rsplit('.').next().unwrap_or(table);
    Ok(singular::singular(&plain(name)?.to_lowercase()))
}

/// The item `up` collections out from the item under test: the aliases of
/// the enclosing queries are in scope of the inner one, as SQL has it.
fn item_out<'a>(item: Option<&'a Item<'a>>, up: usize) -> Result<&'a Item<'a>, CompileError> {
    let mut item = item;
    for _ in 0..up {
        item = item.and_then(|item| item.outer);
    }
    item.ok_or(CompileError::NoCurrentItem)
}

/// A piece of the condition, and how tightly its outermost operator binds.
struct Fragment<V> {
    sql: String,
    params: Vec<V>,
    precedence: u8,
}

impl<V> Fragment<V> {
    fn text(sql: String) -> Self {
        Fragment {
            sql,
            params: Vec::new(),
            precedence: ATOM,
        }
    }

    /// This, as an operand of an operator of `precedence`: parenthesised if
    /// it binds looser, or as tight and `apart` — on the side the operator
    /// does not group towards.
    fn within(self, precedence: u8, apart: bool) -> Self {
        if self.precedence < precedence || (self.precedence == precedence && apart) {
            Fragment {
                sql: format!("({})", self.sql),
                precedence: ATOM,
                ..self
            }
        } else {
            self
        }
    }

    /// This, its type said if there is one to say: a cast binds tighter than
    /// any operator, so what was an atom is one still.
    fn of_type(self, param_type: Option<&'static str>) -> Self {
        match param_type {
            Some(param_type) => Fragment {
                sql: format!("{}::{param_type}", self.sql),
                ..self
            },
            None => self,
        }
    }

    /// The same parameters under other text.
    fn with(self, sql: String, precedence: u8) -> Self {
        Fragment {
            sql,
            precedence,
            ..self
        }
    }

    /// With `params` after its own.
    fn and(self, params: Vec<V>) -> Self {
        let mut all = self.params;
        all.extend(params);
        Fragment {
            params: all,
            ..self
        }
    }
}

fn spelling(op: Infix) -> String {
    match op {
        Infix::Is => "IS NOT DISTINCT FROM".to_owned(),
        Infix::Comparison(_) | Infix::Logical(_) | Infix::Arithmetic(_) => op.to_string(),
    }
}
