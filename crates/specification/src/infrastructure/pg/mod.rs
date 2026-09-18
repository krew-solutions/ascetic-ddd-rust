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
//!     "active AND EXISTS (SELECT 1 FROM unnest(items) AS item_1 WHERE item_1.price > $1)",
//! );
//! assert_eq!(query.params, [Value::Int(500)]);
//! # Ok::<(), pg::CompileError>(())
//! ```
//!
//! Every constant becomes a numbered parameter; the text holds names and
//! operators only, and a name that is not an identifier is refused rather
//! than quoted, so that no tree, whatever it was built from, can put SQL of
//! its own into the query.
//!
//! The sources number parameters and aliases with counters that every
//! visitor shares and changes. Here the count so far goes into each step and
//! the count after comes out of it, so compiling changes nothing anywhere.
//!
//! # Where the text differs from the sources'
//!
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

mod precedence;
mod schema;
mod singular;
#[cfg(feature = "pg")]
mod to_sql;

use std::fmt;

use self::precedence::{ATOM, Associativity};
pub use self::schema::{Relation, Schema, Storage};
use crate::domain::ast::{Expr, Path, Root};
use crate::domain::operator::{Infix, Logical, Prefix};

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
    /// A name that cannot be written into a query as it is: anything but
    /// ASCII letters, digits and `_`, not starting with a digit.
    InvalidIdentifier(String),
}

impl fmt::Display for CompileError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompileError::NoCurrentItem => f.write_str("no current item in context"),
            CompileError::InvalidIdentifier(name) => {
                write!(f, "'{name}' is not a valid identifier")
            }
        }
    }
}

impl std::error::Error for CompileError {}

/// `specification` as a condition, its collections embedded, its parameters
/// numbered from `$1`: the sources' `compile_to_sql`.
pub fn compile<V: Clone>(specification: &Expr<V>) -> Result<Query<V>, CompileError> {
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
    pub fn compile<V: Clone>(&self, specification: &Expr<V>) -> Result<Query<V>, CompileError> {
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

    fn render<V: Clone>(
        &self,
        expr: &Expr<V>,
        item: Option<&Item>,
        next: Next,
    ) -> Result<(Fragment<V>, Next), CompileError> {
        match expr {
            Expr::Value(value) => {
                let param = next.param + 1;
                let fragment = Fragment {
                    sql: format!("${param}"),
                    params: vec![value.clone()],
                    precedence: ATOM,
                };
                Ok((fragment, Next { param, ..next }))
            }
            Expr::Field(path) => Ok((Fragment::text(column(path, item)?), next)),
            Expr::Prefix(op, operand) => {
                let precedence = precedence::prefix(*op);
                let (operand, next) = self.render(operand, item, next)?;
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
                let (operand, next) = self.render(operand, item, next)?;
                let operand = operand.within(precedence, true);
                let sql = format!("{} {op}", operand.sql);
                Ok((operand.with(sql, precedence), next))
            }
            Expr::Infix(left, op, right) => {
                let (precedence, associativity) = precedence::infix(*op);
                let apart = |side| associativity != side && !precedence::regroups(*op);
                let (left, next) = self.render(left, item, next)?;
                let (right, next) = self.render(right, item, next)?;
                let left = left.within(precedence, apart(Associativity::Left));
                let right = right.within(precedence, apart(Associativity::Right));
                let sql = format!("{} {} {}", left.sql, spelling(*op), right.sql);
                Ok((left.with(sql, precedence).and(right.params), next))
            }
            Expr::Any(source, predicate) => self.exists(source, predicate, item, next),
        }
    }

    /// `EXISTS (SELECT 1 FROM … AS alias WHERE …)` over an array of the
    /// parent's row, or over the rows of a table that point at the parent.
    fn exists<V: Clone>(
        &self,
        source: &Path,
        predicate: &Expr<V>,
        item: Option<&Item>,
        next: Next,
    ) -> Result<(Fragment<V>, Next), CompileError> {
        let enclosing = match source.root() {
            Root::Global => None,
            Root::Item => Some(item.ok_or(CompileError::NoCurrentItem)?),
        };
        let logical: Vec<&str> = enclosing
            .map_or(&[][..], |item| &item.logical)
            .iter()
            .copied()
            .chain(source.names())
            .collect();
        // A relation is what a schema says, so it comes with its schema.
        let relation = self
            .schema
            .and_then(|schema| match schema.storage(&logical.join(".")) {
                Storage::Relational(relation) => Some((relation, schema)),
                Storage::Embedded => None,
            });
        let number = next.alias + 1;
        let name = match relation.and_then(|(relation, _)| relation.alias.as_deref()) {
            Some(alias) => identifier(alias)?.to_owned(),
            None => singular::singular(&identifier(source.name())?.to_lowercase()),
        };
        let inner = Item {
            alias: format!("{name}_{number}"),
            logical,
        };
        let next = Next {
            alias: number,
            ..next
        };
        let (predicate, next) = self.render(predicate, Some(&inner), next)?;
        let alias = &inner.alias;
        let fragment = match relation {
            None => {
                let sql = format!(
                    "EXISTS (SELECT 1 FROM unnest({}) AS {alias} WHERE {})",
                    column(source, item)?,
                    predicate.sql,
                );
                predicate.with(sql, ATOM)
            }
            Some((relation, schema)) => {
                let parent = match enclosing {
                    Some(item) => item.alias.as_str(),
                    None => identifier(schema.parent())?,
                };
                let keys = relation
                    .keys
                    .iter()
                    .map(|(child, of_parent)| {
                        Ok(format!(
                            "{alias}.{} = {parent}.{}",
                            identifier(child)?,
                            identifier(of_parent)?,
                        ))
                    })
                    .collect::<Result<Vec<_>, _>>()?
                    .join(" AND ");
                // The predicate is an operand of the `AND` after the keys.
                let (and, _) = precedence::infix(Infix::Logical(Logical::And));
                let predicate = predicate.within(and, false);
                let sql = format!(
                    "EXISTS (SELECT 1 FROM {} AS {alias} WHERE {keys} AND {})",
                    qualified(&relation.table)?,
                    predicate.sql,
                );
                predicate.with(sql, ATOM)
            }
        };
        Ok((fragment, next))
    }
}

/// How many parameters and aliases have been numbered so far.
#[derive(Clone, Copy)]
struct Next {
    param: usize,
    alias: usize,
}

/// The item under test, inside a collection's predicate: what its row is
/// called, and the names that lead to its collection from the candidate.
struct Item<'a> {
    alias: String,
    logical: Vec<&'a str>,
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

/// The path as a column reference: from the item under test, under its
/// alias.
fn column(path: &Path, item: Option<&Item>) -> Result<String, CompileError> {
    let alias = match path.root() {
        Root::Global => None,
        Root::Item => Some(item.ok_or(CompileError::NoCurrentItem)?.alias.as_str()),
    };
    alias
        .map(Ok)
        .into_iter()
        .chain(path.names().map(identifier))
        .collect::<Result<Vec<_>, _>>()
        .map(|names| names.join("."))
}

fn identifier(name: &str) -> Result<&str, CompileError> {
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

/// A table's name, which may carry its schema: `public.items`.
fn qualified(name: &str) -> Result<&str, CompileError> {
    name.split('.')
        .try_for_each(|part| identifier(part).map(drop))?;
    Ok(name)
}

fn spelling(op: Infix) -> String {
    match op {
        Infix::Is => "IS NOT DISTINCT FROM".to_owned(),
        Infix::Comparison(_) | Infix::Logical(_) | Infix::Arithmetic(_) => op.to_string(),
    }
}
