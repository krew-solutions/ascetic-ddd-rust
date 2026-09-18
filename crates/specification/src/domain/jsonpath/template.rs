//! A template: read once, bound to parameters as often as needed.
//!
//! The sources keep the parsed tree and, for each match, copy it with every
//! placeholder marker replaced — a marker being a value of a shape no real
//! value is supposed to have. Here a template's tree is over [`Slot`]s, a
//! bound one over [`Value`]s, and binding is the map from one to the other:
//! that a tree still has placeholders in it is a fact of its type.

use std::collections::BTreeMap;
use std::fmt;
use std::str::FromStr;

use super::error::{BindError, MatchError, SyntaxError};
use super::{lexer, parser};
use crate::domain::ast::Expr;
use crate::domain::evaluate::{Context, is_satisfied_by};
use crate::domain::operand::Operand;
use crate::domain::value::Value;

/// How a placeholder names its parameter.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum ParamKey {
    /// By its place among the template's placeholders, from zero: `%d`.
    Position(usize),
    /// By name: `%(age)d`.
    Name(String),
}

impl fmt::Display for ParamKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ParamKey::Position(position) => write!(f, "#{position}"),
            ParamKey::Name(name) => write!(f, "'{name}'"),
        }
    }
}

/// What a placeholder's letter asks of its parameter. A null fits any.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum ParamKind {
    /// `%s`: a value of any kind, as Python's `%s` takes any object.
    Any,
    /// `%d`: an integer.
    Integer,
    /// `%f`: a number, integer or float.
    Number,
}

impl fmt::Display for ParamKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            ParamKind::Any => "a value",
            ParamKind::Integer => "an integer",
            ParamKind::Number => "a number",
        })
    }
}

impl ParamKind {
    fn admits(self, value: &Value) -> bool {
        match (self, value) {
            (ParamKind::Any, _) | (_, Value::Null) => true,
            (ParamKind::Integer, Value::Int(_)) => true,
            (ParamKind::Number, Value::Int(_) | Value::Float(_)) => true,
            (ParamKind::Integer | ParamKind::Number, _) => false,
        }
    }
}

/// A placeholder.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Param {
    /// Which parameter it takes.
    pub key: ParamKey,
    /// What it asks of it.
    pub kind: ParamKind,
}

/// What stands where a value will: the value, if the template spells it
/// out, or a placeholder for it.
#[derive(Clone, Debug, PartialEq)]
pub enum Slot {
    /// A literal of the template.
    Literal(Value),
    /// A placeholder.
    Param(Param),
}

/// The parameters of one match.
#[derive(Clone, Debug, PartialEq)]
pub enum Params {
    /// For `%s`, `%d`, `%f`, in the order of the placeholders.
    Positional(Vec<Value>),
    /// For `%(name)s` and its kin. A name no placeholder uses is ignored, as
    /// Python's `%` ignores a key of its mapping.
    Named(BTreeMap<String, Value>),
}

impl Params {
    /// No parameters: for a template without placeholders.
    pub fn none() -> Self {
        Params::Positional(Vec::new())
    }

    /// Positional parameters.
    pub fn positional<T: Into<Value>>(values: impl IntoIterator<Item = T>) -> Self {
        Params::Positional(values.into_iter().map(Into::into).collect())
    }

    /// Named parameters.
    pub fn named<N: Into<String>, T: Into<Value>>(
        values: impl IntoIterator<Item = (N, T)>,
    ) -> Self {
        Params::Named(
            values
                .into_iter()
                .map(|(name, value)| (name.into(), value.into()))
                .collect(),
        )
    }

    fn get(&self, key: &ParamKey) -> Result<&Value, BindError> {
        match (self, key) {
            (Params::Positional(values), ParamKey::Position(position)) => values.get(*position),
            (Params::Named(values), ParamKey::Name(name)) => values.get(name),
            _ => return Err(BindError::WrongStyle),
        }
        .ok_or_else(|| BindError::Missing(key.clone()))
    }
}

/// A parsed template.
#[derive(Clone, Debug, PartialEq)]
pub struct Template {
    source: String,
    expr: Expr<Slot>,
    positional: usize,
}

impl Template {
    /// Reads `source`. See the [module](super) for the grammar.
    pub fn parse(source: &str) -> Result<Self, SyntaxError> {
        let tokens = lexer::tokenize(source).map_err(|error| error.within(source))?;
        let expr = parser::template(&tokens, source.chars().count())
            .map_err(|error| error.within(source))?;
        Ok(Template {
            source: source.to_owned(),
            expr,
            positional: lexer::positional(&tokens),
        })
    }

    /// The text this was read from.
    pub fn source(&self) -> &str {
        &self.source
    }

    /// The tree, placeholders in place.
    pub fn expr(&self) -> &Expr<Slot> {
        &self.expr
    }

    /// The specification this is with `params` for its placeholders.
    pub fn bind(&self, params: &Params) -> Result<Expr<Value>, BindError> {
        let bound = self.expr.try_map_values(&|slot| match slot {
            Slot::Literal(value) => Ok(value.clone()),
            Slot::Param(Param { key, kind }) => {
                let value = params.get(key)?;
                if kind.admits(value) {
                    Ok(value.clone())
                } else {
                    Err(BindError::Mismatch {
                        key: key.clone(),
                        expected: *kind,
                        found: value.kind(),
                    })
                }
            }
        })?;
        // Every placeholder has its parameter; is there a parameter without
        // a placeholder? Python's `%` refuses a tuple too long as well.
        match params {
            Params::Positional(values) if values.len() > self.positional => {
                Err(BindError::Unused {
                    placeholders: self.positional,
                    parameters: values.len(),
                })
            }
            Params::Positional(_) | Params::Named(_) => Ok(bound),
        }
    }

    /// Whether `candidate` satisfies this with `params` for its
    /// placeholders: the sources' `match`.
    pub fn matches(
        &self,
        candidate: &dyn Context<Value>,
        params: &Params,
    ) -> Result<bool, MatchError> {
        Ok(is_satisfied_by(&self.bind(params)?, candidate)?)
    }
}

impl FromStr for Template {
    type Err = SyntaxError;

    fn from_str(source: &str) -> Result<Self, Self::Err> {
        Template::parse(source)
    }
}
