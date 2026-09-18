//! A specification written as a JSONPath filter with placeholders: parsed
//! once, bound to parameters for each use.
//!
//! ```
//! use ascetic_ddd_specification::jsonpath::{Params, Template};
//! use ascetic_ddd_specification::{Record, Value};
//!
//! let adults: Template = "$[?@.age >= %d && @.active == true]".parse()?;
//! let user = Record::<Value>::object([
//!     ("age", Record::value(30)),
//!     ("active", Record::value(true)),
//! ]);
//! assert!(adults.matches(&user, &Params::positional([18]))?);
//! assert!(!adults.matches(&user, &Params::positional([65]))?);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```
//!
//! # Grammar
//!
//! ```text
//! template    = "$" ( filter | path "[*]" filter )
//! path        = ( "." name )+
//! filter      = "[" "?" or "]"
//! or          = and ( "||" and )*
//! and         = unary ( "&&" unary )*
//! unary       = "!" unary | comparison
//! comparison  = operand ( ( "==" | "!=" | "<" | "<=" | ">" | ">=" ) operand )?
//! operand     = "(" or ")" | literal | placeholder | query
//! query       = ( "@" | "$" ) path ( "[*]" filter )?
//! literal     = integer | float | string | "true" | "false" | "null"
//! placeholder = "%s" | "%d" | "%f" | "%(" name ")" ( "s" | "d" | "f" )
//! name        = ( letter | "_" ) ( letter | digit | "_" )*
//! ```
//!
//! `$[?p]` is `p` of the candidate, and `@` in it is the candidate.
//! `$.a.items[*][?p]` is "some item of `a.items` satisfies `p`", and `@` in
//! `p` is the item; `$` is the candidate everywhere. A query with `[*]`
//! inside a filter nests the same. This is the language of the sources'
//! three parsers, and it is not RFC 9535's reading of the same text, where
//! a filter selects among the children of what precedes it: a specification
//! is a predicate, not a selection.
//!
//! Operators, literals, escapes and numbers are RFC 9535's. Beyond the
//! sources: either side of a comparison may be any operand, `$` may be used
//! inside a filter, strings have escapes and numbers an exponent.
//!
//! What the sources accept and this does not, each because the sources read
//! it as something other than what it says: a bracket or parenthesis left
//! open or closed twice; a filter on a path without `[*]`, whose path was
//! dropped and the filter applied to the candidate; a name without `@`;
//! positional and named placeholders in one template, which bound the wrong
//! parameters.

mod error;
mod lexer;
mod parser;
mod template;

pub use error::{BindError, MatchError, SyntaxError};
pub use template::{Param, ParamKey, ParamKind, Params, Slot, Template};
