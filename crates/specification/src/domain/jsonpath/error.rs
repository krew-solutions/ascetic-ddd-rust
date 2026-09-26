//! What can go wrong with a template: reading it, binding it, matching it.

use std::fmt;

use super::template::{ParamKey, ParamKind};
use crate::domain::evaluate::EvalError;

/// A template is not in the grammar.
///
/// Prints as the sources' `JSONPathSyntaxError` does — the message, where,
/// what was expected, and the template with a caret under the place:
///
/// ```text
/// Unexpected character '#' at position 7 (expected valid token)
///   $[?@.a # 1]
///          ^
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyntaxError {
    /// What was found.
    pub message: String,
    /// Where: the index of the character, from zero.
    pub position: usize,
    /// What would have been accepted there.
    pub expected: &'static str,
    /// The template.
    pub expression: String,
}

impl SyntaxError {
    pub(super) fn new(message: impl Into<String>, position: usize, expected: &'static str) -> Self {
        SyntaxError {
            message: message.into(),
            position,
            expected,
            expression: String::new(),
        }
    }

    pub(super) fn within(self, expression: &str) -> Self {
        SyntaxError {
            expression: expression.to_owned(),
            ..self
        }
    }
}

impl fmt::Display for SyntaxError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // A template too long to read is not echoed.
        if self.expression.is_empty() {
            return write!(
                f,
                "{} at position {} (expected {})",
                self.message, self.position, self.expected
            );
        }
        // The template is echoed with a control character shown by its
        // escape, so that the message has none in it; the caret moves by
        // what the escapes add before the position.
        let echoed: String = self.expression.chars().map(shown).collect();
        let pointer: usize = self
            .expression
            .chars()
            .take(self.position)
            .map(|c| shown(c).chars().count())
            .sum();
        write!(
            f,
            "{} at position {} (expected {})\n  {}\n  {}^",
            self.message,
            self.position,
            self.expected,
            echoed,
            " ".repeat(pointer),
        )
    }
}

/// A character as an error shows it: a control character by its escape -
/// `\n`, `\t`, `\r`, or `\x00` and the like - not as it is.
pub(super) fn shown(c: char) -> String {
    match c {
        '\n' => "\\n".to_owned(),
        '\t' => "\\t".to_owned(),
        '\r' => "\\r".to_owned(),
        c if c.is_control() => format!("\\x{:02x}", c as u32),
        c => c.to_string(),
    }
}

impl std::error::Error for SyntaxError {}

/// The parameters do not fit the template's placeholders. The sources leave
/// an unbound placeholder in the tree, where it compares unequal to
/// everything; here a template is bound wholly or not at all.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BindError {
    /// No parameter for this placeholder.
    Missing(ParamKey),
    /// More positional parameters than placeholders.
    Unused {
        /// How many placeholders the template has.
        placeholders: usize,
        /// How many parameters were given.
        parameters: usize,
    },
    /// Positional parameters for named placeholders, or the reverse.
    WrongStyle,
    /// The parameter is not of the kind the placeholder's letter asks for.
    Mismatch {
        /// The placeholder.
        key: ParamKey,
        /// The kind it asks for.
        expected: ParamKind,
        /// The kind of the parameter given.
        found: &'static str,
    },
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::Missing(key) => write!(f, "no parameter for placeholder {key}"),
            BindError::Unused {
                placeholders,
                parameters,
            } => write!(f, "{parameters} parameters for {placeholders} placeholders"),
            BindError::WrongStyle => {
                f.write_str("positional parameters for named placeholders, or named for positional")
            }
            BindError::Mismatch {
                key,
                expected,
                found,
            } => write!(f, "placeholder {key} expects {expected}, got {found}"),
        }
    }
}

impl std::error::Error for BindError {}

/// A bound template could not be matched against a candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MatchError {
    /// The parameters do not fit.
    Bind(BindError),
    /// The specification could not be evaluated.
    Eval(EvalError),
}

impl fmt::Display for MatchError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MatchError::Bind(error) => error.fmt(f),
            MatchError::Eval(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for MatchError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            MatchError::Bind(error) => Some(error),
            MatchError::Eval(error) => Some(error),
        }
    }
}

impl From<BindError> for MatchError {
    fn from(error: BindError) -> Self {
        MatchError::Bind(error)
    }
}

impl From<EvalError> for MatchError {
    fn from(error: EvalError) -> Self {
        MatchError::Eval(error)
    }
}
