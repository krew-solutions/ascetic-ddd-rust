//! From the tokens of a template to its tree: one function per rule of the
//! grammar in the [module's](super) documentation, each taking the input
//! left and returning what it read with the input left after it.
//!
//! The sources carry a mutable context through the parser — whether `@` is
//! an item just now, which placeholder comes next — and step over a bracket
//! or a parenthesis where one "may be present", so that a template with
//! none, or with one too many, is read as if it were well formed. Here what
//! `@` means is an argument, the placeholders were numbered by the lexer,
//! and a token the grammar asks for is required.

use super::error::SyntaxError;
use super::lexer::{Kind, Token};
use super::template::{ParamKey, Slot};
use crate::domain::ast::{self, Expr, Path, Root};
use crate::domain::value::Value;

/// How deep a template's tree may be. Every reader of a tree recurses, so
/// an unbounded tree from a text that is not trusted is a stack overflow;
/// no specification a person wrote comes near.
const MAX_DEPTH: usize = 128;

/// The tokens not yet read, and where the template ends — the position of
/// an error met there.
#[derive(Clone, Copy)]
struct Input<'t> {
    tokens: &'t [Token],
    end: usize,
}

impl<'t> Input<'t> {
    fn kind(self) -> Option<&'t Kind> {
        self.tokens.first().map(|token| &token.kind)
    }

    fn position(self) -> usize {
        self.tokens.first().map_or(self.end, |token| token.position)
    }

    fn advance(self) -> Self {
        Input {
            tokens: self.tokens.get(1..).unwrap_or_default(),
            ..self
        }
    }

    /// The input after `kind`, which must come next.
    fn expect(
        self,
        kind: &Kind,
        message: &str,
        expected: &'static str,
    ) -> Result<Self, SyntaxError> {
        if self.kind() == Some(kind) {
            Ok(self.advance())
        } else {
            Err(self.error(message, expected))
        }
    }

    fn error(self, message: &str, expected: &'static str) -> SyntaxError {
        SyntaxError::new(message, self.position(), expected)
    }

    fn unexpected(self, expected: &'static str) -> SyntaxError {
        match self.kind() {
            Some(kind) => self.error(&format!("Unexpected token '{}'", spelling(kind)), expected),
            None => self.error("Unexpected end of expression", expected),
        }
    }
}

type Parsed<'t, T> = Result<(T, Input<'t>), SyntaxError>;

/// `template = "$" ( filter | path "[*]" filter )`, and nothing after it.
pub(super) fn template(tokens: &[Token], end: usize) -> Result<Expr<Slot>, SyntaxError> {
    one_style(tokens)?;
    let input = Input { tokens, end }.expect(
        &Kind::Dollar,
        "Expected '$'",
        "a template starts at the root",
    )?;
    let (names, input) = names(input)?;
    let (expr, input) = match collection(Root::Global, names) {
        None => filter(input, Root::Global, 0)?,
        Some(source) => {
            let (predicate, input) = filter(wildcard(input)?, Root::Item, 1)?;
            (ast::any(source, predicate), input)
        }
    };
    match input.kind() {
        None => Ok(expr),
        Some(_) => Err(input.unexpected("end of expression")),
    }
}

/// Python's `%` takes a tuple or a mapping, never both; a template that
/// mixes the two styles has no parameters it could be bound to.
fn one_style(tokens: &[Token]) -> Result<(), SyntaxError> {
    let named = |token: &&Token| matches!(&token.kind, Kind::Placeholder(param) if matches!(param.key, ParamKey::Name(_)));
    let positional = |token: &&Token| matches!(&token.kind, Kind::Placeholder(param) if matches!(param.key, ParamKey::Position(_)));
    match (tokens.iter().find(named), tokens.iter().find(positional)) {
        (Some(first), Some(second)) => Err(SyntaxError::new(
            "Positional and named placeholders in one template",
            first.position.max(second.position),
            "placeholders of one style",
        )),
        _ => Ok(()),
    }
}

/// `path = ( "." name )*`
fn names(input: Input<'_>) -> Parsed<'_, Vec<String>> {
    let mut names = Vec::new();
    let mut input = input;
    while input.kind() == Some(&Kind::Dot) {
        match input.advance().kind() {
            Some(Kind::Name(name)) => {
                names.push(name.clone());
                input = input.advance().advance();
            }
            _ => return Err(input.advance().error("Expected field name", "after '.'")),
        }
    }
    Ok((names, input))
}

/// The path of `names` from `root`, if there are any.
fn collection(root: Root, names: Vec<String>) -> Option<Path> {
    let mut names = names.into_iter();
    let first = names.next()?;
    Some(names.fold(Path::new(root, first), Path::child))
}

/// `"[*]"`
fn wildcard(input: Input<'_>) -> Result<Input<'_>, SyntaxError> {
    let expected = "after path";
    input
        .expect(&Kind::LeftBracket, "Expected wildcard '[*]'", expected)?
        .expect(&Kind::Star, "Expected wildcard '[*]'", expected)?
        .expect(&Kind::RightBracket, "Expected wildcard '[*]'", expected)
}

/// `filter = "[" "?" or "]"`. `scope` is what `@` means inside.
fn filter(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Expr<Slot>> {
    let message = "Expected filter expression '[?...]'";
    let input = input.expect(&Kind::LeftBracket, message, "'['")?.expect(
        &Kind::Question,
        message,
        "'?'",
    )?;
    let (predicate, input) = or(input, scope, depth)?;
    let input = input.expect(
        &Kind::RightBracket,
        "Expected ']'",
        "end of filter expression",
    )?;
    Ok((predicate, input))
}

/// `or = and ( "||" and )*`, nested to the left.
fn or(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Expr<Slot>> {
    chain(input, depth, &Kind::Or, ast::or, |input, depth| {
        and(input, scope, depth)
    })
}

/// `and = unary ( "&&" unary )*`, nested to the left.
fn and(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Expr<Slot>> {
    chain(input, depth, &Kind::And, ast::and, |input, depth| {
        unary(input, scope, depth)
    })
}

/// `operand ( separator operand )*`, nested to the left. A loop and not a
/// recursion, as the nesting is: the tree gets a level deeper with each
/// operand, and the levels count towards [`MAX_DEPTH`].
fn chain<'t>(
    input: Input<'t>,
    depth: usize,
    separator: &Kind,
    make: fn(Expr<Slot>, Expr<Slot>) -> Expr<Slot>,
    operand: impl Fn(Input<'t>, usize) -> Parsed<'t, Expr<Slot>>,
) -> Parsed<'t, Expr<Slot>> {
    let (mut left, mut input) = operand(input, depth)?;
    let mut depth = depth;
    while input.kind() == Some(separator) {
        depth = deeper(input, depth)?;
        let (right, rest) = operand(input.advance(), depth)?;
        left = make(left, right);
        input = rest;
    }
    Ok((left, input))
}

fn deeper(input: Input<'_>, depth: usize) -> Result<usize, SyntaxError> {
    if depth < MAX_DEPTH {
        Ok(depth + 1)
    } else {
        Err(input.error("Expression is nested too deep", "a simpler expression"))
    }
}

/// `unary = "!" unary | comparison`
fn unary(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Expr<Slot>> {
    if input.kind() == Some(&Kind::Not) {
        let (operand, input) = unary(input.advance(), scope, deeper(input, depth)?)?;
        Ok((ast::not(operand), input))
    } else {
        comparison(input, scope, depth)
    }
}

/// `comparison = operand ( ( "==" | "!=" | "<" | "<=" | ">" | ">=" ) operand )?`
/// — at most one: a comparison does not associate.
fn comparison(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Expr<Slot>> {
    let (left, input) = operand(input, scope, depth)?;
    let make: fn(Expr<Slot>, Expr<Slot>) -> Expr<Slot> = match input.kind() {
        Some(Kind::Eq) => ast::equal,
        Some(Kind::Ne) => ast::not_equal,
        Some(Kind::Gt) => ast::greater_than,
        Some(Kind::Ge) => ast::greater_than_equal,
        Some(Kind::Lt) => ast::less_than,
        Some(Kind::Le) => ast::less_than_equal,
        _ => return Ok((left, input)),
    };
    let (right, input) = operand(input.advance(), scope, deeper(input, depth)?)?;
    Ok((make(left, right), input))
}

/// `operand = "(" or ")" | literal | placeholder | query`
fn operand(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Expr<Slot>> {
    let literal = |value: Value| Ok((Expr::Value(Slot::Literal(value)), input.advance()));
    match input.kind() {
        Some(Kind::LeftParen) => {
            let (inner, rest) = or(input.advance(), scope, deeper(input, depth)?)?;
            let rest = rest.expect(&Kind::RightParen, "Expected ')'", "closing parenthesis")?;
            Ok((inner, rest))
        }
        Some(Kind::Int(value)) => literal(Value::Int(*value)),
        Some(Kind::Float(value)) => literal(Value::Float(*value)),
        Some(Kind::Text(value)) => literal(Value::Text(value.clone())),
        Some(Kind::Name(name)) if name.eq_ignore_ascii_case("true") => literal(Value::Bool(true)),
        Some(Kind::Name(name)) if name.eq_ignore_ascii_case("false") => literal(Value::Bool(false)),
        Some(Kind::Name(name)) if name.eq_ignore_ascii_case("null") => literal(Value::Null),
        Some(Kind::Placeholder(param)) => {
            Ok((Expr::Value(Slot::Param(param.clone())), input.advance()))
        }
        Some(Kind::At) => query(input.advance(), scope, depth),
        Some(Kind::Dollar) => query(input.advance(), Root::Global, depth),
        _ => Err(input.unexpected("value (number, string, boolean, null or placeholder) or query")),
    }
}

/// `query = ( "@" | "$" ) path ( "[*]" filter )?`, after its first token:
/// the value of a member, or a collection with a predicate on its items.
fn query(input: Input<'_>, root: Root, depth: usize) -> Parsed<'_, Expr<Slot>> {
    let (names, input) = names(input)?;
    let path = collection(root, names)
        .ok_or_else(|| input.error("Expected field name", "after '@' or '$'"))?;
    if input.kind() == Some(&Kind::LeftBracket) {
        let (predicate, input) = filter(wildcard(input)?, Root::Item, deeper(input, depth)?)?;
        Ok((ast::any(path, predicate), input))
    } else {
        Ok((Expr::Field(path), input))
    }
}

/// A token as a template spells it, for an error to show.
fn spelling(kind: &Kind) -> String {
    match kind {
        Kind::Dollar => "$".to_owned(),
        Kind::At => "@".to_owned(),
        Kind::Dot => ".".to_owned(),
        Kind::LeftBracket => "[".to_owned(),
        Kind::RightBracket => "]".to_owned(),
        Kind::LeftParen => "(".to_owned(),
        Kind::RightParen => ")".to_owned(),
        Kind::Question => "?".to_owned(),
        Kind::Star => "*".to_owned(),
        Kind::And => "&&".to_owned(),
        Kind::Or => "||".to_owned(),
        Kind::Not => "!".to_owned(),
        Kind::Eq => "==".to_owned(),
        Kind::Ne => "!=".to_owned(),
        Kind::Gt => ">".to_owned(),
        Kind::Ge => ">=".to_owned(),
        Kind::Lt => "<".to_owned(),
        Kind::Le => "<=".to_owned(),
        Kind::Int(value) => value.to_string(),
        Kind::Float(value) => value.to_string(),
        Kind::Text(value) => format!("\"{value}\""),
        Kind::Name(name) => name.clone(),
        Kind::Placeholder(param) => match &param.key {
            ParamKey::Position(_) => "%".to_owned(),
            ParamKey::Name(name) => format!("%({name})"),
        },
    }
}
