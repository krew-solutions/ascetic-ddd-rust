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

/// How tall a template's tree may be: the levels of the longest way down
/// it. Every reader of a tree recurses, and so does dropping it, so an
/// unbounded tree from a text that is not trusted is a stack overflow — an
/// abort, which nothing catches; no specification a person wrote comes near.
const MAX_HEIGHT: usize = 128;

/// How deep the parser may go to read a template: through groups, `!` and
/// the filters of collections, which is where it recurses.
///
/// The two are counted apart, for they are not one number. How deep the
/// parser is comes down to a rule from the rule that called it. How tall a
/// tree is comes up from the trees below it, and grows wherever a node is
/// made — in the loop of a chain as well, where the parser does not recurse
/// at all, and above a left operand, which was read before anything knew it
/// would have an operator over it. One count used to stand for both, and was
/// passed to the right operands alone: a group at the left of a chain was as
/// deep as the parser was when it read it, however many operators came
/// after, and groups so nested multiplied — sixteen thousand levels passed
/// for a hundred and twenty-eight.
///
/// Nor do they cost the same. Measured on x86-64 with rustc 1.97, a level of
/// the parser takes 11 to 16 KiB of stack in a build that is not optimised
/// and 2 to 3 KiB in one that is; a level of a tree takes its readers up to
/// 8 KiB and up to 1.5 KiB. At these bounds a template is parsed, and its
/// tree read by everything that reads one, in under a megabyte of stack not
/// optimised — half of what a thread of Rust is given — and in about an
/// eighth of one optimised: `the_deepest_template_fits_a_megabyte_of_stack`.
const MAX_NESTING: usize = 32;

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

/// A tree, and how tall it is: the levels of the longest way down it.
struct Tree {
    expr: Expr<Slot>,
    height: usize,
}

impl Tree {
    fn leaf(expr: Expr<Slot>) -> Self {
        Tree { expr, height: 1 }
    }

    /// `make` of `operand`, a level taller than it. `at` is where the text
    /// asks for the node, for the error to point at if it may not be made.
    fn over(
        at: Input<'_>,
        make: impl FnOnce(Expr<Slot>) -> Expr<Slot>,
        operand: Tree,
    ) -> Result<Self, SyntaxError> {
        Ok(Tree {
            height: taller(at, operand.height)?,
            expr: make(operand.expr),
        })
    }

    /// `make` of `left` and `right`, a level taller than the taller of them.
    fn over_both(
        at: Input<'_>,
        make: fn(Expr<Slot>, Expr<Slot>) -> Expr<Slot>,
        left: Tree,
        right: Tree,
    ) -> Result<Self, SyntaxError> {
        Ok(Tree {
            height: taller(at, left.height.max(right.height))?,
            expr: make(left.expr, right.expr),
        })
    }
}

/// A level above `height`, if a tree may be that tall.
fn taller(at: Input<'_>, height: usize) -> Result<usize, SyntaxError> {
    if height < MAX_HEIGHT {
        Ok(height + 1)
    } else {
        Err(too_deep(at))
    }
}

/// A level below `depth`, if the parser may go that deep.
fn deeper(at: Input<'_>, depth: usize) -> Result<usize, SyntaxError> {
    if depth < MAX_NESTING {
        Ok(depth + 1)
    } else {
        Err(too_deep(at))
    }
}

fn too_deep(at: Input<'_>) -> SyntaxError {
    at.error("Expression is nested too deep", "a simpler expression")
}

/// `template = "$" ( filter | path "[*]" filter )`, and nothing after it.
pub(super) fn template(tokens: &[Token], end: usize) -> Result<Expr<Slot>, SyntaxError> {
    one_style(tokens)?;
    let input = Input { tokens, end }.expect(
        &Kind::Dollar,
        "Expected '$'",
        "a template starts at the root",
    )?;
    let (names, after) = names(input)?;
    let (tree, input) = match collection(Root::Global, names) {
        None => filter(after, Root::Global, 0)?,
        Some(source) => {
            let (predicate, rest) = filter(wildcard(after)?, Root::Item(0), 1)?;
            let any = Tree::over(after, |predicate| ast::any(source, predicate), predicate)?;
            (any, rest)
        }
    };
    match input.kind() {
        None => Ok(tree.expr),
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

/// `filter = "[" "?" or "]"`. `scope` is what `@` means inside, `depth` how
/// deep the parser is.
fn filter(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Tree> {
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
fn or(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Tree> {
    chain(input, &Kind::Or, ast::or, |input| and(input, scope, depth))
}

/// `and = unary ( "&&" unary )*`, nested to the left.
fn and(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Tree> {
    chain(input, &Kind::And, ast::and, |input| {
        unary(input, scope, depth)
    })
}

/// `operand ( separator operand )*`, nested to the left. A loop and not a
/// recursion, as the nesting is: the parser gets no deeper, and the tree a
/// level taller with each operand.
fn chain<'t>(
    input: Input<'t>,
    separator: &Kind,
    make: fn(Expr<Slot>, Expr<Slot>) -> Expr<Slot>,
    operand: impl Fn(Input<'t>) -> Parsed<'t, Tree>,
) -> Parsed<'t, Tree> {
    let (mut left, mut input) = operand(input)?;
    while input.kind() == Some(separator) {
        let (right, rest) = operand(input.advance())?;
        left = Tree::over_both(input, make, left, right)?;
        input = rest;
    }
    Ok((left, input))
}

/// `unary = "!" unary | comparison`
fn unary(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Tree> {
    if input.kind() == Some(&Kind::Not) {
        let (operand, rest) = unary(input.advance(), scope, deeper(input, depth)?)?;
        Ok((Tree::over(input, ast::not, operand)?, rest))
    } else {
        comparison(input, scope, depth)
    }
}

/// `comparison = operand ( ( "==" | "!=" | "<" | "<=" | ">" | ">=" ) operand )?`
/// — at most one: a comparison does not associate.
fn comparison(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Tree> {
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
    let (right, rest) = operand(input.advance(), scope, depth)?;
    Ok((Tree::over_both(input, make, left, right)?, rest))
}

/// `operand = "(" or ")" | literal | placeholder | query`
fn operand(input: Input<'_>, scope: Root, depth: usize) -> Parsed<'_, Tree> {
    let literal = |value: Value| {
        let leaf = Tree::leaf(Expr::Value(Slot::Literal(value)));
        Ok((leaf, input.advance()))
    };
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
            let leaf = Tree::leaf(Expr::Value(Slot::Param(param.clone())));
            Ok((leaf, input.advance()))
        }
        Some(Kind::At) => query(input.advance(), scope, depth),
        Some(Kind::Dollar) => query(input.advance(), Root::Global, depth),
        _ => Err(input.unexpected("value (number, string, boolean, null or placeholder) or query")),
    }
}

/// `query = ( "@" | "$" ) path ( "[*]" filter )?`, after its first token:
/// the value of a member, or a collection with a predicate on its items.
fn query(input: Input<'_>, root: Root, depth: usize) -> Parsed<'_, Tree> {
    let (names, input) = names(input)?;
    let path = collection(root, names)
        .ok_or_else(|| input.error("Expected field name", "after '@' or '$'"))?;
    if input.kind() == Some(&Kind::LeftBracket) {
        let (predicate, rest) = filter(wildcard(input)?, Root::Item(0), deeper(input, depth)?)?;
        let any = Tree::over(input, |predicate| ast::any(path, predicate), predicate)?;
        Ok((any, rest))
    } else {
        Ok((Tree::leaf(Expr::Field(path)), input))
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
