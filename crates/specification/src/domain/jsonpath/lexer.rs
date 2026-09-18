//! From the text of a template to its tokens.
//!
//! The sources try a list of regular expressions at each position. The
//! token set is small and fixed, so here each token is recognised by its
//! first character, which also lets a string have escapes and a number an
//! exponent, as RFC 9535 has them.

use super::error::SyntaxError;
use super::template::{Param, ParamKey, ParamKind};

#[derive(Clone, Debug, PartialEq)]
pub(super) enum Kind {
    Dollar,
    At,
    Dot,
    LeftBracket,
    RightBracket,
    LeftParen,
    RightParen,
    Question,
    Star,
    And,
    Or,
    Not,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    Int(i64),
    Float(f64),
    Text(String),
    Name(String),
    Placeholder(Param),
}

#[derive(Clone, Debug, PartialEq)]
pub(super) struct Token {
    pub kind: Kind,
    /// The index of the token's first character.
    pub position: usize,
}

const EXPECTED: &str = "valid token";

pub(super) fn tokenize(source: &str) -> Result<Vec<Token>, SyntaxError> {
    let chars: Vec<char> = source.chars().collect();
    let mut tokens = Vec::new();
    let mut position = 0;
    loop {
        position += blanks(&chars, position);
        let Some(first) = chars.get(position) else {
            return Ok(tokens);
        };
        let (kind, next) = token(*first, &chars, position, positional(&tokens))?;
        tokens.push(Token { kind, position });
        position = next;
    }
}

/// How many characters from `from` on satisfy `test`, none if `from` is
/// past the end.
fn run(chars: &[char], from: usize, test: impl Fn(char) -> bool) -> usize {
    chars
        .get(from..)
        .map_or(0, |rest| rest.iter().take_while(|c| test(**c)).count())
}

fn blanks(chars: &[char], from: usize) -> usize {
    run(chars, from, char::is_whitespace)
}

/// How many positional placeholders there are among `tokens`: while
/// tokenizing, the place of the next one; afterwards, how many parameters
/// the template takes.
pub(super) fn positional(tokens: &[Token]) -> usize {
    tokens
        .iter()
        .filter(|token| {
            matches!(
                &token.kind,
                Kind::Placeholder(Param {
                    key: ParamKey::Position(_),
                    ..
                })
            )
        })
        .count()
}

/// The token that starts with `first` at `at`, and where the next one
/// starts.
fn token(
    first: char,
    chars: &[char],
    at: usize,
    positional: usize,
) -> Result<(Kind, usize), SyntaxError> {
    let one = |kind| Ok((kind, at + 1));
    let two = |kind| Ok((kind, at + 2));
    match (first, chars.get(at + 1)) {
        ('$', _) => one(Kind::Dollar),
        ('@', _) => one(Kind::At),
        ('.', _) => one(Kind::Dot),
        ('[', _) => one(Kind::LeftBracket),
        (']', _) => one(Kind::RightBracket),
        ('(', _) => one(Kind::LeftParen),
        (')', _) => one(Kind::RightParen),
        ('?', _) => one(Kind::Question),
        ('*', _) => one(Kind::Star),
        ('&', Some('&')) => two(Kind::And),
        ('|', Some('|')) => two(Kind::Or),
        ('=', Some('=')) => two(Kind::Eq),
        ('!', Some('=')) => two(Kind::Ne),
        ('>', Some('=')) => two(Kind::Ge),
        ('<', Some('=')) => two(Kind::Le),
        ('!', _) => one(Kind::Not),
        ('>', _) => one(Kind::Gt),
        ('<', _) => one(Kind::Lt),
        ('%', _) => placeholder(chars, at, positional),
        ('\'' | '"', _) => text(chars, at, first),
        (c, _) if c.is_ascii_digit() => number(chars, at),
        ('-', Some(c)) if c.is_ascii_digit() => number(chars, at),
        (c, _) if is_name_start(c) => {
            let length = run(chars, at, is_name_part);
            let name = chars.iter().skip(at).take(length).collect();
            Ok((Kind::Name(name), at + length))
        }
        (c, _) => Err(SyntaxError::new(
            format!("Unexpected character '{c}'"),
            at,
            EXPECTED,
        )),
    }
}

fn is_name_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}

fn is_name_part(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// `%s`, `%d`, `%f`, or the same with a name: `%(age)d`.
fn placeholder(chars: &[char], at: usize, positional: usize) -> Result<(Kind, usize), SyntaxError> {
    let malformed = || SyntaxError::new("Malformed placeholder", at, "%s, %d, %f or %(name)s");
    let (key, letter_at) = if chars.get(at + 1) == Some(&'(') {
        let length = run(chars, at + 2, is_name_part);
        let name: String = chars.iter().skip(at + 2).take(length).collect();
        let close = at + 2 + length;
        if name.is_empty() || chars.get(close) != Some(&')') {
            return Err(malformed());
        }
        (ParamKey::Name(name), close + 1)
    } else {
        (ParamKey::Position(positional), at + 1)
    };
    let kind = match chars.get(letter_at) {
        Some('s') => ParamKind::Any,
        Some('d') => ParamKind::Integer,
        Some('f') => ParamKind::Number,
        _ => return Err(malformed()),
    };
    Ok((Kind::Placeholder(Param { key, kind }), letter_at + 1))
}

/// An integer, or — with a fraction or an exponent — a float.
fn number(chars: &[char], at: usize) -> Result<(Kind, usize), SyntaxError> {
    let digits = |from: usize| run(chars, from, |c| c.is_ascii_digit());
    let sign = usize::from(chars.get(at) == Some(&'-'));
    let whole_end = at + sign + digits(at + sign);
    let fraction_end = match (chars.get(whole_end), digits(whole_end + 1)) {
        (Some('.'), count) if count > 0 => whole_end + 1 + count,
        _ => whole_end,
    };
    let exponent_end = match chars.get(fraction_end) {
        Some('e' | 'E') => {
            let sign = usize::from(matches!(chars.get(fraction_end + 1), Some('+' | '-')));
            match digits(fraction_end + 1 + sign) {
                0 => fraction_end,
                count => fraction_end + 1 + sign + count,
            }
        }
        _ => fraction_end,
    };
    let literal: String = chars.iter().skip(at).take(exponent_end - at).collect();
    let out_of_range = || SyntaxError::new("Number out of range", at, "a number that fits");
    let kind = if exponent_end == whole_end {
        Kind::Int(literal.parse().map_err(|_| out_of_range())?)
    } else {
        // `1e999` parses, to infinity.
        let value: f64 = literal.parse().map_err(|_| out_of_range())?;
        Kind::Float(
            Some(value)
                .filter(|value| value.is_finite())
                .ok_or_else(out_of_range)?,
        )
    };
    Ok((kind, exponent_end))
}

/// A string in either quote, with the escapes of RFC 9535.
fn text(chars: &[char], at: usize, quote: char) -> Result<(Kind, usize), SyntaxError> {
    let unterminated = || SyntaxError::new("Unterminated string", at, "closing quote");
    let mut text = String::new();
    let mut position = at + 1;
    loop {
        match chars.get(position).ok_or_else(unterminated)? {
            c if *c == quote => return Ok((Kind::Text(text), position + 1)),
            '\\' => {
                let (c, next) = escape(chars, position)?;
                text.push(c);
                position = next;
            }
            c => {
                text.push(*c);
                position += 1;
            }
        }
    }
}

/// The character the escape at `at` stands for, and where the text goes on.
fn escape(chars: &[char], at: usize) -> Result<(char, usize), SyntaxError> {
    let invalid = || {
        SyntaxError::new(
            "Invalid escape",
            at,
            "\\\\, \\', \\\", \\/, \\b, \\f, \\n, \\r, \\t or \\uXXXX",
        )
    };
    let simple = |c| Ok((c, at + 2));
    match chars.get(at + 1).ok_or_else(invalid)? {
        '\\' => simple('\\'),
        '\'' => simple('\''),
        '"' => simple('"'),
        '/' => simple('/'),
        'b' => simple('\u{8}'),
        'f' => simple('\u{c}'),
        'n' => simple('\n'),
        'r' => simple('\r'),
        't' => simple('\t'),
        'u' => {
            let high = hex4(chars, at + 2).ok_or_else(invalid)?;
            // A code point beyond the basic plane is a pair of escapes.
            if (0xD800..0xDC00).contains(&high) {
                let low = match (chars.get(at + 6), chars.get(at + 7)) {
                    (Some('\\'), Some('u')) => hex4(chars, at + 8),
                    _ => None,
                }
                .filter(|low| (0xDC00..0xE000).contains(low))
                .ok_or_else(invalid)?;
                let code = 0x10000 + ((high - 0xD800) << 10) + (low - 0xDC00);
                char::from_u32(code)
                    .map(|c| (c, at + 12))
                    .ok_or_else(invalid)
            } else {
                char::from_u32(high)
                    .map(|c| (c, at + 6))
                    .ok_or_else(invalid)
            }
        }
        _ => Err(invalid()),
    }
}

fn hex4(chars: &[char], at: usize) -> Option<u32> {
    chars
        .get(at..at + 4)?
        .iter()
        .try_fold(0, |code, c| c.to_digit(16).map(|digit| code * 16 + digit))
}
