//! A specification built from typed terms: the sources' `public` package.
//!
//! The tree takes any operand anywhere: `and(value(1), value("x"))` is a
//! tree, and only evaluating it shows that it means nothing. A [`Term`]
//! knows what sort of value it stands for, so such a tree is not built:
//! only booleans are combined with `&`, `|`, `!`; only numbers are
//! multiplied; a number is compared with a number; `is_null` is offered
//! only by a term declared nullable.
//!
//! ```
//! use ascetic_ddd_specification::dsl::{Boolean, NullDatetime, Number, Text};
//!
//! let price = Number::field("price");
//! let discount = Number::field("discount");
//! let cheap = (price - discount).lt(Number::value(100));
//! let listed = Boolean::field("active") & NullDatetime::field("deleted_at").is_null();
//! let specification = (cheap & listed | Text::field("tag").eq(Text::value("sale"))).into_expr();
//! # let _ = specification;
//! ```
//!
//! What a sort does not have does not compile:
//!
//! ```compile_fail
//! # use ascetic_ddd_specification::dsl::{Number, Text};
//! let _ = Number::field("age") + Text::field("name"); // a text is not added
//! ```
//! ```compile_fail
//! # use ascetic_ddd_specification::dsl::Number;
//! let _ = Number::field("age") & Number::field("rank"); // numbers are not conjoined
//! ```
//! ```compile_fail
//! # use ascetic_ddd_specification::dsl::{Number, Text};
//! let _ = Number::field("age").eq(Text::value("18")); // nor compared with a text
//! ```
//! ```compile_fail
//! # use ascetic_ddd_specification::dsl::Number;
//! let _ = Number::field("age").is_null(); // `age` was not declared nullable
//! ```
//!
//! Python overloads the comparison operators as well. Rust's `==` and `<`
//! return `bool` and nothing else, so comparisons are methods here, as they
//! are in Go; the arithmetic and logical operators are overloaded.

use std::marker::PhantomData;
use std::ops::{Add, BitAnd, BitOr, Div, Mul, Neg, Not, Rem, Shl, Shr, Sub};

use super::ast::{self, Expr, Path};
use super::operator::Infix;
use super::value::{Interval, Timestamp, Value};

/// The sorts of value a term can stand for.
pub mod sort {
    /// A truth value.
    #[derive(Clone, Copy, Debug)]
    pub struct Boolean;
    /// A number, integer or float.
    #[derive(Clone, Copy, Debug)]
    pub struct Number;
    /// A text.
    #[derive(Clone, Copy, Debug)]
    pub struct Text;
    /// A point in time.
    #[derive(Clone, Copy, Debug)]
    pub struct Datetime;
    /// A span of time: what two points in time differ by.
    #[derive(Clone, Copy, Debug)]
    pub struct Timespan;

    /// A sort whose values can be compared with each other.
    pub trait Comparable {}
    impl Comparable for Number {}
    impl Comparable for Text {}
    impl Comparable for Datetime {}
    impl Comparable for Timespan {}

    /// `Self + R` is defined, and is of sort `Output`.
    pub trait Plus<R> {
        /// The sort of the sum.
        type Output;
    }
    impl Plus<Number> for Number {
        type Output = Number;
    }
    impl Plus<Timespan> for Datetime {
        type Output = Datetime;
    }
    impl Plus<Timespan> for Timespan {
        type Output = Timespan;
    }

    /// `Self - R` is defined, and is of sort `Output`.
    pub trait Minus<R> {
        /// The sort of the difference.
        type Output;
    }
    impl Minus<Number> for Number {
        type Output = Number;
    }
    impl Minus<Datetime> for Datetime {
        type Output = Timespan;
    }
    impl Minus<Timespan> for Datetime {
        type Output = Datetime;
    }
    impl Minus<Timespan> for Timespan {
        type Output = Timespan;
    }

    /// `-Self` is defined.
    pub trait Negatable {}
    impl Negatable for Number {}
    impl Negatable for Timespan {}

    /// Declared never null: the term has no `is_null`.
    #[derive(Clone, Copy, Debug)]
    pub struct NotNull;
    /// Declared nullable: the term has `is_null` and `is_not_null`.
    #[derive(Clone, Copy, Debug)]
    pub struct Nullable;
}

/// A Rust value that is a literal of sort `S`.
pub trait Literal<S>: Into<Value> {}

impl Literal<sort::Boolean> for bool {}
impl Literal<sort::Text> for &str {}
impl Literal<sort::Text> for String {}
impl Literal<sort::Datetime> for Timestamp {}
impl Literal<sort::Timespan> for Interval {}

macro_rules! number_literal {
    ($($number:ty),*) => {$( impl Literal<sort::Number> for $number {} )*};
}

number_literal!(i8, i16, i32, i64, u8, u16, u32, f32, f64);

/// A part of a specification that stands for a value of sort `S`, declared
/// nullable or not by `N`: the sources' `Delegating`.
#[derive(Clone, Debug)]
pub struct Term<S, N = sort::NotNull> {
    expr: Expr<Value>,
    sort: PhantomData<(S, N)>,
}

/// A boolean term.
pub type Boolean = Term<sort::Boolean>;
/// A boolean term that may be null.
pub type NullBoolean = Term<sort::Boolean, sort::Nullable>;
/// A numeric term.
pub type Number = Term<sort::Number>;
/// A numeric term that may be null.
pub type NullNumber = Term<sort::Number, sort::Nullable>;
/// A textual term.
pub type Text = Term<sort::Text>;
/// A textual term that may be null.
pub type NullText = Term<sort::Text, sort::Nullable>;
/// A point in time.
pub type Datetime = Term<sort::Datetime>;
/// A point in time that may be null.
pub type NullDatetime = Term<sort::Datetime, sort::Nullable>;
/// A span of time.
pub type Timespan = Term<sort::Timespan>;
/// A span of time that may be null.
pub type NullTimespan = Term<sort::Timespan, sort::Nullable>;

impl<S, N> Term<S, N> {
    /// The member at `path`, declared to be of this sort: the sources'
    /// `make_field`.
    pub fn field(path: impl Into<Path>) -> Self {
        Term::from_expr(ast::field(path))
    }

    /// `expr`, declared to be of this sort. Nothing checks the declaration:
    /// this is the way in for a tree built by other means — an
    /// [`any`](ast::any), say, which is a boolean.
    pub fn from_expr(expr: Expr<Value>) -> Self {
        Term {
            expr,
            sort: PhantomData,
        }
    }

    /// The tree built so far: the sources' `delegate`.
    pub fn expr(&self) -> &Expr<Value> {
        &self.expr
    }

    /// The tree built.
    pub fn into_expr(self) -> Expr<Value> {
        self.expr
    }

    fn infix<S2, N2, S3>(self, op: Infix, other: Term<S2, N2>) -> Term<S3> {
        Term::from_expr(ast::infix(self.expr, op, other.expr))
    }
}

impl<S> Term<S> {
    /// The constant `value`: the sources' `make_value`.
    pub fn value(value: impl Literal<S>) -> Self {
        Term::from_expr(ast::value(value))
    }
}

impl<S> Term<S, sort::Nullable> {
    /// The constant `value`, or the null.
    pub fn value(value: Option<impl Literal<S>>) -> Self {
        Term::from_expr(Expr::Value(value.map_or(Value::Null, Into::into)))
    }

    /// `self IS NULL`.
    pub fn is_null(self) -> Boolean {
        Term::from_expr(ast::is_null(self.expr))
    }

    /// `self IS NOT NULL`.
    pub fn is_not_null(self) -> Boolean {
        Term::from_expr(ast::is_not_null(self.expr))
    }
}

impl<N> Term<sort::Boolean, N> {
    /// `self IS other`: equality in which null is a value.
    pub fn is<N2>(self, other: Term<sort::Boolean, N2>) -> Boolean {
        self.infix(Infix::IS, other)
    }
}

impl<S: sort::Comparable, N> Term<S, N> {
    /// `self = other`.
    pub fn eq<N2>(self, other: Term<S, N2>) -> Boolean {
        self.infix(Infix::EQ, other)
    }

    /// `self != other`.
    pub fn ne<N2>(self, other: Term<S, N2>) -> Boolean {
        self.infix(Infix::NE, other)
    }

    /// `self > other`.
    pub fn gt<N2>(self, other: Term<S, N2>) -> Boolean {
        self.infix(Infix::GT, other)
    }

    /// `self < other`.
    pub fn lt<N2>(self, other: Term<S, N2>) -> Boolean {
        self.infix(Infix::LT, other)
    }

    /// `self >= other`.
    pub fn ge<N2>(self, other: Term<S, N2>) -> Boolean {
        self.infix(Infix::GE, other)
    }

    /// `self <= other`.
    pub fn le<N2>(self, other: Term<S, N2>) -> Boolean {
        self.infix(Infix::LE, other)
    }
}

impl<N> Not for Term<sort::Boolean, N> {
    type Output = Boolean;

    fn not(self) -> Boolean {
        Term::from_expr(ast::not(self.expr))
    }
}

impl<N, N2> BitAnd<Term<sort::Boolean, N2>> for Term<sort::Boolean, N> {
    type Output = Boolean;

    fn bitand(self, other: Term<sort::Boolean, N2>) -> Boolean {
        self.infix(Infix::AND, other)
    }
}

impl<N, N2> BitOr<Term<sort::Boolean, N2>> for Term<sort::Boolean, N> {
    type Output = Boolean;

    fn bitor(self, other: Term<sort::Boolean, N2>) -> Boolean {
        self.infix(Infix::OR, other)
    }
}

impl<S: sort::Negatable, N> Neg for Term<S, N> {
    type Output = Term<S>;

    fn neg(self) -> Term<S> {
        Term::from_expr(ast::neg(self.expr))
    }
}

impl<S: sort::Plus<S2>, N, S2, N2> Add<Term<S2, N2>> for Term<S, N> {
    type Output = Term<S::Output>;

    fn add(self, other: Term<S2, N2>) -> Self::Output {
        self.infix(Infix::ADD, other)
    }
}

impl<S: sort::Minus<S2>, N, S2, N2> Sub<Term<S2, N2>> for Term<S, N> {
    type Output = Term<S::Output>;

    fn sub(self, other: Term<S2, N2>) -> Self::Output {
        self.infix(Infix::SUB, other)
    }
}

macro_rules! number_operator {
    ($($operator:ident $method:ident $infix:ident),*) => {$(
        impl<N, N2> $operator<Term<sort::Number, N2>> for Term<sort::Number, N> {
            type Output = Number;

            fn $method(self, other: Term<sort::Number, N2>) -> Number {
                self.infix(Infix::$infix, other)
            }
        }
    )*};
}

number_operator!(Mul mul MUL, Div div DIV, Rem rem MOD, Shl shl SHL, Shr shr SHR);
