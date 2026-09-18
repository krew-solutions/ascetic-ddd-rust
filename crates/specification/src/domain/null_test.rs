//! Equality with the null, for the notations in which null is a value.
//!
//! In the tree a comparison with null is null, as in SQL: `a = NULL` is true
//! of nothing, and that is what keeps the evaluator and the database agreed.
//! But a JSONPath template finds a null by `@.a == null`, and a Rust function
//! by `u.a == None`: written into the tree word for word they would find
//! none, in either reader, and say nothing of it. What those notations mean
//! by it is `IS NULL`, so that is what their frontends build — the rule of
//! SQLAlchemy's `column == None`.
//!
//! The rule is of constants, not of data: `a = b` with both members null
//! stays null. The equality in which null is a value is [`ast::is`].
//!
//! The tree built by hand, [`ast::equal`], says what it says.
//!
//! [`ast::is`]: super::ast::is
//! [`ast::equal`]: super::ast::equal

use super::ast::{self, Expr};
use super::operand::Operand;
use super::operator::Infix;

/// `left = right`; or, if either is the null constant, `IS NULL` of the
/// other.
pub fn equal<V: Operand>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    test_or_compare(left, right, ast::is_null, ast::equal)
}

/// `left != right`; or, if either is the null constant, `IS NOT NULL` of
/// the other.
pub fn not_equal<V: Operand>(left: Expr<V>, right: Expr<V>) -> Expr<V> {
    test_or_compare(left, right, ast::is_not_null, ast::not_equal)
}

/// `expr` with every equality in it read by [`equal`] and [`not_equal`]: for
/// a tree whose constants were not known when it was built, as those of a
/// template are not until it is bound.
pub fn throughout<V: Operand>(expr: Expr<V>) -> Expr<V> {
    match expr {
        Expr::Infix(left, Infix::EQ, right) => equal(throughout(*left), throughout(*right)),
        Expr::Infix(left, Infix::NE, right) => not_equal(throughout(*left), throughout(*right)),
        Expr::Infix(left, op, right) => ast::infix(throughout(*left), op, throughout(*right)),
        Expr::Prefix(op, operand) => Expr::Prefix(op, Box::new(throughout(*operand))),
        Expr::Postfix(operand, op) => Expr::Postfix(Box::new(throughout(*operand)), op),
        Expr::Any(source, predicate) => Expr::Any(source, Box::new(throughout(*predicate))),
        leaf @ (Expr::Value(_) | Expr::Field(_)) => leaf,
    }
}

/// `test` of the operand that stands beside the null constant, or `compare`
/// of the two if neither is one. Of two nulls the left is tested, and is
/// null: true, as `null == null` is in the notations this is for.
fn test_or_compare<V: Operand>(
    left: Expr<V>,
    right: Expr<V>,
    test: fn(Expr<V>) -> Expr<V>,
    compare: fn(Expr<V>, Expr<V>) -> Expr<V>,
) -> Expr<V> {
    let is_null = |expr: &Expr<V>| matches!(expr, Expr::Value(value) if value.is_null());
    if is_null(&right) {
        test(left)
    } else if is_null(&left) {
        test(right)
    } else {
        compare(left, right)
    }
}
