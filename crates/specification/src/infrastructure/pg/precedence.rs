//! How tightly PostgreSQL's operators bind, and to which side: what decides
//! where the compiled text needs parentheses to mean what the tree means.
//!
//! The numbers are the rows of the table in the PostgreSQL manual,
//! "Operator Precedence", bottom up; only their order matters.

use crate::domain::operator::{Arithmetic, Infix, Logical, Postfix, Prefix};

/// To which side a run of operators of one precedence groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Associativity {
    /// `a - b - c` is `(a - b) - c`.
    Left,
    /// `a ^ b ^ c` would be `a ^ (b ^ c)`. No infix operator of a
    /// specification groups so; it is the side a left-grouping one leaves.
    Right,
    /// `a = b = c` is not PostgreSQL at all.
    Non,
}

/// Of what needs no parentheses wherever it stands: a column, a parameter,
/// `EXISTS (…)`.
pub(super) const ATOM: u8 = u8::MAX;

/// Of `::`, the row above every operator: what an operand is parenthesised
/// against before its type is said.
pub(super) const CAST: u8 = 160;

pub(super) fn prefix(op: Prefix) -> u8 {
    match op {
        Prefix::Neg => 140,
        Prefix::Not => 60,
    }
}

pub(super) fn infix(op: Infix) -> (u8, Associativity) {
    match op {
        Infix::Arithmetic(Arithmetic::Mul | Arithmetic::Div | Arithmetic::Mod) => {
            (120, Associativity::Left)
        }
        Infix::Arithmetic(Arithmetic::Add | Arithmetic::Sub) => (110, Associativity::Left),
        // "Any other operator."
        Infix::Arithmetic(Arithmetic::Shl | Arithmetic::Shr) => (100, Associativity::Left),
        Infix::Comparison(_) => (80, Associativity::Non),
        Infix::Is => (70, Associativity::Non),
        Infix::Logical(Logical::And) => (50, Associativity::Left),
        Infix::Logical(Logical::Or) => (40, Associativity::Left),
    }
}

pub(super) fn postfix(op: Postfix) -> u8 {
    match op {
        Postfix::IsNull | Postfix::IsNotNull => 70,
    }
}

/// Whether regrouping a run of `op` leaves its value what it was, so that
/// `a AND (b AND c)` can be written without the parentheses. True of the
/// logical connectives and of nothing else: integer `+` can overflow one way
/// and not the other, float `+` rounds differently.
pub(super) fn regroups(op: Infix) -> bool {
    matches!(op, Infix::Logical(_))
}
