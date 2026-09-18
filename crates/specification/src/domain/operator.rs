//! The operators of a specification.
//!
//! An operator is grouped by what it needs of its operands, because that is
//! how every reader of the tree tells them apart: a comparison needs values
//! that can be compared, arithmetic needs values that can be computed with,
//! a logical connective needs truth values and nothing of the values
//! themselves. The grouping is in the type, so a reader's `match` is total
//! without a catch-all arm.
//!
//! Precedence and associativity are not here. A tree needs neither — its
//! shape already says what applies to what. They belong to a notation: the
//! JSONPath parser has its own to read with, the PostgreSQL compiler its own
//! to write with.

use std::fmt;

/// An operator written before its operand.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Prefix {
    /// Logical negation, `NOT x`.
    Not,
    /// Arithmetic negation, `-x`.
    Neg,
}

/// An operator written after its operand.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Postfix {
    /// `x IS NULL`: true or false, never unknown.
    IsNull,
    /// `x IS NOT NULL`: true or false, never unknown.
    IsNotNull,
}

/// A comparison of two values: unknown if either is null.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Comparison {
    /// `=`
    Eq,
    /// `!=`
    Ne,
    /// `>`
    Gt,
    /// `<`
    Lt,
    /// `>=`
    Ge,
    /// `<=`
    Le,
}

/// A logical connective, in three-valued logic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Logical {
    /// `AND`: false if either side is false, even beside an unknown.
    And,
    /// `OR`: true if either side is true, even beside an unknown.
    Or,
}

/// A computation on two values: null if either is null.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Arithmetic {
    /// `+`
    Add,
    /// `-`
    Sub,
    /// `*`
    Mul,
    /// `/`
    Div,
    /// `%`
    Mod,
    /// `<<`, a shift of the bits of an integer.
    Shl,
    /// `>>`, a shift of the bits of an integer.
    Shr,
}

/// An operator written between its operands.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Infix {
    /// A comparison.
    Comparison(Comparison),
    /// Equality that treats null as a value: two nulls are equal, a null
    /// and a value are not. True or false, never unknown.
    Is,
    /// A logical connective.
    Logical(Logical),
    /// A computation.
    Arithmetic(Arithmetic),
}

impl Infix {
    /// `=`
    pub const EQ: Infix = Infix::Comparison(Comparison::Eq);
    /// `!=`
    pub const NE: Infix = Infix::Comparison(Comparison::Ne);
    /// `>`
    pub const GT: Infix = Infix::Comparison(Comparison::Gt);
    /// `<`
    pub const LT: Infix = Infix::Comparison(Comparison::Lt);
    /// `>=`
    pub const GE: Infix = Infix::Comparison(Comparison::Ge);
    /// `<=`
    pub const LE: Infix = Infix::Comparison(Comparison::Le);
    /// `IS`
    pub const IS: Infix = Infix::Is;
    /// `AND`
    pub const AND: Infix = Infix::Logical(Logical::And);
    /// `OR`
    pub const OR: Infix = Infix::Logical(Logical::Or);
    /// `+`
    pub const ADD: Infix = Infix::Arithmetic(Arithmetic::Add);
    /// `-`
    pub const SUB: Infix = Infix::Arithmetic(Arithmetic::Sub);
    /// `*`
    pub const MUL: Infix = Infix::Arithmetic(Arithmetic::Mul);
    /// `/`
    pub const DIV: Infix = Infix::Arithmetic(Arithmetic::Div);
    /// `%`
    pub const MOD: Infix = Infix::Arithmetic(Arithmetic::Mod);
    /// `<<`
    pub const SHL: Infix = Infix::Arithmetic(Arithmetic::Shl);
    /// `>>`
    pub const SHR: Infix = Infix::Arithmetic(Arithmetic::Shr);
}

impl fmt::Display for Prefix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Prefix::Not => "NOT",
            Prefix::Neg => "-",
        })
    }
}

impl fmt::Display for Postfix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Postfix::IsNull => "IS NULL",
            Postfix::IsNotNull => "IS NOT NULL",
        })
    }
}

impl fmt::Display for Comparison {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Comparison::Eq => "=",
            Comparison::Ne => "!=",
            Comparison::Gt => ">",
            Comparison::Lt => "<",
            Comparison::Ge => ">=",
            Comparison::Le => "<=",
        })
    }
}

impl fmt::Display for Logical {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Logical::And => "AND",
            Logical::Or => "OR",
        })
    }
}

impl fmt::Display for Arithmetic {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Arithmetic::Add => "+",
            Arithmetic::Sub => "-",
            Arithmetic::Mul => "*",
            Arithmetic::Div => "/",
            Arithmetic::Mod => "%",
            Arithmetic::Shl => "<<",
            Arithmetic::Shr => ">>",
        })
    }
}

impl fmt::Display for Infix {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Infix::Comparison(op) => op.fmt(f),
            Infix::Is => f.write_str("IS"),
            Infix::Logical(op) => op.fmt(f),
            Infix::Arithmetic(op) => op.fmt(f),
        }
    }
}
