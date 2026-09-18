#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod domain;
pub mod infrastructure;

pub use crate::domain::ast::{self, Expr, Path, Root};
pub use crate::domain::evaluate::{Context, ContextError, EvalError, evaluate, is_satisfied_by};
pub use crate::domain::operand::{Operand, OperandError};
pub use crate::domain::operator::{Arithmetic, Comparison, Infix, Logical, Postfix, Prefix};
pub use crate::domain::record::Record;
pub use crate::domain::value::{Interval, Timestamp, Value};
pub use crate::domain::{dsl, jsonpath, null_test};
pub use crate::infrastructure::pg;
pub use crate::infrastructure::transform::{Mapped, Mapping, TransformError, transform};

#[cfg(feature = "macros")]
pub use ascetic_ddd_specification_macros::specification;
