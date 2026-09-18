//! The specification itself: the tree, the values, and the ways to build
//! and to evaluate it. Pure: no IO, no dependency, nothing of any storage.

pub mod ast;
pub mod dsl;
pub mod evaluate;
pub mod jsonpath;
pub mod operand;
pub mod operator;
pub mod record;
pub mod value;
