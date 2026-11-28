#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(clippy::all)]
// The machine is a function of its inputs (ADR-0047 decision 1): the clock and
// the ambient sources listed in `clippy.toml` cannot be allowed back in by any
// module.
#![forbid(clippy::disallowed_methods, clippy::disallowed_types)]
#![deny(clippy::arithmetic_side_effects)]
#![warn(missing_docs)]

pub mod fits;
pub mod frontier;
pub mod journal;
pub mod judge;
pub mod machine;
pub mod order;
pub mod pace;
pub mod recheck;
pub mod sim;
pub mod verdict;
