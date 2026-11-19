#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(clippy::all)]
// Every generated value is a function of the Plan and nothing else
// (ADR-0045 decision 3): the ambient sources and the float functions of
// unspecified precision listed in `clippy.toml` cannot be allowed back in by
// any module.
#![forbid(clippy::disallowed_methods, clippy::disallowed_types)]
// Debug and release builds agree: an overflow neither panics in one nor wraps
// in the other.
#![deny(clippy::arithmetic_side_effects)]
#![warn(missing_docs)]

pub mod apportion;
pub mod generator;
mod golden;
pub mod key;
pub mod law;
pub mod math;
pub mod permutation;
pub mod population;
pub mod testing;

pub use crate::apportion::{Apportionment, apportion};
pub use crate::key::{Encoder, Key, KeyPart, Namespace, Seed, Unit};
pub use crate::law::{IndexLaw, InvalidLaw, Law};
pub use crate::permutation::Permutation;
pub use crate::population::{Plan, Population, Ref};

/// The algorithm's version: the digest of the golden tables, the crate's
/// record of what it generates (ADR-0045 decision 8). A change to any generated
/// value changes the tables, and so this, in the same edit; a scenario's header
/// carries it, and a reader refuses another.
pub const ALGORITHM: &str = "ef4077eedd17cf26cf1fb4158ea9f242";
