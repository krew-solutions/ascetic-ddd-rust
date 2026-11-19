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
pub mod key;
pub mod law;
pub mod math;
pub mod permutation;
pub mod testing;

pub use crate::apportion::{Apportionment, apportion};
pub use crate::key::{Encoder, Key, KeyPart, Namespace, Seed, Unit};
pub use crate::law::{IndexLaw, InvalidLaw, Law};
pub use crate::permutation::Permutation;
