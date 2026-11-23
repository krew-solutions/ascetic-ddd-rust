#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(clippy::all)]
// One clock (ADR-0047 decision 16): the clocks `clippy.toml` refuses are read
// by the `clock` module alone, the one module that allows them
// (`tests/lint.rs`).
#![deny(clippy::disallowed_methods, clippy::disallowed_types)]
#![warn(missing_docs)]

pub mod carrier;
pub mod clock;
mod io;
pub mod rest;
pub mod run;
pub mod sink;
pub mod source;
pub mod stamps;
pub mod stand;
mod turn;
