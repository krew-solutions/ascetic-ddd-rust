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
pub mod decorate;
mod io;
pub mod live;
pub mod memory;
pub mod rest;
pub mod run;
pub mod sample;
pub mod sink;
pub mod source;
pub mod stages;
pub mod stamps;
pub mod stand;
pub mod tcp;
mod turn;
