#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(clippy::all)]
// Every planned time and every byte of a log is a function of the Plan
// (ADR-0046): the ambient sources and the float functions of unspecified
// precision listed in `clippy.toml` cannot be allowed back in by any module.
#![forbid(clippy::disallowed_methods, clippy::disallowed_types)]
#![deny(clippy::arithmetic_side_effects)]
#![warn(missing_docs)]

pub mod disk;
pub mod engine;
pub mod live;
pub mod log;
pub mod plan;
pub mod rate;
pub mod record;
pub mod render;
mod runs;
pub mod sample;
mod spill;
pub mod template;
pub mod validate;
pub mod window;
mod wire;

pub use crate::log::{
    Compression, FORMAT, FORMAT_VERSION, LogError, Reader, Writer, encode, open, open_file,
    write_file,
};
pub use crate::record::Line;
