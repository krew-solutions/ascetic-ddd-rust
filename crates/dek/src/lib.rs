#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod domain;
mod error;
#[cfg(feature = "pg")]
pub mod pg;
mod port;

pub use crate::domain::{Keyring, Resource, VersionedCipher};
pub use crate::error::Error;
#[cfg(feature = "pg")]
pub use crate::pg::PgDekStore;
pub use crate::port::DekStore;
