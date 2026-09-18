#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod domain;
mod error;
#[cfg(feature = "pg")]
pub mod pg;
mod port;
#[cfg(feature = "bus")]
pub mod stage;

pub use crate::domain::{Keyring, Resource, VersionedCipher};
pub use crate::error::Error;
#[cfg(feature = "pg")]
pub use crate::pg::PgDekStore;
pub use crate::port::DekStore;
#[cfg(feature = "bus")]
pub use crate::stage::EnvelopeStage;
