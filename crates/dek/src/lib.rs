#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod domain;
#[cfg(feature = "bus")]
mod draws;
mod error;
#[cfg(feature = "pg")]
pub mod pg;
mod port;
#[cfg(feature = "bus")]
pub mod sealing;
#[cfg(feature = "bus")]
pub mod stage;
pub mod supply;

pub use crate::supply::InvalidParameters;

pub use crate::domain::{Keyring, Resource, VersionedCipher};
#[cfg(feature = "bus")]
pub use crate::draws::{Flight, Held, Landed, SealingObserver, Unavailable};
pub use crate::error::Error;
#[cfg(feature = "pg")]
pub use crate::pg::PgDekStore;
pub use crate::port::DekStore;
#[cfg(feature = "bus")]
pub use crate::sealing::{ByDestination, Sealing, SealingParameters, UnsealedChannel};
#[cfg(feature = "bus")]
pub use crate::stage::{EnvelopeStage, Pacing, Reuse};
