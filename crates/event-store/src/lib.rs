#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod codecs;
pub mod domain;
mod error;
#[cfg(feature = "pg")]
pub mod pg;
mod port;

pub use crate::codecs::{Codecs, Encrypted, Plain};
pub use crate::domain::{
    Appended, CausalDependency, Encoded, EncryptionCodec, EventCodec, EventMeta, JsonCodec,
    PayloadCodec, Recorded, StreamId, ZlibCodec,
};
pub use crate::error::{BoxError, Error};
#[cfg(feature = "pg")]
pub use crate::pg::PgEventStore;
pub use crate::port::EventStore;
