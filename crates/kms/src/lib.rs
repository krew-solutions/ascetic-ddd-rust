#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod cache;
pub mod domain;
mod error;
#[cfg(feature = "pg")]
pub mod pg;
mod port;
#[cfg(feature = "vault")]
pub mod vault;

pub use crate::cache::Cached;
pub use crate::domain::{
    Aes256Gcm, Algorithm, Cipher, Kek, Key, MASTER_KEY_VERSION, MasterKey, WrappedKey,
};
pub use crate::error::{BoxError, Error};
#[cfg(feature = "pg")]
pub use crate::pg::PgKeyManagementService;
pub use crate::port::KeyManagementService;
#[cfg(feature = "vault")]
pub use crate::vault::VaultTransitService;
