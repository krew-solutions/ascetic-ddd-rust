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
#[cfg(feature = "software")]
mod software;
#[cfg(feature = "testing")]
pub mod testing;
#[cfg(feature = "vault")]
pub mod vault;

pub use crate::cache::Cached;
pub use crate::domain::{
    Aes256Gcm, Algorithm, Cipher, Kek, Key, KeyLabel, MASTER_KEY_VERSION, Mac, MasterKey,
    Signature, SignatureAlgorithm, WrappedKey,
};
pub use crate::error::{BoxError, Error};
#[cfg(feature = "pg")]
pub use crate::pg::PgKeyManagementService;
pub use crate::port::{KeyManagementService, MacService, SigningService};
#[cfg(feature = "vault")]
pub use crate::vault::VaultTransitService;
