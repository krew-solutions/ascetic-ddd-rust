#![doc = include_str!("../README.md")]
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod cipher;
mod error;
pub mod key;
#[cfg(feature = "pg")]
pub mod pg;
mod port;
#[cfg(feature = "vault")]
pub mod vault;

pub use crate::cipher::{Aes256Gcm, Algorithm, Cipher, Key, Nonce};
pub use crate::error::{BoxError, Error};
pub use crate::key::{Ciphertext, Kek, MASTER_KEY_VERSION, MasterKey};
#[cfg(feature = "pg")]
pub use crate::pg::PgKeyManagementService;
pub use crate::port::KeyManagementService;
#[cfg(feature = "vault")]
pub use crate::vault::VaultTransitService;
