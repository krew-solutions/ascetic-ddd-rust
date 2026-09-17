//! The model: what this crate does, in one place.
//!
//! Three kinds of key and one operation between them. The [`MasterKey`]
//! wraps a tenant's key-encryption keys; a [`Kek`] wraps data-encryption
//! keys; a [`WrappedKey`] names the version of the key that wrapped it, so
//! that the right version is reached for when it is unwrapped. The tenant's
//! id is the associated data of every wrapping, so a key wrapped for one
//! tenant does not unwrap under another tenant's key, even one of the same
//! bytes. Under the hierarchy sits the primitive: a [`Cipher`] that seals
//! bytes under a [`Key`], chosen by its [`Algorithm`].
//!
//! The counterpart of the Python port's `models.py`, one file per type.

mod cipher;
mod kek;
mod key;
mod master_key;

pub use self::cipher::{Aes256Gcm, Algorithm, Cipher};
pub use self::kek::Kek;
pub use self::key::{Key, VERSION_SIZE, WrappedKey};
pub use self::master_key::{MASTER_KEY_VERSION, MasterKey};
