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
//! Beside the hierarchy, a tenant's other keys, one version of a keyring
//! each as the [`Kek`] is: a [`SigningKey`] of a [`SignatureAlgorithm`],
//! whose private half makes a [`Signature`] and whose [`PublicKey`]
//! verifies it; a [`MacKey`] for a purpose named by a [`KeyLabel`], making
//! a [`Mac`]. They take the primitives from the libraries as the cipher
//! does; what storage keeps is the private half, wrapped, and the name the
//! key is wrapped under is the key's own. The public halves of a key's
//! versions that verify, together, are a [`PublicKeyring`]: what a
//! verifier outside the KMS holds.
//!
//! The counterpart of the Python port's `models.py`, one file per type.

mod cipher;
mod kek;
mod key;
mod mac_key;
mod master_key;
mod public_keyring;
mod signature;
mod signing_key;

pub use self::cipher::{Aes256Gcm, Algorithm, Cipher};
pub use self::kek::Kek;
pub use self::key::{Key, VERSION_SIZE, WrappedKey};
pub use self::mac_key::MacKey;
pub use self::master_key::{MASTER_KEY_VERSION, MasterKey};
pub use self::public_keyring::PublicKeyring;
#[cfg(feature = "vault")]
pub(crate) use self::signature::is_key_part;
pub use self::signature::{KeyLabel, Mac, Signature, SignatureAlgorithm};
pub use self::signing_key::{PublicKey, SigningKey};
