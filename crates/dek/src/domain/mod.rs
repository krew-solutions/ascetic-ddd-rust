//! The model: what this crate does, in one place.
//!
//! A data-encryption key belongs to a [`Resource`] — one thing of one
//! tenant, named by its kind and its id: an aggregate's stream in an event
//! store, a document, whatever a repository keeps under a key of its own.
//! The resource's DEKs are versioned, and a codec sees them as ciphers over
//! bytes that carry the version along: a [`VersionedCipher`] is one version,
//! for writing; a [`Keyring`] is every version the resource has, for reading
//! back what any of them sealed. The keys themselves rest wrapped by the
//! tenant's key-encryption key, through the KMS, in the store.
//!
//! The counterpart of the `_VersionedCipher` and `_CompositeVersionedCipher`
//! of the Python port's `dek_store.py`, with the resource named.

pub(crate) mod canonical;
mod resource;
mod versioned;

pub use self::resource::Resource;
pub use self::versioned::{Keyring, VersionedCipher};
