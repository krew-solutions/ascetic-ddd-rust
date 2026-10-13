//! The key-encryption key: one version of a tenant's key, wrapping its DEKs.
//!
//! A KEK wraps under associated data that binds the wrap to the tenant —
//! and, where the key serves one derivation context, to that context too,
//! so a DEK wrapped for one channel does not unwrap as another's: the
//! property Vault's derived keys give by deriving a key per context, given
//! here by the binding of the wrap (ADR-0017).

use std::fmt;

use super::cipher::{Algorithm, Cipher};
use super::key::{Key, WrappedKey};
use crate::error::Error;

/// One version of a tenant's key-encryption key, with the form the master
/// key wrapped it in, which is what storage keeps.
pub struct Kek {
    tenant_id: String,
    context: Option<String>,
    version: u32,
    algorithm: Algorithm,
    cipher: Box<dyn Cipher>,
    wrapped: WrappedKey,
}

impl Kek {
    /// The KEK `key` of `tenant_id`, version `version`, for `algorithm`,
    /// which the master key wrapped as `wrapped`; serving `context` when
    /// one is given, so that what it wraps is bound to it. The bytes are
    /// not kept: the cipher holds what it needs and wipes it when dropped.
    pub(super) fn new(
        tenant_id: &str,
        context: Option<&str>,
        version: u32,
        algorithm: Algorithm,
        key: &Key,
        wrapped: WrappedKey,
    ) -> Result<Self, Error> {
        Ok(Kek {
            tenant_id: tenant_id.to_owned(),
            context: context.map(str::to_owned),
            version,
            algorithm,
            cipher: algorithm.cipher(key, binding(tenant_id, context))?,
            wrapped,
        })
    }

    /// The tenant this key belongs to.
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }

    /// The derivation context this key serves, if one.
    pub fn context(&self) -> Option<&str> {
        self.context.as_deref()
    }

    /// The version, from 1.
    pub fn version(&self) -> u32 {
        self.version
    }

    /// The algorithm of the key, and so of every DEK it makes.
    pub fn algorithm(&self) -> Algorithm {
        self.algorithm
    }

    /// The key as the master key wrapped it.
    pub fn wrapped(&self) -> &WrappedKey {
        &self.wrapped
    }

    /// Wraps `dek`, naming this version.
    pub fn wrap(&self, dek: &Key) -> Result<WrappedKey, Error> {
        WrappedKey::wrap(self.version, &*self.cipher, dek)
    }

    /// Unwraps what [`Kek::wrap`] wrapped. A key wrapped by another version
    /// is [`Error::WrongKeyVersion`] before the cipher is tried.
    pub fn unwrap(&self, wrapped: &WrappedKey) -> Result<Key, Error> {
        wrapped.unwrap(self.version, &*self.cipher)
    }

    /// Wraps again, under this version, what `from` — an earlier version of
    /// the tenant's key — wrapped: what a DEK goes through after a rotation.
    pub fn rewrap(&self, wrapped: &WrappedKey, from: &Kek) -> Result<WrappedKey, Error> {
        self.wrap(&from.unwrap(wrapped)?)
    }

    /// A fresh DEK of this KEK's algorithm — made by its cipher, so of its
    /// kind — in the clear and wrapped by this KEK.
    pub fn generate_dek(&self) -> Result<(Key, WrappedKey), Error> {
        let dek = self.cipher.generate_key()?;
        let wrapped = self.wrap(&dek)?;
        Ok((dek, wrapped))
    }
}

/// The associated data a KEK wraps under: the tenant's id alone, as it
/// always was, or — for a key serving a derivation context — the tenant's
/// id, length-prefixed, followed by the context, so that neither can run
/// into the other.
pub(super) fn binding(tenant_id: &str, context: Option<&str>) -> Vec<u8> {
    match context {
        None => tenant_id.as_bytes().to_vec(),
        Some(context) => {
            let length = u32::try_from(tenant_id.len()).unwrap_or(u32::MAX);
            let mut bound = Vec::with_capacity(4 + tenant_id.len() + context.len());
            bound.extend_from_slice(&length.to_be_bytes());
            bound.extend_from_slice(tenant_id.as_bytes());
            bound.extend_from_slice(context.as_bytes());
            bound
        }
    }
}

impl fmt::Debug for Kek {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Kek")
            .field("tenant_id", &self.tenant_id)
            .field("context", &self.context)
            .field("version", &self.version)
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}
