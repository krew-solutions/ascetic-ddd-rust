//! The port: what a repository sees.

use ascetic_ddd_session::Session;

use crate::domain::{Keyring, Resource, VersionedCipher};
use crate::error::Error;

/// A resource's data-encryption keys, as ciphers, in the caller's session.
///
/// The write path takes [`DekStore::get_or_create`]: the current version, made
/// on first contact. The read path takes [`DekStore::get_all`]: every
/// version, so that what any of them sealed opens. [`DekStore::get`] is for
/// a version already known — read from a ciphertext. After the tenant's KEK
/// rotates, [`DekStore::rewrap`] wraps the tenant's DEKs again under the
/// new version; [`DekStore::delete`] forgets one resource for good.
pub trait DekStore<S: Session>: Send + Sync {
    /// The resource's current DEK, made as version 1 if the resource has
    /// none.
    fn get_or_create(
        &self,
        session: &S,
        resource: &Resource,
    ) -> impl Future<Output = Result<VersionedCipher, Error>> + Send;

    /// The resource's DEK of `version`; [`Error::DekNotFound`] if there is
    /// none.
    fn get(
        &self,
        session: &S,
        resource: &Resource,
        version: u32,
    ) -> impl Future<Output = Result<VersionedCipher, Error>> + Send;

    /// Every version of the resource's DEK; [`Error::DekNotFound`] if there
    /// is none.
    fn get_all(
        &self,
        session: &S,
        resource: &Resource,
    ) -> impl Future<Output = Result<Keyring, Error>> + Send;

    /// Wraps every DEK of the tenant again, under the tenant's current KEK;
    /// how many.
    fn rewrap(
        &self,
        session: &S,
        tenant_id: &str,
    ) -> impl Future<Output = Result<u64, Error>> + Send;

    /// Deletes every version of the resource's DEK: nothing they sealed
    /// opens again.
    fn delete(
        &self,
        session: &S,
        resource: &Resource,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}
