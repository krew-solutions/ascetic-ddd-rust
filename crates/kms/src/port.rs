//! The port: what the application layer sees.

use ascetic_ddd_session::Session;

use crate::domain::{Key, KeyLabel, Mac, Signature, SignatureAlgorithm};
use crate::error::Error;

/// Envelope encryption for one tenant at a time: data-encryption keys are
/// wrapped and unwrapped by the tenant's current key-encryption key, which
/// the service makes on first contact, rotates on request, and deletes for
/// crypto-shredding. The surface of Vault Transit; the PostgreSQL adapter
/// keeps the same surface.
///
/// A wrapped key is bytes opaque to the caller, in the adapter's own form:
/// only the adapter that wrapped it unwraps it.
pub trait KeyManagementService<S: Session>: Send + Sync {
    /// Wraps `dek` under the tenant's current KEK, making one if the tenant
    /// has none.
    fn encrypt_dek(
        &self,
        session: &S,
        tenant_id: &str,
        dek: &Key,
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send;

    /// Unwraps `encrypted_dek` with the KEK version it names.
    fn decrypt_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> impl Future<Output = Result<Key, Error>> + Send;

    /// A fresh DEK, in the clear and wrapped under the tenant's current KEK,
    /// making one if the tenant has none.
    fn generate_dek(
        &self,
        session: &S,
        tenant_id: &str,
    ) -> impl Future<Output = Result<(Key, Vec<u8>), Error>> + Send;

    /// The next version of the tenant's KEK, or the first; returns the new
    /// version. Earlier versions stay, so what they wrapped still unwraps.
    fn rotate_kek(
        &self,
        session: &S,
        tenant_id: &str,
    ) -> impl Future<Output = Result<u32, Error>> + Send;

    /// `encrypted_dek` wrapped again under the current KEK, after a rotation.
    fn rewrap_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send;

    /// Deletes every version of the tenant's KEK: nothing wrapped under them
    /// unwraps again. Nothing to delete is not an error.
    fn delete_kek(
        &self,
        session: &S,
        tenant_id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Digital signatures under a tenant's keys: one key per tenant and
/// algorithm, made on first signing, versioned by rotation. The latest
/// version signs; every version verifies, until an operator retires it.
/// The algorithm is an input of signing and a property of the signature,
/// so a tenant moves to another algorithm by signing with it — a new key,
/// not a rotation — while what the old key signed keeps verifying.
///
/// Deleting a key ends the verifiability of everything it ever signed: the
/// tenant's off-boarding, never a rotation.
pub trait SigningService<S: Session>: Send + Sync {
    /// Signs `message` with the latest version of the tenant's key of
    /// `algorithm`, making the key if the tenant has none.
    fn sign(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        message: &[u8],
    ) -> impl Future<Output = Result<Signature, Error>> + Send;

    /// Verifies `signature` over `message` under the key version it names.
    /// `Ok(())` when it verifies, [`Error::InvalidSignature`] when it does
    /// not; never makes a key — a signature by a key the tenant has not got
    /// is [`Error::SigningKeyNotFound`], by a version the key has not got,
    /// or no longer serves, [`Error::NoKeyOfVersion`].
    fn verify(
        &self,
        session: &S,
        tenant_id: &str,
        message: &[u8],
        signature: &Signature,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Makes the next version of the tenant's key of `algorithm`, or its
    /// first; answers the version. Earlier versions keep verifying.
    fn rotate_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> impl Future<Output = Result<u32, Error>> + Send;

    /// Deletes every version of the tenant's key of `algorithm`; nothing to
    /// delete is not an error.
    fn delete_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Message authentication under a tenant's keys: one key per tenant and
/// purpose, made on first use, versioned by rotation outside this port.
/// A MAC is checked by recomputation, so the port only makes them.
pub trait MacService<S: Session>: Send + Sync {
    /// HMAC-SHA-256 of `message` under the latest version of the tenant's
    /// key for `purpose`, making the key if the tenant has none.
    fn mac(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        message: &[u8],
    ) -> impl Future<Output = Result<Mac, Error>> + Send;

    /// Deletes every version of the tenant's key for `purpose`; nothing to
    /// delete is not an error.
    fn delete_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}
