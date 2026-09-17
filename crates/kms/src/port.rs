//! The port: what the application layer sees.

use ascetic_ddd_session::Session;

use crate::domain::Key;
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
