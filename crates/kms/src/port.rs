//! The port: what the application layer sees.

use std::num::NonZeroU32;
use std::time::Duration;

use ascetic_ddd_session::Session;

use crate::domain::{Key, KeyLabel, Mac, PublicKeyring, Signature, SignatureAlgorithm};
use crate::error::Error;

/// Envelope encryption for one tenant at a time: data-encryption keys are
/// wrapped and unwrapped by the tenant's current key-encryption key, which
/// the service makes on first contact unless its keys are managed
/// elsewhere. The use of the KEK only; making, rotating, retiring and
/// deleting it is its custody, [`KeyCustody`] (ADR-0020). The surface of
/// Vault Transit; the PostgreSQL adapter keeps the same surface.
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

    /// `encrypted_dek` wrapped again under the current KEK, after a rotation.
    fn rewrap_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> impl Future<Output = Result<Vec<u8>, Error>> + Send;
}

/// What a tenant's KEK is made with, by whoever has custody of it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KekOptions {
    /// Every operation on the key takes a derivation context, and what is
    /// wrapped under one context does not unwrap under another; the
    /// context is a channel's name on the bus (ADR-0017). Fixed at
    /// creation: a key that exists otherwise is a conflict.
    pub derived: bool,
    /// The KMS rotates the key itself, this often; an adapter without a
    /// scheduler refuses it rather than ignore it.
    pub auto_rotate_period: Option<Duration>,
}

/// What a tenant's signing or MAC key is made with, by whoever has custody
/// of it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct KeyOptions {
    /// The KMS rotates the key itself, this often; an adapter without a
    /// scheduler refuses it rather than ignore it. `None` asks nothing of a
    /// key that exists: it keeps the period it has.
    pub auto_rotate_period: Option<Duration>,
}

/// Custody of a tenant's KEK: what makes it, with its options, rotates it,
/// retires its versions and deletes it — the tenant-management context's,
/// held by nobody who merely wraps and unwraps (ADR-0017, ADR-0020). Beside
/// [`KeyManagementService`], not on it: a producer or a consumer of the bus
/// is bound by that port and must not hold this one.
///
/// The rules of every custody port (ADR-0020): making is idempotent and
/// typed; a rotation never makes the first version and is not idempotent —
/// a retried one makes one version more, which serves as well; a
/// retirement keeps the latest version and never lowers the floor, so a
/// retried or a stale one does nothing; a deletion of nothing is not an
/// error.
pub trait KeyCustody<S: Session>: Send + Sync {
    /// Makes the tenant's KEK with `options`, version 1, and answers its
    /// latest version; a key that exists with the same options is left as
    /// it is and its latest version answered, one that exists with other
    /// options is [`Error::Conflict`].
    fn create_kek(
        &self,
        session: &S,
        tenant_id: &str,
        options: &KekOptions,
    ) -> impl Future<Output = Result<u32, Error>> + Send;

    /// The next version of the tenant's KEK; answers it. Earlier versions
    /// stay, so what they wrapped still unwraps.
    ///
    /// # Errors
    /// [`Error::KekNotFound`] when the tenant has none: a rotation never
    /// makes the first version, so a scheduled one that meets an off-boarded
    /// tenant does not bring its key back.
    fn rotate_kek(
        &self,
        session: &S,
        tenant_id: &str,
    ) -> impl Future<Output = Result<u32, Error>> + Send;

    /// Retires every version of the tenant's KEK below `version`: what they
    /// wrapped never unwraps again, [`Error::NoKeyOfVersion`] from then on.
    /// The bound of a message's life on the bus, applied once the channel's
    /// retention has passed (ADR-0017). A bound at or below the oldest
    /// version kept does nothing: the floor is never lowered.
    ///
    /// # Errors
    /// [`Error::KekNotFound`] when the tenant has none;
    /// [`Error::NoKeyOfVersion`] for a bound above the latest version, which
    /// would retire the key whole, and nothing is retired.
    fn retire_kek_versions_before(
        &self,
        session: &S,
        tenant_id: &str,
        version: u32,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Deletes every version of the tenant's KEK: nothing wrapped under them
    /// unwraps again — crypto-shredding. Nothing to delete is not an error.
    fn delete_kek(
        &self,
        session: &S,
        tenant_id: &str,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// Digital signatures under a tenant's keys: one key per tenant and
/// algorithm, versioned by rotation; every version verifies until it is
/// retired. The use of the key only; its custody is [`SigningKeyCustody`]
/// (ADR-0020).
///
/// Which version signs is the caller's, by the method it calls.
/// [`sign`](Self::sign) takes the latest and makes the key on first
/// signing, unless the adapter's keys are managed elsewhere;
/// [`sign_with_version`](Self::sign_with_version) takes the version named
/// and makes nothing. The latest is right only where the signed bytes
/// do not name the version and every verifier learns of a version when the
/// KMS makes it — verification through this port. A message that names its
/// key's version inside what is signed — a JWS's `kid` — must have the
/// version before it is signed; a verifier that holds the public keys itself
/// — a gateway's JWKS, a device's key directory — refuses a version until it
/// is given it. Both sign with the version named (ADR-0019).
///
/// The algorithm is an input of signing and a property of the signature,
/// so a tenant moves to another algorithm by signing with it — a new key,
/// not a rotation — while what the old key signed keeps verifying.
///
/// Deleting a key ends the verifiability of everything it ever signed: the
/// tenant's off-boarding, never a rotation.
pub trait SigningService<S: Session>: Send + Sync {
    /// Signs `message` with the latest version of the tenant's key of
    /// `algorithm`, making the key if the tenant has none — unless the
    /// adapter's keys are managed elsewhere, when none is
    /// [`Error::SigningKeyNotFound`]. A rotation makes the new version sign
    /// at once.
    fn sign(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        message: &[u8],
    ) -> impl Future<Output = Result<Signature, Error>> + Send;

    /// Signs `message` with version `key_version` of the tenant's key of
    /// `algorithm`, and with no other: the signature names `key_version`
    /// whatever the latest is. Never makes a key — a version is named
    /// because something outside the KMS already holds it, and a key made
    /// here would be a version nobody holds, or one numbered as a deleted
    /// key's was. It also signs under an identity that may not make keys.
    ///
    /// The version is not zero by its type: Transit reads zero as the
    /// latest, which is what this method exists not to do.
    ///
    /// # Errors
    /// [`Error::SigningKeyNotFound`] when the tenant has no key of the
    /// algorithm; [`Error::NoKeyOfVersion`] for a version the key has not
    /// got yet, has retired, or no longer signs with — one that still
    /// verifies included; [`Error::Forbidden`] when the KMS refuses to sign
    /// under this identity and the key is there.
    fn sign_with_version(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        key_version: NonZeroU32,
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
}

/// Custody of a tenant's signing keys: what makes, rotates, retires and
/// deletes them — the tenant-management context's, or an operator's
/// procedure for a key of the platform's own, and never what signs
/// (ADR-0020). Beside [`SigningService`], not on it, as [`KeyCustody`] is
/// beside [`KeyManagementService`]; the rules are [`KeyCustody`]'s.
pub trait SigningKeyCustody<S: Session>: Send + Sync {
    /// Makes the tenant's key of `algorithm` with `options`, version 1, and
    /// answers its latest version; a key that exists is left as it is and
    /// its latest version answered, its rotation period set when `options`
    /// asks for another.
    ///
    /// # Errors
    /// [`Error::Conflict`] when the name holds a key of another type, or a
    /// derived one; [`Error::Unsupported`] for a rotation period an adapter
    /// without a scheduler cannot keep.
    fn create_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        options: &KeyOptions,
    ) -> impl Future<Output = Result<u32, Error>> + Send;

    /// Makes the next version of the tenant's key of `algorithm`; answers
    /// it. Earlier versions keep verifying until they are retired.
    ///
    /// # Errors
    /// [`Error::SigningKeyNotFound`] when the tenant has none: a rotation
    /// never makes the first version.
    fn rotate_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> impl Future<Output = Result<u32, Error>> + Send;

    /// Retires every version of the tenant's key of `algorithm` below
    /// `version`: they neither sign, verify nor are published again —
    /// [`Error::NoKeyOfVersion`] from then on. A version that still signs
    /// may be retired: that is how a compromised one is answered; the
    /// latest never is. A bound at or below the oldest version kept does
    /// nothing.
    ///
    /// # Errors
    /// [`Error::SigningKeyNotFound`] when the tenant has none;
    /// [`Error::NoKeyOfVersion`] for a bound above the latest version, and
    /// nothing is retired.
    fn retire_signing_versions_before(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Deletes every version of the tenant's key of `algorithm`: what it
    /// signed never verifies again. Nothing to delete is not an error.
    fn delete_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}

/// The public halves of a tenant's signing keys, for verifiers outside the
/// KMS — a JWKS, a gateway's configuration, a device's key directory. A
/// query, beside [`SigningService`] rather than on it, so that an adapter
/// that only publishes keys is bound by reading them, and a holder of
/// public keys that cannot sign can serve it (ADR-0019).
pub trait PublicKeyService<S: Session>: Send + Sync {
    /// The public halves of the versions of the tenant's key of
    /// `algorithm` that verify — from the oldest an operator has not
    /// retired to the latest, a version that verifies and no longer signs
    /// among them. Never makes a key. Where the adapter holds the private
    /// halves, each public half is derived from its private half, never
    /// taken from beside it.
    ///
    /// # Errors
    /// [`Error::SigningKeyNotFound`] when the tenant has no key of the
    /// algorithm; [`Error::Malformed`] when the KMS describes the key as
    /// another algorithm's, or a stored public half is not its private
    /// half's.
    fn public_keys(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> impl Future<Output = Result<PublicKeyring, Error>> + Send;
}

/// Message authentication under a tenant's keys: one key per tenant and
/// purpose, versioned by rotation. A MAC is checked by recomputation, so
/// the port only makes them. The use of the key only; its custody is
/// [`MacKeyCustody`] (ADR-0020).
pub trait MacService<S: Session>: Send + Sync {
    /// HMAC-SHA-256 of `message` under the latest version of the tenant's
    /// key for `purpose`, making the key if the tenant has none — unless
    /// the adapter's keys are managed elsewhere, when none is
    /// [`Error::MacKeyNotFound`].
    fn mac(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        message: &[u8],
    ) -> impl Future<Output = Result<Mac, Error>> + Send;
}

/// Custody of a tenant's MAC keys: what makes, rotates, retires and deletes
/// them, never what makes MACs (ADR-0020). The rules are [`KeyCustody`]'s.
/// Since a MAC is made with the latest version alone, a retirement changes
/// nothing a holder of [`MacService`] sees: it bounds the key material
/// kept at rest.
pub trait MacKeyCustody<S: Session>: Send + Sync {
    /// Makes the tenant's key for `purpose` with `options`, version 1, and
    /// answers its latest version; a key that exists is left as it is and
    /// its latest version answered, its rotation period set when `options`
    /// asks for another.
    ///
    /// # Errors
    /// [`Error::Conflict`] when the name holds a key of another type, or a
    /// derived one; [`Error::Unsupported`] for a rotation period an adapter
    /// without a scheduler cannot keep.
    fn create_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        options: &KeyOptions,
    ) -> impl Future<Output = Result<u32, Error>> + Send;

    /// Makes the next version of the tenant's key for `purpose`; answers
    /// it. What the earlier version made no longer matches what the key
    /// makes: a consumer that checks by recomputation sees it as stale.
    ///
    /// # Errors
    /// [`Error::MacKeyNotFound`] when the tenant has none.
    fn rotate_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> impl Future<Output = Result<u32, Error>> + Send;

    /// Retires every version of the tenant's key for `purpose` below
    /// `version`; the latest never is. A bound at or below the oldest
    /// version kept does nothing.
    ///
    /// # Errors
    /// [`Error::MacKeyNotFound`] when the tenant has none;
    /// [`Error::NoKeyOfVersion`] for a bound above the latest version, and
    /// nothing is retired.
    fn retire_mac_versions_before(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        version: u32,
    ) -> impl Future<Output = Result<(), Error>> + Send;

    /// Deletes every version of the tenant's key for `purpose`; nothing to
    /// delete is not an error.
    fn delete_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> impl Future<Output = Result<(), Error>> + Send;
}
