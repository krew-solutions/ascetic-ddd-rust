//! The envelope stage of the bus: a DEK per message, wrapped through the
//! KMS, carried in a header.
//!
//! On the way out the stage draws a fresh DEK for the message's tenant from
//! the KMS, seals the payload with it, and puts the DEK, wrapped by the
//! tenant's KEK, in the `dek` header, base64, with the cipher's name in
//! `dek_algorithm`. On the way in it reads the header back, unwraps the DEK
//! through the KMS, and opens the payload. Nothing between the two ends —
//! the outbox, a broker, the inbox — holds a payload in the clear, and the
//! receiving side needs no table of keys, only a KMS that holds the
//! tenant's KEK (ADR-0002). This is the envelope of Tink's `KmsEnvelopeAead`
//! and of the AWS Encryption SDK, with the wrapped key in a header rather
//! than in front of the ciphertext.
//!
//! The stage reaches the KMS through a session pool of its own: the KMS's
//! session is not the data's — Vault's is HTTP, a PostgreSQL KMS may be in
//! another database — and a key drawn for a message that is then rolled
//! back rests nowhere.
//!
//! The tenant's id is the `tenant_id` header, and the associated data of
//! the payload, as at every level of the key hierarchy. A message without
//! one cannot be sealed, and one that arrives without one, or whose key or
//! payload will not open, or whose tenant's KEK is gone, is refused for good:
//! [`Permanent`], which the inbox parks at once. A KMS that cannot be
//! reached is a failure of the moment, and the message is tried again.

use std::fmt;

use ascetic_ddd_bus::{BoxError, Message, Permanent, Stage};
use ascetic_ddd_kms::{Algorithm, Error as KmsError, KeyManagementService};
use ascetic_ddd_session::SessionPool;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::future::BoxFuture;

/// The header naming the tenant, and the associated data of the payload.
pub const TENANT_ID: &str = "tenant_id";

/// The header carrying the message's DEK, wrapped by the tenant's KEK,
/// base64.
pub const DEK: &str = "dek";

/// The header naming the cipher the payload is sealed with.
pub const DEK_ALGORITHM: &str = "dek_algorithm";

/// The envelope stage over the KMS `K`, reached through sessions of `P`.
pub struct EnvelopeStage<P, K> {
    sessions: P,
    kms: K,
    algorithm: Algorithm,
}

impl<P, K> EnvelopeStage<P, K> {
    /// A stage sealing with AES-256-GCM, drawing and unwrapping DEKs through
    /// `kms` in sessions of `sessions`.
    pub fn new(sessions: P, kms: K) -> Self {
        EnvelopeStage {
            sessions,
            kms,
            algorithm: Algorithm::Aes256Gcm,
        }
    }

    /// The same stage sealing with `algorithm`. What was sealed before opens
    /// with the cipher its header names.
    pub fn with_algorithm(self, algorithm: Algorithm) -> Self {
        EnvelopeStage { algorithm, ..self }
    }
}

impl<P, K> Stage for EnvelopeStage<P, K>
where
    P: SessionPool + Send + Sync + 'static,
    P::Session: Sync,
    K: KeyManagementService<P::Session> + 'static,
{
    fn outbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            let tenant_id = tenant_of(&message)?;
            let (dek, wrapped) = self
                .sessions
                .session(async |session| self.kms.generate_dek(&session, &tenant_id).await)
                .await
                .map_err(verdict)?;
            let cipher = self
                .algorithm
                .cipher(&dek, tenant_id.as_bytes())
                .map_err(verdict)?;
            let sealed = cipher.encrypt(message.payload()).map_err(verdict)?;
            Ok(message
                .with_payload(sealed)
                .with_header(DEK, BASE64.encode(wrapped))
                .with_header(DEK_ALGORITHM, self.algorithm.as_str()))
        })
    }

    fn inbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            let tenant_id = tenant_of(&message)?;
            let wrapped = BASE64
                .decode(header(&message, DEK)?)
                .map_err(|error| refused(format!("the `{DEK}` header is not base64: {error}")))?;
            let algorithm: Algorithm = header(&message, DEK_ALGORITHM)?.parse().map_err(verdict)?;
            let dek = self
                .sessions
                .session(async |session| self.kms.decrypt_dek(&session, &tenant_id, &wrapped).await)
                .await
                .map_err(verdict)?;
            let cipher = algorithm
                .cipher(&dek, tenant_id.as_bytes())
                .map_err(verdict)?;
            let opened = cipher.decrypt(message.payload()).map_err(verdict)?;
            Ok(message
                .with_payload(opened)
                .without_header(DEK)
                .without_header(DEK_ALGORITHM))
        })
    }
}

impl<P, K> fmt::Debug for EnvelopeStage<P, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvelopeStage")
            .field("algorithm", &self.algorithm)
            .finish_non_exhaustive()
    }
}

/// The tenant the message names.
fn tenant_of(message: &Message) -> Result<String, BoxError> {
    Ok(header(message, TENANT_ID)?.to_owned())
}

/// A header as text; a message without it, or with bytes that are not
/// text, is refused for good.
fn header<'a>(message: &'a Message, name: &str) -> Result<&'a str, BoxError> {
    let value = message
        .header(name)
        .ok_or_else(|| refused(format!("the message has no `{name}` header")))?;
    std::str::from_utf8(value).map_err(|_| refused(format!("the `{name}` header is not text")))
}

fn refused(what: String) -> BoxError {
    Permanent::new(what).into()
}

/// Which failures of the KMS no retry will mend: a key that is gone, a
/// ciphertext that does not open, bytes that are not what they should be.
/// The rest — the session, the database, Vault out of reach — are of the
/// moment.
fn verdict(error: KmsError) -> BoxError {
    match error {
        KmsError::KekNotFound { .. }
        | KmsError::Decrypt
        | KmsError::WrongKeyVersion { .. }
        | KmsError::NoKeyOfVersion(_)
        | KmsError::UnsupportedAlgorithm(_)
        | KmsError::Malformed(_) => Permanent::new(error).into(),
        _ => error.into(),
    }
}
