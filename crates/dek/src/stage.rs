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
//! back rests nowhere. A fresh DEK per message is one KMS call per message;
//! where that is a network round trip, [`EnvelopeStage::reusing`] keeps a
//! tenant's DEK for so many messages or so long, as the AWS Encryption
//! SDK's caching materials manager does. The opening side unwraps each
//! wrapped key once for its waiters and keeps what it unwraps from the
//! unwrap's start for its `keep`, its entries capped, a key gone told before
//! any key held ([`EnvelopeStage::opened`], ADR-0039 slice 4).
//!
//! The payload is bound to the message's identity: its associated data is
//! the canonical text of the headers the stage is bound to — the tenant and
//! the `message_id` unless told otherwise — so a payload moved under another
//! message's headers does not open, while the same message delivered again
//! does. The names go along in the `dek_bound_to` header, and the opening
//! side reads them from there, so the two sides cannot disagree; changing
//! that header helps nobody, since the associated data was fixed at
//! sealing. The tenant's id is the `tenant_id` header, and names the KEK.
//!
//! A message missing a header it is bound to cannot be sealed, and one that
//! arrives without one, or whose key or payload will not open, or whose
//! tenant's KEK is gone or retired, or which the KMS refuses to open under
//! this identity or context, is refused for good: [`Permanent`], which the
//! inbox parks at once. A KMS that cannot be reached is a failure of the
//! moment, and the message is tried again.
//!
//! One stage serves one channel: the KMS it is built over carries the
//! channel as its derivation context, taken from the contract both ends
//! are built from, so a DEK drawn for the channel unwraps there and
//! nowhere else, and the DEK in service ([`Reuse`]) is the channel's
//! (ADR-0017).

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use ascetic_ddd_bus::{BoxError, Message, Permanent, Scope, Stage, Unavailable};
use ascetic_ddd_kms::{Algorithm, Class, Error as KmsError, KeyManagementService};
use ascetic_ddd_session::SessionPool;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::future::BoxFuture;
use serde_json::Value;

use crate::domain::canonical;
use crate::draws::{Draws, Held, SealingObserver, causes};
use crate::opens::{Opener, OpeningHeld};
use crate::supply::{InvalidParameters, LONGEST, Supply, SupplyParameters};
use crate::unwraps::Opening;

/// The header naming the tenant, and the associated data of the payload.
pub const TENANT_ID: &str = "tenant_id";

/// The header carrying the message's DEK, wrapped by the tenant's KEK,
/// base64.
pub const DEK: &str = "dek";

/// The header naming the cipher the payload is sealed with.
pub const DEK_ALGORITHM: &str = "dek_algorithm";

/// The header naming the headers the payload is bound to, comma-separated.
pub const DEK_BOUND_TO: &str = "dek_bound_to";

/// The header carrying the message's identity across the bus.
pub const MESSAGE_ID: &str = "message_id";

/// How long a tenant's DEK serves at the sealing side before a fresh one is
/// drawn: so many messages, or so long, whichever comes first.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Reuse {
    /// Messages sealed under one DEK, at most.
    pub messages: u32,
    /// How long one DEK serves, at most.
    pub lifetime: Duration,
}

impl Default for Reuse {
    /// A thousand messages, or a minute: far under the four billion
    /// messages NIST SP 800-38D allows a key with random nonces, and a
    /// thousandth of the KMS calls.
    fn default() -> Self {
        Reuse {
            messages: 1_000,
            lifetime: Duration::from_secs(60),
        }
    }
}

/// How a stage's data keys are drawn (ADR-0039): how long a seal waits for
/// a draw, how long a draw may take, how early a key's successor is drawn,
/// how a failed draw backs off, how many draws run at once, and the most
/// messages one caller's transaction seals for one tenant. Built by
/// [`Pacing::fitted`] from a [`Reuse`], each part then set by the
/// deployment; checked against the reuse when the stage is built.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Pacing {
    /// How long a seal waits for a draw that will land in time.
    pub wait: Duration,
    /// How long a draw may take before it is abandoned.
    pub draw: Duration,
    /// How long before a key's end its successor is drawn: from, to.
    pub lead: (Duration, Duration),
    /// How much earlier than its lifetime a key may end, drawn per key.
    pub jitter: Duration,
    /// A failed draw's back-off: its first step, its cap.
    pub backoff: (Duration, Duration),
    /// How long a tenant whose key is gone is not asked again.
    pub gone: Duration,
    /// How many draws a stage runs at once.
    pub flights: u32,
    /// The most messages one caller's transaction seals for one tenant.
    pub burst: u32,
}

impl Pacing {
    /// A pacing that fits `reuse`, whatever it is: a draw of at most five
    /// seconds and a twelfth of the lifetime, a wait of at most a quarter of
    /// a second and half the draw, the successor drawn one to two draws
    /// ahead, a twentieth of the lifetime's jitter, a back-off from a second
    /// to thirty, ten seconds' memory of a key gone, sixteen draws at once,
    /// a burst of a tenth of the messages. A reuse's lifetime is taken as a
    /// second at least and a century at most ([`LONGEST`]).
    #[must_use]
    pub fn fitted(reuse: Reuse) -> Self {
        let lifetime = bounded(reuse.lifetime);
        let draw = Duration::from_secs(5).min(lifetime / 12);
        Pacing {
            wait: Duration::from_millis(250).min(draw / 2),
            draw,
            lead: (draw, draw * 2),
            jitter: lifetime / 20,
            backoff: (Duration::from_secs(1), Duration::from_secs(30)),
            gone: Duration::from_secs(10),
            flights: 16,
            burst: (reuse.messages / 10).max(1),
        }
    }

    /// The same pacing waiting `wait`.
    #[must_use]
    pub const fn with_wait(self, wait: Duration) -> Self {
        Pacing { wait, ..self }
    }

    /// The same pacing abandoning a draw after `draw`.
    #[must_use]
    pub const fn with_draw(self, draw: Duration) -> Self {
        Pacing { draw, ..self }
    }

    /// The same pacing drawing a successor between `from` and `to` ahead.
    #[must_use]
    pub const fn with_lead(self, from: Duration, to: Duration) -> Self {
        Pacing {
            lead: (from, to),
            ..self
        }
    }

    /// The same pacing ending keys up to `jitter` early.
    #[must_use]
    pub const fn with_jitter(self, jitter: Duration) -> Self {
        Pacing { jitter, ..self }
    }

    /// The same pacing backing off from `first` to `cap`.
    #[must_use]
    pub const fn with_backoff(self, first: Duration, cap: Duration) -> Self {
        Pacing {
            backoff: (first, cap),
            ..self
        }
    }

    /// The same pacing remembering a key gone for `gone`.
    #[must_use]
    pub const fn with_gone(self, gone: Duration) -> Self {
        Pacing { gone, ..self }
    }

    /// The same pacing running `flights` draws at once.
    #[must_use]
    pub const fn with_flights(self, flights: u32) -> Self {
        Pacing { flights, ..self }
    }

    /// The same pacing for callers sealing `burst` messages of one tenant
    /// in one transaction.
    #[must_use]
    pub const fn with_burst(self, burst: u32) -> Self {
        Pacing { burst, ..self }
    }

    /// The supply of a stage reusing keys as `reuse` says, paced so.
    ///
    /// # Errors
    /// [`InvalidParameters`] naming the rule the two break together.
    pub fn supply(&self, reuse: Reuse) -> Result<Supply, InvalidParameters> {
        Supply::new(SupplyParameters {
            lifetime: bounded(reuse.lifetime),
            messages: reuse.messages.max(1),
            burst: self.burst,
            wait: self.wait,
            draw: self.draw,
            lead: self.lead,
            jitter: self.jitter,
            backoff: self.backoff,
            gone: self.gone,
            flights: self.flights,
        })
    }
}

/// A reuse's lifetime as a supply takes it: a second at least, a century at
/// most.
fn bounded(lifetime: Duration) -> Duration {
    lifetime.clamp(Duration::from_secs(1), LONGEST)
}

/// The envelope stage over the KMS `K`, reached through sessions of `P`.
pub struct EnvelopeStage<P, K> {
    draws: Arc<Draws<P, K>>,
    opener: Arc<Opener<P, K>>,
    sentinel: Option<String>,
    algorithm: Algorithm,
    bound_to: Vec<String>,
    reuse: Reuse,
}

/// The entries an opening end holds by default: keys unwrapped and
/// memories of refusals.
const KEYS: usize = 16_384;

/// A key per message: what a stage that reuses none draws.
const PER_MESSAGE: Reuse = Reuse {
    messages: 1,
    lifetime: Duration::from_secs(60),
};

impl<P, K> EnvelopeStage<P, K> {
    /// A stage sealing with AES-256-GCM, drawing and unwrapping DEKs through
    /// `kms` in sessions of `sessions`, binding every payload to the
    /// message's `tenant_id` and `message_id`.
    pub fn new(sessions: P, kms: K) -> Self
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        let supply = Pacing::fitted(PER_MESSAGE)
            .supply(PER_MESSAGE)
            .expect("a fitted pacing fits its reuse");
        let draws = Arc::new(Draws::new(sessions, kms, supply));
        let opener = Arc::new(Opener::over(
            Arc::clone(&draws.parts),
            Opening::fitted(PER_MESSAGE.lifetime, KEYS),
            None,
        ));
        EnvelopeStage {
            draws,
            opener,
            sentinel: None,
            algorithm: Algorithm::Aes256Gcm,
            bound_to: vec![TENANT_ID.to_owned(), MESSAGE_ID.to_owned()],
            reuse: PER_MESSAGE,
        }
    }

    /// The same stage over new draws, paced by `supply` and changed by
    /// `change`. Whatever the old draws hold or fly stays theirs: a flight
    /// out when the stage is configured again lands in them and goes with
    /// them.
    fn redrawn(self, supply: Supply, change: impl FnOnce(Draws<P, K>) -> Draws<P, K>) -> Self
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        let draws = Arc::new(change(self.draws.anew(supply)));
        EnvelopeStage { draws, ..self }
    }

    /// The same stage sealing a tenant's messages under one DEK for as long
    /// as `reuse` allows, instead of a fresh one per message. The wrapped
    /// key travels in every message as before; the opening side needs no
    /// change, and with a cache in front of its KMS unwraps it once.
    pub fn reusing(self, reuse: Reuse) -> Self
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        self.paced(reuse, Pacing::fitted(reuse))
            .expect("a fitted pacing fits its reuse")
    }

    /// The same stage reusing keys as `reuse` says and drawing them as
    /// `pacing` says (ADR-0039).
    ///
    /// # Errors
    /// [`InvalidParameters`] when the two do not fit together.
    pub fn paced(self, reuse: Reuse, pacing: Pacing) -> Result<Self, InvalidParameters>
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        let supply = pacing.supply(reuse)?;
        let stage = self.redrawn(supply, |draws| draws);
        Ok(EnvelopeStage { reuse, ..stage })
    }

    /// The same stage drawing its jitters from `seed`: a test's, so that
    /// what it draws is known.
    pub fn seeded(self, seed: u64) -> Self
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        let supply = self.draws.supply();
        self.redrawn(supply, |draws| draws.with_seed(seed))
    }

    /// The same stage telling `observer` what becomes of its draws.
    pub fn observed_by(self, observer: Arc<dyn SealingObserver>) -> Self
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        let supply = self.draws.supply();
        self.redrawn(supply, |draws| draws.with_observer(observer))
    }

    /// The same stage drawing for `sentinel` — the host's own account,
    /// under the same KMS and policy — when it refuses a seal on no evidence
    /// of whether its KMS serves: at most once per `wait`, so that a quiet
    /// stage tells a tenant's trouble from its KMS's (ADR-0040 decision 3).
    pub fn with_sentinel(self, sentinel: impl Into<String>) -> Self
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        let supply = self.draws.supply();
        let sentinel = sentinel.into();
        let opening = *self.opener.opening();
        let stage = self.redrawn(supply, |draws| draws.with_sentinel(sentinel.clone()));
        let opener = Arc::new(Opener::over(
            Arc::clone(&stage.draws.parts),
            opening,
            Some(sentinel.clone()),
        ));
        EnvelopeStage {
            opener,
            sentinel: Some(sentinel),
            ..stage
        }
    }

    /// The same stage opening as `opening` says (ADR-0039 slice 4): each key
    /// unwrapped once for its waiters, kept from its unwrap's start for
    /// `keep`, its entries capped. What the old opening held stays its own.
    ///
    /// # Errors
    /// [`InvalidParameters`] naming the rule broken.
    pub fn opened(self, opening: Opening) -> Result<Self, InvalidParameters>
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        let opening = opening.validated()?;
        let opener = Arc::new(Opener::over(
            Arc::clone(&self.draws.parts),
            opening,
            self.sentinel.clone(),
        ));
        Ok(EnvelopeStage { opener, ..self })
    }

    /// What the opening end holds now: its keys, its entries, its unwraps.
    pub fn opening_held(&self) -> OpeningHeld
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        self.opener.held()
    }

    /// What the stage holds now: its keys, its tenants, its draws.
    pub fn held(&self) -> Held
    where
        P: SessionPool + Send + Sync + 'static,
        P::Session: Sync,
        K: KeyManagementService<P::Session> + 'static,
    {
        self.draws.held()
    }

    /// The same stage sealing with `algorithm`. What was sealed before opens
    /// with the cipher its header names.
    pub fn with_algorithm(self, algorithm: Algorithm) -> Self {
        EnvelopeStage { algorithm, ..self }
    }

    /// The same stage binding every payload to these headers, in this order:
    /// a message without one of them is not sealed. What was sealed before
    /// opens under the binding its header names. A header's value is bound
    /// as the text it travels as, so bind to headers whose text is stable
    /// on the way, not to ones the channels structure and print again.
    pub fn bound_to(self, headers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        EnvelopeStage {
            bound_to: headers.into_iter().map(Into::into).collect(),
            ..self
        }
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
            let aad = associated_data(&message, &self.bound_to)?;
            let served = self.draws.key(&tenant_id).await?;
            let (dek, wrapped) = (served.dek, served.wrapped);
            let cipher = self.algorithm.cipher(&dek, aad).map_err(verdict)?;
            let sealed = cipher.encrypt(message.payload()).map_err(verdict)?;
            Ok(message
                .with_payload(sealed)
                .with_header(DEK, BASE64.encode(&wrapped))
                .with_header(DEK_ALGORITHM, self.algorithm.as_str())
                .with_header(DEK_BOUND_TO, self.bound_to.join(",")))
        })
    }

    fn inbound(&self, message: Message) -> BoxFuture<'_, Result<Message, BoxError>> {
        Box::pin(async move {
            let tenant_id = tenant_of(&message)?;
            let wrapped = BASE64
                .decode(header(&message, DEK)?)
                .map_err(|error| refused(format!("the `{DEK}` header is not base64: {error}")))?;
            let algorithm: Algorithm = header(&message, DEK_ALGORITHM)?.parse().map_err(verdict)?;
            let bound_to: Vec<String> = header(&message, DEK_BOUND_TO)?
                .split(',')
                .map(str::to_owned)
                .collect();
            let aad = associated_data(&message, &bound_to)?;
            let dek = self.opener.key(&tenant_id, &wrapped).await?;
            let cipher = algorithm.cipher(&dek, aad).map_err(verdict)?;
            let opened = cipher.decrypt(message.payload()).map_err(verdict)?;
            Ok(message
                .with_payload(opened)
                .without_header(DEK)
                .without_header(DEK_ALGORITHM)
                .without_header(DEK_BOUND_TO))
        })
    }
}

impl<P, K> fmt::Debug for EnvelopeStage<P, K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvelopeStage")
            .field("algorithm", &self.algorithm)
            .field("bound_to", &self.bound_to)
            .field("reuse", &self.reuse)
            .field("draws", &self.draws)
            .finish_non_exhaustive()
    }
}

/// The tenant the message names.
fn tenant_of(message: &Message) -> Result<String, BoxError> {
    Ok(header(message, TENANT_ID)?.to_owned())
}

/// What the payload is bound to: the canonical text of the named headers
/// as name-value pairs, in order — `[["tenant_id","t1"],["message_id","…"]]`.
fn associated_data(message: &Message, names: &[String]) -> Result<String, BoxError> {
    let pairs = names
        .iter()
        .map(|name| {
            let value = header(message, name)?;
            Ok(Value::Array(vec![
                Value::String(name.clone()),
                Value::String(value.to_owned()),
            ]))
        })
        .collect::<Result<Vec<_>, BoxError>>()?;
    Ok(canonical::json(&Value::Array(pairs)))
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
    Permanent::because(causes::MALFORMED, what).into()
}

/// The bus's verdict on a failure at the opening end, by the KMS's own
/// class (ADR-0039 decision 8, ADR-0040 decisions 2 and 3):
/// - a key that is gone, for good, under `kms.gone`;
/// - an operation refused, a ciphertext that does not open, bytes that are
///   not what they should be, for good, each under the cause of its remedy
///   ([`causes::of`]): a deploy, not a retry, mends a policy;
/// - the KMS out of reach, unavailable over the stage: a Vault that answers
///   no one answers no tenant, and the inbox holds the message without
///   spending its attempts;
/// - the rest — the session, the database, what the KMS did not explain —
///   of the moment.
///
/// At the sealing end the draws decide (`draws.rs`).
fn verdict(error: KmsError) -> BoxError {
    let cause = causes::of(&error);
    match error.class() {
        Class::Gone | Class::Refused => Permanent::because(cause, error).into(),
        Class::Unavailable => Unavailable::new(cause, Scope::Stage, None, error).into(),
        Class::Moment => error.into(),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "tests")]
mod tests {
    use ascetic_ddd_bus::Cause;
    use ascetic_ddd_session::SessionError;

    use super::*;

    /// Every failure of the KMS has the cause of its remedy, and two remedies
    /// never share a cause (seedwork ADR-0042 decision 9): an operator tells
    /// them apart without the error's text, which may name a tenant.
    #[test]
    fn every_kms_failure_has_the_cause_of_its_remedy() {
        let tenant = || "t".to_owned();
        let cases: Vec<(KmsError, Cause)> = vec![
            (
                KmsError::KekNotFound {
                    tenant_id: tenant(),
                    key_version: None,
                },
                causes::GONE,
            ),
            (
                KmsError::KekNotFound {
                    tenant_id: tenant(),
                    key_version: Some(3),
                },
                causes::KEY_VERSION,
            ),
            (
                KmsError::WrongKeyVersion {
                    expected: 1,
                    found: 2,
                },
                causes::KEY_VERSION,
            ),
            (KmsError::NoKeyOfVersion(4), causes::KEY_VERSION),
            (KmsError::Decrypt, causes::CIPHERTEXT),
            (KmsError::InvalidSignature, causes::CIPHERTEXT),
            (KmsError::Malformed("x".into()), causes::KMS_MALFORMED),
            (KmsError::Unsupported("x"), causes::UNSUPPORTED),
            (
                KmsError::Conflict {
                    tenant_id: tenant(),
                    detail: "x".into(),
                },
                causes::CONFLICT,
            ),
            (
                KmsError::Forbidden {
                    tenant_id: tenant(),
                    operation: "decrypt",
                },
                causes::FORBIDDEN,
            ),
            (
                KmsError::Session(SessionError::Acquire("down".into())),
                causes::UNAVAILABLE,
            ),
            (
                KmsError::Session(SessionError::Abandoned),
                causes::UNEXPLAINED,
            ),
        ];
        for (error, cause) in &cases {
            assert_eq!(causes::of(error), *cause, "{error:?}");
        }
        let distinct: std::collections::HashSet<Cause> =
            cases.iter().map(|(_, cause)| *cause).collect();
        assert_eq!(distinct.len(), 9, "a remedy per cause: {distinct:?}");
    }

    /// Each class of the KMS's failures at the opening end comes to its
    /// verdict on the bus, under its cause (ADR-0040 decisions 2 and 3).
    #[test]
    fn an_opening_failure_comes_to_its_class_s_verdict() {
        let gone = verdict(KmsError::KekNotFound {
            tenant_id: "t".into(),
            key_version: None,
        });
        assert_eq!(
            gone.downcast_ref::<Permanent>().map(Permanent::cause),
            Some(causes::GONE)
        );
        let refused = verdict(KmsError::Forbidden {
            tenant_id: "t".into(),
            operation: "decrypt",
        });
        assert_eq!(
            refused.downcast_ref::<Permanent>().map(Permanent::cause),
            Some(causes::FORBIDDEN)
        );
        let away = verdict(KmsError::Session(SessionError::Acquire("down".into())));
        let away = away
            .downcast_ref::<Unavailable>()
            .expect("unavailable, the stage's");
        assert_eq!(
            (away.cause(), away.scope()),
            (causes::UNAVAILABLE, Scope::Stage)
        );
        let moment = verdict(KmsError::Session(SessionError::Abandoned));
        assert!(
            !moment.is::<Permanent>() && !moment.is::<Unavailable>(),
            "of the moment: {moment}"
        );
    }
}
