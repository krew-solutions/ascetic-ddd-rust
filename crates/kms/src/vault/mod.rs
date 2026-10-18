//! HashiCorp Vault Transit as the key management service.
//!
//! Vault keeps the keys and does the cryptography; nothing but a DEK in the
//! clear crosses into this process. A wrapped DEK is Vault's own ciphertext
//! text, `vault:v1:…`, carried as bytes and opaque here. The HTTP client is
//! the session's ([`HttpAccess`]), and every call goes through
//! [`HttpAccess::request`] so that the session's observer times it; the
//! client speaks to Vault through [`VaultTransport`], implemented for
//! `reqwest::Client` under the `reqwest` feature.
//!
//! One mount holds a tenant's keys of every kind, told apart by name: the
//! KEK is the tenant's id, percent-encoded (what the Python and Go ports
//! name); a signing key is `{tenant}.sign.{algorithm}` (Transit types
//! `ed25519`, `ecdsa-p256`), a MAC key `{tenant}.mac.{purpose}` (type
//! `hmac`). The dot is the separator Transit allows that the parts cannot
//! contain: for the dotted names the tenant's id is checked to be letters,
//! digits, `_` and `-`, not encoded — a `/` is "unsupported path" to
//! Transit even encoded.
//!
//! Signatures and MACs are Vault's `vault:v{n}:…` texts parsed once into
//! their version and bytes; signatures are asked for and verified in JWS
//! marshaling, so an ECDSA signature is `r || s` and never DER. Transit
//! answers `400` both for a key it has not got and for a version a key has
//! not got or no longer serves; the adapter tells them apart by reading the
//! key once, on that path only, rather than by Vault's text. Signing at a
//! named version is refused with a `500` for a version above the latest,
//! retired, or below the key's `min_encryption_version` (measured, Vault
//! 1.21), so a refusal of it is read the same way whether `400`, `403` or
//! `500` (ADR-0019). The public keys are `GET keys/{name}`'s
//! `data.keys.{n}.public_key` of the versions served: base64 of the raw
//! bytes for Ed25519, a PEM SubjectPublicKeyInfo for P-256. A `403` on a
//! KEK operation is read the same way: the key there, it is
//! [`Error::Forbidden`] — the policy does not allow the operation, or the
//! context is not one it admits — and no retry mends it; the key not
//! there, [`Error::KekNotFound`]; the read itself refused, the `403` as it
//! came, for the operator.
//!
//! Built [`with_derivation_context`](VaultTransitService::with_derivation_context),
//! the service serves a derived key (Transit's `derived`): every operation
//! carries the context, base64, and a KEK it makes is made `derived`; what
//! was wrapped under one context does not unwrap under another, and a
//! policy may pin the context a caller passes (ADR-0017). Built
//! [`managed_keys`](VaultTransitService::managed_keys), it makes no KEK on
//! first contact — Transit's `encrypt` would, under a token that may
//! create or update keys — and a tenant without one is
//! [`Error::KekNotFound`] on every wrap; the key is made by whoever has
//! custody, [`KeyCustody`].

#[cfg(feature = "reqwest")]
mod reqwest;
pub mod transport;

use std::num::NonZeroU32;

use ascetic_ddd_session::Session;
use ascetic_ddd_session::rest::HttpAccess;
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD as BASE64_URL};
use futures::future::BoxFuture;
use p256::pkcs8::DecodePublicKey as _;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Value, json};

use crate::domain::{
    Key, KeyLabel, Mac, MacKey, PublicKey, PublicKeyring, Signature, SignatureAlgorithm,
    SigningKey, is_key_part,
};
use crate::error::Error;
use crate::port::{
    KekOptions, KeyCustody, KeyManagementService, KeyOptions, MacKeyCustody, MacService,
    PublicKeyService, SigningKeyCustody, SigningService,
};

pub use self::transport::{VaultRequest, VaultResponse, VaultTransport};

/// A key name in a path: everything but the unreserved characters is
/// encoded, as Python's `quote(safe="")` does.
const KEY_NAME: &AsciiSet = &NON_ALPHANUMERIC
    .remove(b'-')
    .remove(b'_')
    .remove(b'.')
    .remove(b'~');

/// The key management service over a Vault Transit mount.
pub struct VaultTransitService {
    addr: String,
    token: String,
    mount: String,
    key_type: String,
    context: Option<String>,
    managed_keys: bool,
}

impl VaultTransitService {
    /// A service over the Vault at `addr` with `token`, on the `transit`
    /// mount, making keys of type `aes256-gcm96`.
    pub fn new(addr: impl Into<String>, token: impl Into<String>) -> Self {
        let addr = addr.into();
        VaultTransitService {
            addr: addr.trim_end_matches('/').to_owned(),
            token: token.into(),
            mount: "transit".to_owned(),
            key_type: "aes256-gcm96".to_owned(),
            context: None,
            managed_keys: false,
        }
    }

    /// The same service on a derived key, passing `context` with every
    /// KEK operation and making its KEKs `derived`: one instance per
    /// channel of the bus, built in the composition root from the
    /// channel's name (ADR-0017).
    pub fn with_derivation_context(self, context: impl Into<String>) -> Self {
        VaultTransitService {
            context: Some(context.into()),
            ..self
        }
    }

    /// The same service making no key on first use, of any kind: a tenant
    /// without one is [`Error::KekNotFound`] on every wrap,
    /// [`Error::SigningKeyNotFound`] on `sign` and [`Error::MacKeyNotFound`]
    /// on `mac`, each named by one read of the key. For a holder of the use
    /// ports under a token that may not make keys — a producer or a consumer
    /// of the bus, a service that signs and MACs — whose keys are made by
    /// whoever has their custody (ADR-0017, ADR-0020).
    pub fn managed_keys(self) -> Self {
        VaultTransitService {
            managed_keys: true,
            ..self
        }
    }

    /// What a KEK this service makes is made with: its type, and `derived`
    /// when the service serves a context.
    fn kek_body(&self) -> Value {
        json!({ "type": self.key_type, "derived": self.context.is_some() })
    }

    /// `body` with the derivation context, base64, when the service serves
    /// one.
    fn in_context(&self, mut body: Value) -> Value {
        if let (Some(context), Some(object)) = (&self.context, body.as_object_mut()) {
            object.insert(
                "context".to_owned(),
                Value::String(BASE64.encode(context.as_bytes())),
            );
        }
        body
    }

    /// The same service on another mount.
    pub fn with_mount(self, mount: impl Into<String>) -> Self {
        VaultTransitService {
            mount: mount.into(),
            ..self
        }
    }

    /// The same service making keys of another Transit type.
    pub fn with_key_type(self, key_type: impl Into<String>) -> Self {
        VaultTransitService {
            key_type: key_type.into(),
            ..self
        }
    }

    /// The Transit key of a tenant.
    fn key_name(tenant_id: &str) -> String {
        utf8_percent_encode(tenant_id, KEY_NAME).to_string()
    }

    /// The tenant's id as the first part of a dotted key name.
    fn tenant_part(tenant_id: &str) -> Result<&str, Error> {
        if is_key_part(tenant_id) {
            Ok(tenant_id)
        } else {
            Err(Error::Malformed(format!(
                "a tenant id in a key name is letters, digits, `_` and `-`, not `{tenant_id}`"
            )))
        }
    }

    /// The Transit key a tenant signs with, per algorithm: the model's name
    /// for it, once the tenant's id is known to fit a Transit key name.
    fn signing_key_name(tenant_id: &str, algorithm: SignatureAlgorithm) -> Result<String, Error> {
        Ok(SigningKey::name_of(
            Self::tenant_part(tenant_id)?,
            algorithm,
        ))
    }

    /// The Transit key a tenant makes MACs with, per purpose, likewise.
    fn mac_key_name(tenant_id: &str, purpose: &KeyLabel) -> Result<String, Error> {
        Ok(MacKey::name_of(Self::tenant_part(tenant_id)?, purpose))
    }

    /// One call under the mount; `404` is [`Error::KekNotFound`], any other
    /// refusal [`Error::Vault`], `204` an empty object.
    ///
    /// Boxed, with every borrow under one lifetime: the future of a call
    /// through two `impl Future`-returning trait methods — the transport's
    /// `send` inside the session's `request` — cannot be shown `Send` for
    /// every lifetime at once on stable Rust (ADR-0004), and a boxed future
    /// says it once, for this one.
    fn request<'a, S>(
        &'a self,
        session: &'a S,
        tenant_id: &'a str,
        method: &'static str,
        path: &'a str,
        body: Option<Value>,
    ) -> BoxFuture<'a, Result<Value, Error>>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        Box::pin(async move {
            let url = format!("{}/v1/{}{}", self.addr, self.mount, path);
            let call = session.http().send(VaultRequest {
                method,
                url: url.clone(),
                token: &self.token,
                body,
            });
            let response = session
                .request(method, &url, call)
                .await
                .map_err(Error::Transport)?;
            match response.status {
                404 => Err(Error::KekNotFound {
                    tenant_id: tenant_id.to_owned(),
                    key_version: None,
                }),
                204 => Ok(json!({})),
                status if (200..300).contains(&status) => {
                    Ok(response.body.unwrap_or_else(|| json!({})))
                }
                status => Err(Error::Vault {
                    status,
                    method,
                    path: path.to_owned(),
                    message: errors_of(response.body.as_ref()),
                }),
            }
        })
    }

    /// One call under the mount, classified by status alone: `204` is an
    /// empty answer, any other `2xx` its body, anything else
    /// [`Error::Vault`] — a `404` included, for the callers that name the
    /// key themselves.
    fn transit<'a, S>(
        &'a self,
        session: &'a S,
        method: &'static str,
        path: &'a str,
        body: Option<Value>,
    ) -> BoxFuture<'a, Result<Value, Error>>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        Box::pin(async move {
            let url = format!("{}/v1/{}{}", self.addr, self.mount, path);
            let call = session.http().send(VaultRequest {
                method,
                url: url.clone(),
                token: &self.token,
                body,
            });
            let response = session
                .request(method, &url, call)
                .await
                .map_err(Error::Transport)?;
            match response.status {
                204 => Ok(json!({})),
                status if (200..300).contains(&status) => {
                    Ok(response.body.unwrap_or_else(|| json!({})))
                }
                status => Err(Error::Vault {
                    status,
                    method,
                    path: path.to_owned(),
                    message: errors_of(response.body.as_ref()),
                }),
            }
        })
    }

    /// `POST keys/{name}` with `body`: makes the key, or does nothing to one
    /// that exists.
    async fn ensure_named<S>(&self, session: &S, name: &str, body: Value) -> Result<(), Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let path = format!("/keys/{name}");
        self.transit(session, "POST", &path, Some(body)).await?;
        Ok(())
    }

    /// The key as Vault describes it, `None` when there is none.
    async fn read_named<S>(&self, session: &S, name: &str) -> Result<Option<Value>, Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let path = format!("/keys/{name}");
        match self.transit(session, "GET", &path, None).await {
            Ok(answer) => Ok(Some(answer)),
            Err(Error::Vault { status: 404, .. }) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Makes the named key with `body` — `POST keys/{name}`, which Transit
    /// answers for a key that exists with its description and without
    /// touching it (measured, Vault 1.21) — and holds what is there to what
    /// was asked: another type, or another `derived`, is
    /// [`Error::Conflict`]; a rotation period asked and not the key's is set
    /// by `config`. Answers the latest version. One read only where Vault
    /// answered without the description.
    #[allow(clippy::too_many_arguments)]
    async fn create_named<S>(
        &self,
        session: &S,
        tenant_id: &str,
        name: &str,
        key_type: &str,
        derived: bool,
        auto_rotate_period: Option<std::time::Duration>,
        mut body: Value,
    ) -> Result<u32, Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        if let (Some(period), Some(object)) = (auto_rotate_period, body.as_object_mut()) {
            object.insert("auto_rotate_period".to_owned(), json!(period.as_secs()));
        }
        let path = format!("/keys/{name}");
        let answer = match self.transit(session, "POST", &path, Some(body)).await {
            Ok(answer) => answer,
            // A making refused under this identity is the policy's, whether
            // the key is there or not: no read explains more.
            Err(Error::Vault { status: 403, .. }) => {
                return Err(Error::Forbidden {
                    tenant_id: tenant_id.to_owned(),
                    operation: "create",
                });
            }
            Err(error) => return Err(error),
        };
        let answer = if answer
            .get("data")
            .and_then(|data| data.get("type"))
            .is_some()
        {
            answer
        } else {
            self.read_named(session, name).await?.ok_or_else(|| {
                Error::Malformed(format!("key `{name}` vanished while being made"))
            })?
        };
        let data = answer.get("data");
        let found_type = data
            .and_then(|data| data.get("type"))
            .and_then(Value::as_str)
            .unwrap_or_default();
        let found_derived = data
            .and_then(|data| data.get("derived"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        if found_type != key_type || found_derived != derived {
            let kind = |key_type: &str, derived: bool| {
                format!("{}{key_type}", if derived { "derived " } else { "" })
            };
            return Err(Error::Conflict {
                tenant_id: tenant_id.to_owned(),
                detail: format!(
                    "`{name}` is a {} key, a {} one was asked",
                    kind(found_type, found_derived),
                    kind(key_type, derived)
                ),
            });
        }
        if let Some(period) = auto_rotate_period {
            let found = data
                .and_then(|data| data.get("auto_rotate_period"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            if found != period.as_secs() {
                let path = format!("/keys/{name}/config");
                self.transit(
                    session,
                    "POST",
                    &path,
                    Some(json!({ "auto_rotate_period": period.as_secs() })),
                )
                .await?;
            }
        }
        version_field(&answer, "latest_version")
    }

    /// The next version of the named key, `POST keys/{name}/rotate`, which
    /// Transit refuses for a key it has not got: a rotation never makes the
    /// first version. Answers the new latest version, from the answer or,
    /// where Vault said nothing, one read.
    async fn rotate_named<S>(
        &self,
        session: &S,
        tenant_id: &str,
        name: &str,
        missing: Error,
    ) -> Result<u32, Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let path = format!("/keys/{name}/rotate");
        let answer = match self.transit(session, "POST", &path, Some(json!({}))).await {
            Ok(answer) => answer,
            Err(error) => {
                return Err(self
                    .explain_custody(session, tenant_id, name, "rotate", missing, error)
                    .await);
            }
        };
        if let Ok(version) = version_field(&answer, "latest_version") {
            return Ok(version);
        }
        let answer = self
            .read_named(session, name)
            .await?
            .ok_or_else(|| Error::Malformed(format!("key `{name}` vanished while rotating")))?;
        version_field(&answer, "latest_version")
    }

    /// Retires the named key's versions below `version` by its
    /// `min_decryption_version`, after one read: a bound above the latest
    /// is [`Error::NoKeyOfVersion`] and nothing is written; one at or below
    /// the floor there is writes nothing — Transit lowers the floor when
    /// asked (measured, Vault 1.21), and a retried or a stale retirement
    /// must not bring a version back. Where the key has a signing floor of
    /// its own (`min_encryption_version`, not zero) below the bound, it is
    /// raised with it in the same call: Transit holds it at or above the
    /// decryption floor, and a retired version signs nothing anyway.
    async fn retire_named<S>(
        &self,
        session: &S,
        tenant_id: &str,
        name: &str,
        version: u32,
        missing: Error,
    ) -> Result<(), Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let Some(answer) = self.read_named(session, name).await? else {
            return Err(missing);
        };
        let latest = version_field(&answer, "latest_version")?;
        if version > latest {
            return Err(Error::NoKeyOfVersion(version));
        }
        let floor = version_field(&answer, "min_decryption_version").unwrap_or(1);
        if version <= floor {
            return Ok(());
        }
        let mut body = json!({ "min_decryption_version": version });
        let signing_floor = version_field(&answer, "min_encryption_version").unwrap_or(0);
        if (1..version).contains(&signing_floor)
            && let Some(object) = body.as_object_mut()
        {
            object.insert("min_encryption_version".to_owned(), json!(version));
        }
        let path = format!("/keys/{name}/config");
        match self.transit(session, "POST", &path, Some(body)).await {
            Ok(_) => Ok(()),
            Err(error) => Err(self
                .explain_custody(session, tenant_id, name, "retire", missing, error)
                .await),
        }
    }

    /// What a refusal of a custody operation on the named key means, by one
    /// read on a `400` or a `403`: the key not there, `missing`; a `403`
    /// with the key there, [`Error::Forbidden`] — the identity holds no
    /// custody; a `400` with the key there, the request's own fault,
    /// [`Error::Malformed`] with Vault's words. Anything else, and a read
    /// refused, as it came.
    async fn explain_custody<S>(
        &self,
        session: &S,
        tenant_id: &str,
        name: &str,
        operation: &'static str,
        missing: Error,
        error: Error,
    ) -> Error
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let Error::Vault {
            status: status @ (400 | 403),
            ref message,
            ..
        } = error
        else {
            return error;
        };
        match self.read_named(session, name).await {
            Ok(None) => missing,
            Ok(Some(_)) if status == 403 => Error::Forbidden {
                tenant_id: tenant_id.to_owned(),
                operation,
            },
            Ok(Some(_)) => Error::Malformed(format!("vault refused {operation}: {message}")),
            Err(_) => error,
        }
    }

    /// Deletes the named key, every version; none is not an error. A
    /// refusal under this identity, the key there, is [`Error::Forbidden`].
    async fn delete_named<S>(&self, session: &S, tenant_id: &str, name: &str) -> Result<(), Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        if self.read_named(session, name).await?.is_none() {
            return Ok(());
        }
        let forbidden = |error: Error| match error {
            Error::Vault { status: 403, .. } => Error::Forbidden {
                tenant_id: tenant_id.to_owned(),
                operation: "delete",
            },
            error => error,
        };
        let path = format!("/keys/{name}/config");
        self.transit(
            session,
            "POST",
            &path,
            Some(json!({ "deletion_allowed": true })),
        )
        .await
        .map_err(forbidden)?;
        let path = format!("/keys/{name}");
        self.transit(session, "DELETE", &path, None)
            .await
            .map_err(forbidden)?;
        Ok(())
    }

    /// What a `400` from an operation on the named key means: the key is
    /// not there (`missing`), the version the operation named is outside
    /// what the key serves ([`Error::NoKeyOfVersion`]), or something else,
    /// left as it was. One read of the key, on this path only.
    async fn explain<S>(
        &self,
        session: &S,
        name: &str,
        key_version: Option<u32>,
        missing: impl FnOnce() -> Error + Send,
        error: Error,
    ) -> Error
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        if !matches!(error, Error::Vault { status: 400, .. }) {
            return error;
        }
        match self.read_named(session, name).await {
            Ok(None) => missing(),
            Ok(Some(answer)) => match key_version {
                Some(version) if !serves(&answer, version) => Error::NoKeyOfVersion(version),
                _ => error,
            },
            Err(_) => error,
        }
    }

    /// Makes the tenant's KEK unless the keys are managed elsewhere.
    async fn ensure_key<S>(&self, session: &S, tenant_id: &str) -> Result<(), Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        if self.managed_keys {
            return Ok(());
        }
        let path = format!("/keys/{}", Self::key_name(tenant_id));
        self.request(session, tenant_id, "POST", &path, Some(self.kek_body()))
            .await?;
        Ok(())
    }

    /// What a refusal of a KEK operation means, by one read of the key: a
    /// `400` as [`VaultTransitService::explain`] reads it, with the tenant's
    /// KEK as the key that may be missing — and, the key there and the
    /// version served, the request's own fault, which no retry mends: on
    /// `decrypt` and `rewrap` [`Error::Decrypt`] (a wrong context, bytes
    /// changed since they were sealed), elsewhere [`Error::Malformed`] with
    /// Vault's words; a `403` with the key there is [`Error::Forbidden`],
    /// with the key not there [`Error::KekNotFound`], with the read refused
    /// too the `403` as it came.
    async fn explain_kek<S>(
        &self,
        session: &S,
        tenant_id: &str,
        key_version: Option<u32>,
        operation: &'static str,
        error: Error,
    ) -> Error
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let name = Self::key_name(tenant_id);
        let missing = || Error::KekNotFound {
            tenant_id: tenant_id.to_owned(),
            key_version: None,
        };
        match error {
            Error::Vault { status: 400, .. } => {
                match self
                    .explain(session, &name, key_version, missing, error)
                    .await
                {
                    Error::Vault {
                        status: 400,
                        message,
                        ..
                    } if matches!(operation, "decrypt" | "rewrap") => {
                        let _ = message;
                        Error::Decrypt
                    }
                    Error::Vault {
                        status: 400,
                        message,
                        ..
                    } => Error::Malformed(format!("vault refused {operation}: {message}")),
                    explained => explained,
                }
            }
            Error::Vault { status: 403, .. } => match self.read_named(session, &name).await {
                Ok(Some(_)) => Error::Forbidden {
                    tenant_id: tenant_id.to_owned(),
                    operation,
                },
                Ok(None) => missing(),
                Err(_) => error,
            },
            error => error,
        }
    }
}

impl VaultTransitService {
    /// What a refusal of signing or of a MAC means, by one read of the key —
    /// on a `400`, a `403` or a `500`, since Transit answers `400` for a key
    /// it has not got and, signing at a named version, `500` for a version
    /// above the latest, retired, or below the one the key signs from
    /// (measured, Vault 1.21): the key not there, `missing`; a `403` with
    /// the key there, [`Error::Forbidden`]; a version named and outside what
    /// the key signs with, [`Error::NoKeyOfVersion`]; a `400` the read does
    /// not explain, the request's own fault, [`Error::Malformed`] (the rule
    /// ADR-0017 point 5 set for the KEK); a `500` it does not explain stays
    /// Vault's, of the moment; the read refused, the refusal as it came. Any
    /// other status — `429`, `502`, `503`, `504` — is left as it came,
    /// unread: a read would add to the load a rate limit protects, or meet
    /// the same sealed or standby node. Vault's texts are not matched.
    #[allow(clippy::too_many_arguments)]
    async fn explain_use<S>(
        &self,
        session: &S,
        tenant_id: &str,
        name: &str,
        operation: &'static str,
        version: Option<u32>,
        missing: Error,
        error: Error,
    ) -> Error
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let Error::Vault {
            status: status @ (400 | 403 | 500),
            ref message,
            ..
        } = error
        else {
            return error;
        };
        match self.read_named(session, name).await {
            Ok(None) => missing,
            Ok(Some(_)) if status == 403 => Error::Forbidden {
                tenant_id: tenant_id.to_owned(),
                operation,
            },
            Ok(Some(answer)) if version.is_some_and(|version| !signs(&answer, version)) => {
                Error::NoKeyOfVersion(version.unwrap_or_default())
            }
            Ok(Some(_)) if status == 400 => {
                Error::Malformed(format!("vault refused {operation}: {message}"))
            }
            Ok(Some(_)) | Err(_) => error,
        }
    }
}

/// What Vault said in `{"errors": [...]}`, joined.
fn errors_of(body: Option<&Value>) -> String {
    body.and_then(|body| body.get("errors"))
        .and_then(Value::as_array)
        .map(|errors| {
            errors
                .iter()
                .filter_map(Value::as_str)
                .collect::<Vec<_>>()
                .join("; ")
        })
        .unwrap_or_default()
}

/// `data.<field>` of an answer, as text.
fn text_field<'a>(answer: &'a Value, field: &str) -> Result<&'a str, Error> {
    answer
        .get("data")
        .and_then(|data| data.get(field))
        .and_then(Value::as_str)
        .ok_or_else(|| Error::Malformed(format!("vault answered without `data.{field}`")))
}

/// `data.<field>` of an answer, base64 as Vault writes it, decoded.
fn base64_field(answer: &Value, field: &str) -> Result<Vec<u8>, Error> {
    BASE64
        .decode(text_field(answer, field)?)
        .map_err(|error| Error::Malformed(format!("vault's `data.{field}` is not base64: {error}")))
}

/// A wrapped DEK is Vault's ciphertext text.
fn ciphertext_text(encrypted_dek: &[u8]) -> Result<&str, Error> {
    std::str::from_utf8(encrypted_dek)
        .map_err(|_| Error::Malformed("a Vault ciphertext is text, `vault:v1:…`".into()))
}

/// `data.<field>` of an answer as a version.
fn version_field(answer: &Value, field: &str) -> Result<u32, Error> {
    answer
        .get("data")
        .and_then(|data| data.get(field))
        .and_then(Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
        .ok_or_else(|| Error::Malformed(format!("vault answered without `data.{field}`")))
}

/// Whether the key, as Vault describes it, serves `version`: made, and not
/// retired by `min_decryption_version`.
fn serves(answer: &Value, version: u32) -> bool {
    let latest = version_field(answer, "latest_version").unwrap_or(0);
    let oldest = version_field(answer, "min_decryption_version").unwrap_or(1);
    (oldest..=latest).contains(&version)
}

/// Whether the key, as Vault describes it, signs with `version`: made, not
/// retired, and not below `min_encryption_version` — which Transit holds as
/// a floor of signing too, `0` or absent meaning no floor of its own
/// (measured, Vault 1.21: a retirement does not raise it).
fn signs(answer: &Value, version: u32) -> bool {
    let latest = version_field(answer, "latest_version").unwrap_or(0);
    let floor = version_field(answer, "min_decryption_version")
        .unwrap_or(1)
        .max(version_field(answer, "min_encryption_version").unwrap_or(0))
        .max(1);
    (floor..=latest).contains(&version)
}

/// The signature of `algorithm` in a `sign` answer: the version Vault
/// names, and the JWS bytes.
fn signature_of(algorithm: SignatureAlgorithm, answer: &Value) -> Result<Signature, Error> {
    let (key_version, payload) = versioned(text_field(answer, "signature")?, "signature")?;
    let bytes = BASE64_URL.decode(payload).map_err(|error| {
        Error::Malformed(format!("vault's signature is not JWS base64: {error}"))
    })?;
    Ok(Signature::new(algorithm, key_version, bytes))
}

/// The public halves of the versions a key described by `answer` serves:
/// `data.keys.{n}.public_key`, base64 of the raw bytes for Ed25519, a PEM
/// SubjectPublicKeyInfo for P-256, each read into the model's form. The key
/// must be of the algorithm's Transit type.
fn public_keyring(algorithm: SignatureAlgorithm, answer: &Value) -> Result<PublicKeyring, Error> {
    let kind = text_field(answer, "type")?;
    if kind != transit_key_type(algorithm) {
        return Err(Error::Malformed(format!(
            "vault describes the key as `{kind}`, not as a {algorithm} key"
        )));
    }
    let keys = answer
        .get("data")
        .and_then(|data| data.get("keys"))
        .and_then(Value::as_object)
        .ok_or_else(|| Error::Malformed("vault answered without `data.keys`".into()))?;
    let mut ring = Vec::with_capacity(keys.len());
    for (version, entry) in keys {
        let version: u32 = version
            .parse()
            .map_err(|_| Error::Malformed(format!("vault names a key version `{version}`")))?;
        if !serves(answer, version) {
            continue;
        }
        let text = entry
            .get("public_key")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::Malformed(format!("vault answered no public key for v{version}"))
            })?;
        ring.push((version, public_key_of(algorithm, text)?));
    }
    PublicKeyring::new(algorithm, ring)
}

/// A public key as Transit publishes it, in the model's form.
fn public_key_of(algorithm: SignatureAlgorithm, text: &str) -> Result<PublicKey, Error> {
    let malformed =
        |what: &str| Error::Malformed(format!("vault's {algorithm} public key: {what}"));
    match algorithm {
        SignatureAlgorithm::Ed25519 => {
            let bytes = BASE64
                .decode(text.trim())
                .map_err(|_| malformed("not base64"))?;
            PublicKey::new(algorithm, bytes)
        }
        SignatureAlgorithm::EcdsaP256 => {
            // The PEM without its armor is the DER of a SubjectPublicKeyInfo,
            // base64; the key inside is the SEC1 point the model holds.
            let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
            if lines.next() != Some("-----BEGIN PUBLIC KEY-----") {
                return Err(malformed("not a PEM public key"));
            }
            let body: String = lines
                .take_while(|line| !line.starts_with("-----"))
                .collect();
            if !text.contains("-----END PUBLIC KEY-----") {
                return Err(malformed("not a PEM public key"));
            }
            let der = BASE64.decode(body).map_err(|_| malformed("not base64"))?;
            let key = p256::ecdsa::VerifyingKey::from_public_key_der(&der)
                .map_err(|_| malformed("not a P-256 SubjectPublicKeyInfo"))?;
            PublicKey::new(algorithm, key.to_sec1_bytes())
        }
    }
}

/// The version and the payload of a `vault:v{n}:{payload}` text.
fn versioned(text: &str, what: &str) -> Result<(u32, String), Error> {
    let malformed = || Error::Malformed(format!("a Vault {what} is `vault:v<n>:…`, not `{text}`"));
    let rest = text.strip_prefix("vault:v").ok_or_else(malformed)?;
    let (version, payload) = rest.split_once(':').ok_or_else(malformed)?;
    let version = version.parse::<u32>().map_err(|_| malformed())?;
    Ok((version, payload.to_owned()))
}

/// The parameters a signing operation names beside the input: JWS
/// marshaling for every algorithm, so the bytes are the algorithm's raw
/// form; the hash where the algorithm takes one.
fn signing_parameters(algorithm: SignatureAlgorithm) -> Value {
    match algorithm {
        SignatureAlgorithm::Ed25519 => json!({ "marshaling_algorithm": "jws" }),
        SignatureAlgorithm::EcdsaP256 => json!({
            "marshaling_algorithm": "jws",
            "hash_algorithm": "sha2-256",
        }),
    }
}

/// The Transit type of the key an algorithm signs with.
fn transit_key_type(algorithm: SignatureAlgorithm) -> &'static str {
    match algorithm {
        SignatureAlgorithm::Ed25519 => "ed25519",
        SignatureAlgorithm::EcdsaP256 => "ecdsa-p256",
    }
}

fn with_input(mut parameters: Value, input: &[u8]) -> Value {
    if let Some(object) = parameters.as_object_mut() {
        object.insert("input".to_owned(), Value::String(BASE64.encode(input)));
    }
    parameters
}

impl<S> KeyManagementService<S> for VaultTransitService
where
    S: Session + HttpAccess + Sync,
    S::Client: VaultTransport,
{
    async fn encrypt_dek(&self, session: &S, tenant_id: &str, dek: &Key) -> Result<Vec<u8>, Error> {
        self.ensure_key(session, tenant_id).await?;
        let path = format!("/encrypt/{}", Self::key_name(tenant_id));
        let body = self.in_context(json!({ "plaintext": BASE64.encode(dek.as_bytes()) }));
        let answer = match self
            .request(session, tenant_id, "POST", &path, Some(body))
            .await
        {
            Ok(answer) => answer,
            Err(error) => {
                return Err(self
                    .explain_kek(session, tenant_id, None, "encrypt", error)
                    .await);
            }
        };
        Ok(text_field(&answer, "ciphertext")?.as_bytes().to_vec())
    }

    async fn decrypt_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Key, Error> {
        let text = ciphertext_text(encrypted_dek)?;
        let (key_version, _) = versioned(text, "ciphertext")?;
        let path = format!("/decrypt/{}", Self::key_name(tenant_id));
        let body = self.in_context(json!({ "ciphertext": text }));
        let answer = match self
            .request(session, tenant_id, "POST", &path, Some(body))
            .await
        {
            Ok(answer) => answer,
            Err(error) => {
                return Err(self
                    .explain_kek(session, tenant_id, Some(key_version), "decrypt", error)
                    .await);
            }
        };
        Ok(Key::new(base64_field(&answer, "plaintext")?))
    }

    async fn generate_dek(&self, session: &S, tenant_id: &str) -> Result<(Key, Vec<u8>), Error> {
        self.ensure_key(session, tenant_id).await?;
        let path = format!("/datakey/plaintext/{}", Self::key_name(tenant_id));
        let body = self.in_context(json!({ "bits": 256 }));
        let answer = match self
            .request(session, tenant_id, "POST", &path, Some(body))
            .await
        {
            Ok(answer) => answer,
            Err(error) => {
                return Err(self
                    .explain_kek(session, tenant_id, None, "datakey", error)
                    .await);
            }
        };
        let dek = Key::new(base64_field(&answer, "plaintext")?);
        let encrypted_dek = text_field(&answer, "ciphertext")?.as_bytes().to_vec();
        Ok((dek, encrypted_dek))
    }

    async fn rewrap_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let text = ciphertext_text(encrypted_dek)?;
        let (key_version, _) = versioned(text, "ciphertext")?;
        let path = format!("/rewrap/{}", Self::key_name(tenant_id));
        let body = self.in_context(json!({ "ciphertext": text }));
        let answer = match self
            .request(session, tenant_id, "POST", &path, Some(body))
            .await
        {
            Ok(answer) => answer,
            Err(error) => {
                return Err(self
                    .explain_kek(session, tenant_id, Some(key_version), "rewrap", error)
                    .await);
            }
        };
        Ok(text_field(&answer, "ciphertext")?.as_bytes().to_vec())
    }
}

impl<S> KeyCustody<S> for VaultTransitService
where
    S: Session + HttpAccess + Sync,
    S::Client: VaultTransport,
{
    /// `POST keys/{name}` with the service's key type, `derived` and the
    /// rotation period asked; a key there of another type or another
    /// `derived` is [`Error::Conflict`], since neither can be changed; the
    /// period, which can, is set by `config` when it differs.
    async fn create_kek(
        &self,
        session: &S,
        tenant_id: &str,
        options: &KekOptions,
    ) -> Result<u32, Error> {
        self.create_named(
            session,
            tenant_id,
            &Self::key_name(tenant_id),
            &self.key_type,
            options.derived,
            options.auto_rotate_period,
            json!({ "type": self.key_type, "derived": options.derived }),
        )
        .await
    }

    async fn rotate_kek(&self, session: &S, tenant_id: &str) -> Result<u32, Error> {
        self.rotate_named(
            session,
            tenant_id,
            &Self::key_name(tenant_id),
            kek_not_found(tenant_id),
        )
        .await
    }

    /// `min_decryption_version` of the key raised to `version`, never
    /// lowered.
    async fn retire_kek_versions_before(
        &self,
        session: &S,
        tenant_id: &str,
        version: u32,
    ) -> Result<(), Error> {
        self.retire_named(
            session,
            tenant_id,
            &Self::key_name(tenant_id),
            version,
            kek_not_found(tenant_id),
        )
        .await
    }

    async fn delete_kek(&self, session: &S, tenant_id: &str) -> Result<(), Error> {
        self.delete_named(session, tenant_id, &Self::key_name(tenant_id))
            .await
    }
}

/// The refusal of a tenant without a KEK.
fn kek_not_found(tenant_id: &str) -> Error {
    Error::KekNotFound {
        tenant_id: tenant_id.to_owned(),
        key_version: None,
    }
}

/// The refusal of a tenant without a signing key of the algorithm.
fn signing_key_not_found(tenant_id: &str, algorithm: SignatureAlgorithm) -> Error {
    Error::SigningKeyNotFound {
        tenant_id: tenant_id.to_owned(),
        algorithm,
    }
}

/// The refusal of a tenant without a MAC key for the purpose.
fn mac_key_not_found(tenant_id: &str, purpose: &KeyLabel) -> Error {
    Error::MacKeyNotFound {
        tenant_id: tenant_id.to_owned(),
        purpose: purpose.as_str().to_owned(),
    }
}

impl<S> SigningService<S> for VaultTransitService
where
    S: Session + HttpAccess + Sync,
    S::Client: VaultTransport,
{
    async fn sign(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        message: &[u8],
    ) -> Result<Signature, Error> {
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        if !self.managed_keys {
            self.ensure_named(
                session,
                &name,
                json!({ "type": transit_key_type(algorithm) }),
            )
            .await?;
        }
        let path = format!("/sign/{name}");
        let body = with_input(signing_parameters(algorithm), message);
        match self.transit(session, "POST", &path, Some(body)).await {
            Ok(answer) => signature_of(algorithm, &answer),
            Err(error) => Err(self
                .explain_use(
                    session,
                    tenant_id,
                    &name,
                    "sign",
                    None,
                    signing_key_not_found(tenant_id, algorithm),
                    error,
                )
                .await),
        }
    }

    async fn sign_with_version(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        key_version: NonZeroU32,
        message: &[u8],
    ) -> Result<Signature, Error> {
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        let path = format!("/sign/{name}");
        let mut body = with_input(signing_parameters(algorithm), message);
        if let Some(object) = body.as_object_mut() {
            object.insert("key_version".to_owned(), json!(key_version.get()));
        }
        let answer = match self.transit(session, "POST", &path, Some(body)).await {
            Ok(answer) => answer,
            Err(error) => {
                return Err(self
                    .explain_use(
                        session,
                        tenant_id,
                        &name,
                        "sign",
                        Some(key_version.get()),
                        signing_key_not_found(tenant_id, algorithm),
                        error,
                    )
                    .await);
            }
        };
        let signature = signature_of(algorithm, &answer)?;
        if signature.key_version() == key_version.get() {
            Ok(signature)
        } else {
            Err(Error::Malformed(format!(
                "vault signed with v{}, v{key_version} was asked",
                signature.key_version()
            )))
        }
    }

    async fn verify(
        &self,
        session: &S,
        tenant_id: &str,
        message: &[u8],
        signature: &Signature,
    ) -> Result<(), Error> {
        let algorithm = signature.algorithm();
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        let path = format!("/verify/{name}");
        let mut body = with_input(signing_parameters(algorithm), message);
        if let Some(object) = body.as_object_mut() {
            object.insert(
                "signature".to_owned(),
                Value::String(format!(
                    "vault:v{}:{}",
                    signature.key_version(),
                    BASE64_URL.encode(signature.as_bytes())
                )),
            );
        }
        let answer = match self.transit(session, "POST", &path, Some(body)).await {
            Ok(answer) => answer,
            Err(error) => {
                return Err(self
                    .explain(
                        session,
                        &name,
                        Some(signature.key_version()),
                        || Error::SigningKeyNotFound {
                            tenant_id: tenant_id.to_owned(),
                            algorithm,
                        },
                        error,
                    )
                    .await);
            }
        };
        match answer
            .get("data")
            .and_then(|data| data.get("valid"))
            .and_then(Value::as_bool)
        {
            Some(true) => Ok(()),
            Some(false) => Err(Error::InvalidSignature),
            None => Err(Error::Malformed(
                "vault answered without `data.valid`".into(),
            )),
        }
    }
}

/// Custody of the signing keys on the mount: `{tenant}.sign.{algorithm}`,
/// made of the algorithm's Transit type, never derived.
impl<S> SigningKeyCustody<S> for VaultTransitService
where
    S: Session + HttpAccess + Sync,
    S::Client: VaultTransport,
{
    async fn create_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        options: &KeyOptions,
    ) -> Result<u32, Error> {
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        let key_type = transit_key_type(algorithm);
        self.create_named(
            session,
            tenant_id,
            &name,
            key_type,
            false,
            options.auto_rotate_period,
            json!({ "type": key_type }),
        )
        .await
    }

    async fn rotate_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        self.rotate_named(
            session,
            tenant_id,
            &name,
            signing_key_not_found(tenant_id, algorithm),
        )
        .await
    }

    async fn retire_signing_versions_before(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
    ) -> Result<(), Error> {
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        self.retire_named(
            session,
            tenant_id,
            &name,
            version,
            signing_key_not_found(tenant_id, algorithm),
        )
        .await
    }

    async fn delete_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<(), Error> {
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        self.delete_named(session, tenant_id, &name).await
    }
}

/// The public keys from `GET keys/{name}`: the versions the key serves,
/// read into the model's form. Never makes a key.
impl<S> PublicKeyService<S> for VaultTransitService
where
    S: Session + HttpAccess + Sync,
    S::Client: VaultTransport,
{
    async fn public_keys(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<PublicKeyring, Error> {
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        let answer =
            self.read_named(session, &name)
                .await?
                .ok_or_else(|| Error::SigningKeyNotFound {
                    tenant_id: tenant_id.to_owned(),
                    algorithm,
                })?;
        public_keyring(algorithm, &answer)
    }
}

impl<S> MacService<S> for VaultTransitService
where
    S: Session + HttpAccess + Sync,
    S::Client: VaultTransport,
{
    async fn mac(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        message: &[u8],
    ) -> Result<Mac, Error> {
        let name = Self::mac_key_name(tenant_id, purpose)?;
        if !self.managed_keys {
            self.ensure_named(session, &name, mac_key_body()).await?;
        }
        let path = format!("/hmac/{name}");
        let body = with_input(json!({ "algorithm": "sha2-256" }), message);
        let answer = match self.transit(session, "POST", &path, Some(body)).await {
            Ok(answer) => answer,
            Err(error) => {
                return Err(self
                    .explain_use(
                        session,
                        tenant_id,
                        &name,
                        "hmac",
                        None,
                        mac_key_not_found(tenant_id, purpose),
                        error,
                    )
                    .await);
            }
        };
        let (key_version, payload) = versioned(text_field(&answer, "hmac")?, "hmac")?;
        let bytes = BASE64
            .decode(payload)
            .map_err(|error| Error::Malformed(format!("vault's hmac is not base64: {error}")))?;
        Ok(Mac::new(key_version, bytes))
    }
}

/// What a MAC key is made with: HMAC over thirty-two bytes.
fn mac_key_body() -> Value {
    json!({ "type": "hmac", "key_size": 32 })
}

/// Custody of the MAC keys on the mount: `{tenant}.mac.{purpose}`, of type
/// `hmac`, never derived.
impl<S> MacKeyCustody<S> for VaultTransitService
where
    S: Session + HttpAccess + Sync,
    S::Client: VaultTransport,
{
    async fn create_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        options: &KeyOptions,
    ) -> Result<u32, Error> {
        let name = Self::mac_key_name(tenant_id, purpose)?;
        self.create_named(
            session,
            tenant_id,
            &name,
            "hmac",
            false,
            options.auto_rotate_period,
            mac_key_body(),
        )
        .await
    }

    async fn rotate_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<u32, Error> {
        let name = Self::mac_key_name(tenant_id, purpose)?;
        self.rotate_named(
            session,
            tenant_id,
            &name,
            mac_key_not_found(tenant_id, purpose),
        )
        .await
    }

    async fn retire_mac_versions_before(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        version: u32,
    ) -> Result<(), Error> {
        let name = Self::mac_key_name(tenant_id, purpose)?;
        self.retire_named(
            session,
            tenant_id,
            &name,
            version,
            mac_key_not_found(tenant_id, purpose),
        )
        .await
    }

    async fn delete_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<(), Error> {
        let name = Self::mac_key_name(tenant_id, purpose)?;
        self.delete_named(session, tenant_id, &name).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tenant_id_becomes_a_path_segment() {
        assert_eq!(VaultTransitService::key_name("tenant-1"), "tenant-1");
        assert_eq!(VaultTransitService::key_name("a/b c"), "a%2Fb%20c");
        assert_eq!(VaultTransitService::key_name("x.y_z~"), "x.y_z~");
    }

    #[test]
    fn a_trailing_slash_in_the_address_is_dropped() {
        let service = VaultTransitService::new("http://vault:8200/", "t");
        assert_eq!(service.addr, "http://vault:8200");
    }

    #[test]
    fn a_dotted_name_takes_only_a_plain_tenant_id() {
        let label = KeyLabel::new("draft-etag").unwrap();
        assert_eq!(
            VaultTransitService::signing_key_name("tenant-1", SignatureAlgorithm::Ed25519).unwrap(),
            "tenant-1.sign.ed25519"
        );
        assert_eq!(
            VaultTransitService::mac_key_name("tenant_1", &label).unwrap(),
            "tenant_1.mac.draft-etag"
        );
        for bad in ["a/b", "a.b", "", "a b"] {
            assert!(
                matches!(
                    VaultTransitService::signing_key_name(bad, SignatureAlgorithm::Ed25519),
                    Err(Error::Malformed(_))
                ),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_versioned_text_is_its_version_and_payload() {
        assert_eq!(
            versioned("vault:v12:abc", "signature").unwrap(),
            (12, "abc".to_owned())
        );
        for bad in ["v1:abc", "vault:vx:abc", "vault:v1", ""] {
            assert!(
                matches!(versioned(bad, "hmac"), Err(Error::Malformed(_))),
                "{bad:?}"
            );
        }
    }

    #[test]
    fn a_key_serves_the_versions_between_the_retired_and_the_latest() {
        let key = json!({ "data": { "latest_version": 3, "min_decryption_version": 2 } });
        assert!(!serves(&key, 1));
        assert!(serves(&key, 2));
        assert!(serves(&key, 3));
        assert!(!serves(&key, 4));
    }

    #[test]
    fn a_key_signs_from_the_higher_of_its_two_floors_to_the_latest() {
        let no_floor = json!({ "data": { "latest_version": 3, "min_decryption_version": 2, "min_encryption_version": 0 } });
        assert!(!signs(&no_floor, 1));
        assert!(signs(&no_floor, 2));
        assert!(signs(&no_floor, 3));
        assert!(!signs(&no_floor, 4));
        let absent = json!({ "data": { "latest_version": 3, "min_decryption_version": 1 } });
        assert!(signs(&absent, 1));
        let raised = json!({ "data": { "latest_version": 3, "min_decryption_version": 1, "min_encryption_version": 3 } });
        assert!(!signs(&raised, 2));
        assert!(signs(&raised, 3));
        // Verifies, no longer signs.
        assert!(serves(&raised, 2));
    }

    #[test]
    fn vault_s_errors_are_joined() {
        assert_eq!(
            errors_of(Some(&json!({ "errors": ["one", "two"] }))),
            "one; two"
        );
        assert_eq!(errors_of(None), "");
    }
}
