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
//! key once, on that path only, rather than by Vault's text.

#[cfg(feature = "reqwest")]
mod reqwest;
pub mod transport;

use ascetic_ddd_session::Session;
use ascetic_ddd_session::rest::HttpAccess;
use base64::Engine as _;
use base64::engine::general_purpose::{STANDARD as BASE64, URL_SAFE_NO_PAD as BASE64_URL};
use futures::future::BoxFuture;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Value, json};

use crate::domain::{
    Key, KeyLabel, Mac, MacKey, Signature, SignatureAlgorithm, SigningKey, is_key_part,
};
use crate::error::Error;
use crate::port::{KeyManagementService, MacService, SigningService};

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
        }
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

    /// The next version of the named key, or its first, made with `body`.
    async fn rotate_named<S>(&self, session: &S, name: &str, body: Value) -> Result<u32, Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        if self.read_named(session, name).await?.is_none() {
            self.ensure_named(session, name, body).await?;
            return Ok(1);
        }
        let path = format!("/keys/{name}/rotate");
        self.transit(session, "POST", &path, Some(json!({})))
            .await?;
        let answer = self
            .read_named(session, name)
            .await?
            .ok_or_else(|| Error::Malformed(format!("key `{name}` vanished while rotating")))?;
        version_field(&answer, "latest_version")
    }

    /// Deletes the named key, every version; none is not an error.
    async fn delete_named<S>(&self, session: &S, name: &str) -> Result<(), Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        if self.read_named(session, name).await?.is_none() {
            return Ok(());
        }
        let path = format!("/keys/{name}/config");
        self.transit(
            session,
            "POST",
            &path,
            Some(json!({ "deletion_allowed": true })),
        )
        .await?;
        let path = format!("/keys/{name}");
        self.transit(session, "DELETE", &path, None).await?;
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

    async fn ensure_key<S>(&self, session: &S, tenant_id: &str) -> Result<(), Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let path = format!("/keys/{}", Self::key_name(tenant_id));
        self.request(
            session,
            tenant_id,
            "POST",
            &path,
            Some(json!({ "type": self.key_type })),
        )
        .await?;
        Ok(())
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
        let body = json!({ "plaintext": BASE64.encode(dek.as_bytes()) });
        let answer = self
            .request(session, tenant_id, "POST", &path, Some(body))
            .await?;
        Ok(text_field(&answer, "ciphertext")?.as_bytes().to_vec())
    }

    async fn decrypt_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Key, Error> {
        let path = format!("/decrypt/{}", Self::key_name(tenant_id));
        let body = json!({ "ciphertext": ciphertext_text(encrypted_dek)? });
        let answer = self
            .request(session, tenant_id, "POST", &path, Some(body))
            .await?;
        Ok(Key::new(base64_field(&answer, "plaintext")?))
    }

    async fn generate_dek(&self, session: &S, tenant_id: &str) -> Result<(Key, Vec<u8>), Error> {
        self.ensure_key(session, tenant_id).await?;
        let path = format!("/datakey/plaintext/{}", Self::key_name(tenant_id));
        let answer = self
            .request(
                session,
                tenant_id,
                "POST",
                &path,
                Some(json!({ "bits": 256 })),
            )
            .await?;
        let dek = Key::new(base64_field(&answer, "plaintext")?);
        let encrypted_dek = text_field(&answer, "ciphertext")?.as_bytes().to_vec();
        Ok((dek, encrypted_dek))
    }

    async fn rotate_kek(&self, session: &S, tenant_id: &str) -> Result<u32, Error> {
        self.rotate_named(
            session,
            &Self::key_name(tenant_id),
            json!({ "type": self.key_type }),
        )
        .await
    }

    async fn rewrap_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let path = format!("/rewrap/{}", Self::key_name(tenant_id));
        let body = json!({ "ciphertext": ciphertext_text(encrypted_dek)? });
        let answer = self
            .request(session, tenant_id, "POST", &path, Some(body))
            .await?;
        Ok(text_field(&answer, "ciphertext")?.as_bytes().to_vec())
    }

    async fn delete_kek(&self, session: &S, tenant_id: &str) -> Result<(), Error> {
        self.delete_named(session, &Self::key_name(tenant_id)).await
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
        self.ensure_named(
            session,
            &name,
            json!({ "type": transit_key_type(algorithm) }),
        )
        .await?;
        let path = format!("/sign/{name}");
        let body = with_input(signing_parameters(algorithm), message);
        let answer = self.transit(session, "POST", &path, Some(body)).await?;
        let (key_version, payload) = versioned(text_field(&answer, "signature")?, "signature")?;
        let bytes = BASE64_URL.decode(payload).map_err(|error| {
            Error::Malformed(format!("vault's signature is not JWS base64: {error}"))
        })?;
        Ok(Signature::new(algorithm, key_version, bytes))
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

    async fn rotate_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        let name = Self::signing_key_name(tenant_id, algorithm)?;
        self.rotate_named(
            session,
            &name,
            json!({ "type": transit_key_type(algorithm) }),
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
        self.delete_named(session, &name).await
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
        self.ensure_named(session, &name, json!({ "type": "hmac", "key_size": 32 }))
            .await?;
        let path = format!("/hmac/{name}");
        let body = with_input(json!({ "algorithm": "sha2-256" }), message);
        let answer = self.transit(session, "POST", &path, Some(body)).await?;
        let (key_version, payload) = versioned(text_field(&answer, "hmac")?, "hmac")?;
        let bytes = BASE64
            .decode(payload)
            .map_err(|error| Error::Malformed(format!("vault's hmac is not base64: {error}")))?;
        Ok(Mac::new(key_version, bytes))
    }

    async fn delete_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<(), Error> {
        let name = Self::mac_key_name(tenant_id, purpose)?;
        self.delete_named(session, &name).await
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
    fn vault_s_errors_are_joined() {
        assert_eq!(
            errors_of(Some(&json!({ "errors": ["one", "two"] }))),
            "one; two"
        );
        assert_eq!(errors_of(None), "");
    }
}
