//! HashiCorp Vault Transit as the key management service.
//!
//! Vault keeps the KEKs and does the cryptography; nothing but a DEK in the
//! clear crosses into this process. A wrapped DEK is Vault's own ciphertext
//! text, `vault:v1:…`, carried as bytes and opaque here. Transit's key name
//! is the tenant's id, percent-encoded. The HTTP client is the session's
//! ([`HttpAccess`]), and every call goes through [`HttpAccess::request`] so
//! that the session's observer times it; the client speaks to Vault through
//! [`VaultTransport`], implemented for `reqwest::Client` under the `reqwest`
//! feature.

#[cfg(feature = "reqwest")]
mod reqwest;
pub mod transport;

use ascetic_ddd_session::Session;
use ascetic_ddd_session::rest::HttpAccess;
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use futures::future::BoxFuture;
use percent_encoding::{AsciiSet, NON_ALPHANUMERIC, utf8_percent_encode};
use serde_json::{Value, json};

use crate::cipher::Key;
use crate::error::Error;
use crate::port::KeyManagementService;

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

    async fn key_exists<S>(&self, session: &S, tenant_id: &str) -> Result<bool, Error>
    where
        S: Session + HttpAccess + Sync,
        S::Client: VaultTransport,
    {
        let path = format!("/keys/{}", Self::key_name(tenant_id));
        match self.request(session, tenant_id, "GET", &path, None).await {
            Ok(_) => Ok(true),
            Err(Error::KekNotFound { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Makes the tenant's key if there is none; Transit's `POST /keys/:name`
    /// leaves an existing key as it is.
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
        let key_name = Self::key_name(tenant_id);
        if !self.key_exists(session, tenant_id).await? {
            let path = format!("/keys/{key_name}");
            self.request(
                session,
                tenant_id,
                "POST",
                &path,
                Some(json!({ "type": self.key_type })),
            )
            .await?;
            return Ok(1);
        }
        let path = format!("/keys/{key_name}/rotate");
        self.request(session, tenant_id, "POST", &path, Some(json!({})))
            .await?;
        let path = format!("/keys/{key_name}");
        let answer = self.request(session, tenant_id, "GET", &path, None).await?;
        answer
            .get("data")
            .and_then(|data| data.get("latest_version"))
            .and_then(Value::as_u64)
            .and_then(|version| u32::try_from(version).ok())
            .ok_or_else(|| Error::Malformed("vault answered without `data.latest_version`".into()))
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
        if !self.key_exists(session, tenant_id).await? {
            return Ok(());
        }
        let key_name = Self::key_name(tenant_id);
        let path = format!("/keys/{key_name}/config");
        self.request(
            session,
            tenant_id,
            "POST",
            &path,
            Some(json!({ "deletion_allowed": true })),
        )
        .await?;
        let path = format!("/keys/{key_name}");
        self.request(session, tenant_id, "DELETE", &path, None)
            .await?;
        Ok(())
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
    fn vault_s_errors_are_joined() {
        assert_eq!(
            errors_of(Some(&json!({ "errors": ["one", "two"] }))),
            "one; two"
        );
        assert_eq!(errors_of(None), "");
    }
}
