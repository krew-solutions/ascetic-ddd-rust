//! What the Vault adapter needs of an HTTP client.

use serde_json::Value;

use crate::error::BoxError;

/// One call to Vault: method, full URL, the token for the `X-Vault-Token`
/// header, and a JSON body if there is one. Its `Debug` names neither the
/// token nor the body, which may carry a key's plaintext (seedwork
/// ADR-0039 decision 2).
pub struct VaultRequest<'a> {
    /// The HTTP method, upper-case.
    pub method: &'static str,
    /// The full URL, `https://vault:8200/v1/transit/keys/tenant-1`.
    pub url: String,
    /// The token, for the `X-Vault-Token` header.
    pub token: &'a str,
    /// The JSON body, if the call has one.
    pub body: Option<Value>,
}

/// What Vault answered: the status, and the body when there was one. Its
/// `Debug` does not show the body, which may carry a key's plaintext.
pub struct VaultResponse {
    /// The HTTP status.
    pub status: u16,
    /// The body as JSON; `None` for an empty body, such as a `204`.
    pub body: Option<Value>,
}

/// What `Debug` shows of a body: whether there is one.
fn shown(body: Option<&Value>) -> &'static str {
    if body.is_some() { "<redacted>" } else { "none" }
}

impl std::fmt::Debug for VaultRequest<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultRequest")
            .field("method", &self.method)
            .field("url", &self.url)
            .field("token", &"<redacted>")
            .field("body", &shown(self.body.as_ref()))
            .finish()
    }
}

impl std::fmt::Debug for VaultResponse {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VaultResponse")
            .field("status", &self.status)
            .field("body", &shown(self.body.as_ref()))
            .finish()
    }
}

/// An HTTP client as the Vault adapter uses it. Implemented for
/// `reqwest::Client` under the `reqwest` feature; any other client is a
/// few lines in the application.
pub trait VaultTransport: Send + Sync {
    /// Sends the request and reads the whole answer. A status Vault chose,
    /// whatever it is, is a response; only not reaching Vault, or an answer
    /// that is not JSON, is an error.
    fn send(
        &self,
        request: VaultRequest<'_>,
    ) -> impl Future<Output = Result<VaultResponse, BoxError>> + Send;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn debug_names_neither_the_token_nor_a_body() {
        let request = VaultRequest {
            method: "POST",
            url: "https://vault/v1/transit-bus/encrypt/k".to_owned(),
            token: "s.the-token",
            body: Some(serde_json::json!({ "plaintext": "a2V5LW1hdGVyaWFs" })),
        };
        let response = VaultResponse {
            status: 200,
            body: Some(serde_json::json!({ "data": { "plaintext": "a2V5LW1hdGVyaWFs" } })),
        };
        for shown in [format!("{request:?}"), format!("{response:?}")] {
            assert!(!shown.contains("s.the-token"), "{shown}");
            assert!(!shown.contains("a2V5LW1hdGVyaWFs"), "{shown}");
        }
        assert!(format!("{request:?}").contains("transit-bus/encrypt/k"));
    }
}
