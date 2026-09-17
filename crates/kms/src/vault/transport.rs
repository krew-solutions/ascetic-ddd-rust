//! What the Vault adapter needs of an HTTP client.

use serde_json::Value;

use crate::error::BoxError;

/// One call to Vault: method, full URL, the token for the `X-Vault-Token`
/// header, and a JSON body if there is one.
#[derive(Debug)]
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

/// What Vault answered: the status, and the body when there was one.
#[derive(Debug)]
pub struct VaultResponse {
    /// The HTTP status.
    pub status: u16,
    /// The body as JSON; `None` for an empty body, such as a `204`.
    pub body: Option<Value>,
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
