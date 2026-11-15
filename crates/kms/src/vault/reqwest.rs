//! [`VaultTransport`] for `reqwest::Client`.

use super::transport::{VaultRequest, VaultResponse, VaultTransport};
use crate::error::BoxError;

impl VaultTransport for reqwest::Client {
    async fn send(&self, request: VaultRequest<'_>) -> Result<VaultResponse, BoxError> {
        let method = reqwest::Method::from_bytes(request.method.as_bytes())?;
        let mut builder = self
            .request(method, &request.url)
            .header("X-Vault-Token", request.token);
        if let Some(body) = &request.body {
            builder = builder.json(body);
        }
        let response = builder.send().await?;
        let status = response.status().as_u16();
        // The answer may carry a key's plaintext: its text is wiped when it
        // goes. The JSON read from it is out of this crate's reach until the
        // caller drops it (seedwork ADR-0039 decision 2).
        let text = zeroize::Zeroizing::new(response.text().await?);
        let body = match text.trim() {
            "" => None,
            json => Some(serde_json::from_str(json)?),
        };
        Ok(VaultResponse { status, body })
    }
}
