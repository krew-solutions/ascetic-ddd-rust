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
        // reqwest's error names the URL, whose last part is the key's name,
        // its tenant's (seedwork ADR-0046): it goes without it.
        let response = builder.send().await.map_err(reqwest::Error::without_url)?;
        let status = response.status().as_u16();
        // The answer may carry a key's plaintext: its text is wiped when it
        // goes. The JSON read from it is out of this crate's reach until the
        // caller drops it (seedwork ADR-0039 decision 2).
        let text =
            zeroize::Zeroizing::new(response.text().await.map_err(reqwest::Error::without_url)?);
        let body = match text.trim() {
            "" => None,
            json => Some(serde_json::from_str(json)?),
        };
        Ok(VaultResponse { status, body })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// reqwest refuses a scheme it does not serve before any I/O, naming the
    /// URL: the error goes without it (seedwork ADR-0046), offline.
    #[tokio::test]
    async fn a_refused_call_names_no_url() {
        const TENANT: &str = "0190a01a-7e5b-7d80-a000-00000000e001";
        let refused = reqwest::Client::new()
            .send(VaultRequest {
                method: "POST",
                url: format!("ftp://vault:8200/v1/transit-bus/datakey/plaintext/{TENANT}"),
                token: "a-token-nobody-reads",
                body: None,
            })
            .await
            .expect_err("an ftp URL is refused");
        let text = format!("{refused} {refused:?}");
        assert!(!text.contains(TENANT), "{text}");
        assert!(!text.contains("ftp://"), "{text}");
    }
}
