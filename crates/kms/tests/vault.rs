//! Integration tests for the Vault Transit adapter. They need a Vault with
//! the `transit` engine enabled — a dev server does:
//!
//! ```text
//! docker run --rm --cap-add=IPC_LOCK -p 8200:8200 \
//!     -e VAULT_DEV_ROOT_TOKEN_ID=test-root-token hashicorp/vault
//! vault secrets enable transit          # VAULT_ADDR=http://localhost:8200
//! ASCETIC_DDD_TEST_VAULT_ADDR=http://localhost:8200 ASCETIC_DDD_TEST_VAULT_TOKEN=test-root-token \
//!     cargo test -p ascetic-ddd-kms --features reqwest -- --ignored
//! ```

#![cfg(feature = "reqwest")]

use ascetic_ddd_kms::{Algorithm, Error, Key, KeyManagementService, VaultTransitService};
use std::sync::Arc;

use ascetic_ddd_session::rest::RestSession;
use ascetic_ddd_session::{RestSessionPool, Session, SessionPool};

fn key() -> Key {
    Algorithm::Aes256Gcm.generate_key().unwrap()
}

struct Fixture {
    sessions: RestSessionPool<reqwest::Client>,
    kms: Arc<VaultTransitService>,
    tenants: Vec<String>,
}

/// Tenants named after the test, so tests run in parallel; their keys are
/// deleted when the fixture is dropped.
fn fixture(name: &str) -> Fixture {
    let addr = std::env::var("ASCETIC_DDD_TEST_VAULT_ADDR")
        .unwrap_or_else(|_| "http://localhost:8200".to_owned());
    let token = std::env::var("ASCETIC_DDD_TEST_VAULT_TOKEN")
        .unwrap_or_else(|_| "test-root-token".to_owned());
    Fixture {
        sessions: RestSessionPool::new(reqwest::Client::new()),
        kms: Arc::new(VaultTransitService::new(addr, token)),
        tenants: vec![format!("test-{name}-1"), format!("test-{name}-2")],
    }
}

impl Fixture {
    fn tenant(&self, n: usize) -> String {
        self.tenants[n].clone()
    }

    /// Runs `scope` in one scope of the session, with the fixture's service.
    async fn atomic<T, Fut>(
        &self,
        scope: impl FnOnce(RestSession<reqwest::Client>, Arc<VaultTransitService>) -> Fut + Send,
    ) -> Result<T, Error>
    where
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        self.atomic_with(Arc::clone(&self.kms), scope).await
    }

    /// The same with another service, such as one with the wrong token.
    async fn atomic_with<T, Fut>(
        &self,
        kms: Arc<VaultTransitService>,
        scope: impl FnOnce(RestSession<reqwest::Client>, Arc<VaultTransitService>) -> Fut + Send,
    ) -> Result<T, Error>
    where
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        self.sessions
            .session(async |session| session.atomic(async |tx| scope(tx, kms).await).await)
            .await
    }

    async fn cleanup(&self) {
        for tenant in &self.tenants {
            self.atomic(|tx, kms| async move { kms.delete_kek(&tx, tenant).await })
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_dek_wrapped_after_a_rotation_unwraps() {
    let f = fixture("rotate_and_wrap");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        let dek = key();
        let wrapped = kms.encrypt_dek(&tx, tenant_0.as_str(), &dek).await?;
        assert!(wrapped.starts_with(b"vault:v1:"));
        assert_eq!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped).await?,
            dek
        );
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_generated_dek_is_thirty_two_bytes_and_unwraps() {
    let f = fixture("generate_dek");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        let (dek, wrapped) = kms.generate_dek(&tx, tenant_0.as_str()).await?;
        assert_eq!(dek.as_bytes().len(), 32);
        assert_eq!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped).await?,
            dek
        );
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn each_rotation_is_the_next_version() {
    let f = fixture("rotation_versions");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        assert_eq!(kms.rotate_kek(&tx, tenant_0.as_str()).await?, 1);
        assert_eq!(kms.rotate_kek(&tx, tenant_0.as_str()).await?, 2);
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn what_an_earlier_version_wrapped_still_unwraps() {
    let f = fixture("unwrap_after_rotation");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        let dek = key();
        let wrapped_v1 = kms.encrypt_dek(&tx, tenant_0.as_str(), &dek).await?;
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        assert_eq!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped_v1).await?,
            dek
        );
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_rewrap_moves_a_dek_to_the_current_version() {
    let f = fixture("rewrap");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        let dek = key();
        let wrapped_v1 = kms.encrypt_dek(&tx, tenant_0.as_str(), &dek).await?;
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        let wrapped_v2 = kms.rewrap_dek(&tx, tenant_0.as_str(), &wrapped_v1).await?;
        assert_ne!(wrapped_v1, wrapped_v2);
        assert!(wrapped_v1.starts_with(b"vault:v1:"));
        assert!(wrapped_v2.starts_with(b"vault:v2:"));
        assert_eq!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped_v2).await?,
            dek
        );
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn deleting_the_key_shreds_what_it_wrapped() {
    let f = fixture("crypto_shredding");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        let wrapped = kms.encrypt_dek(&tx, tenant_0.as_str(), &key()).await?;
        kms.delete_kek(&tx, tenant_0.as_str()).await?;
        assert!(matches!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped).await,
            Err(Error::Vault { status: 400, .. })
        ));
        // deleting again is not an error
        kms.delete_kek(&tx, tenant_0.as_str()).await?;
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn the_first_contact_makes_the_key() {
    let f = fixture("first_contact");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        let dek = key();
        let wrapped = kms.encrypt_dek(&tx, tenant_0.as_str(), &dek).await?;
        assert_eq!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped).await?,
            dek
        );
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn one_tenant_s_key_does_not_unwrap_another_s_dek() {
    let f = fixture("tenant_isolation");
    let (tenant_0, tenant_1) = (f.tenant(0), f.tenant(1));
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        kms.rotate_kek(&tx, tenant_1.as_str()).await?;
        let dek = key();
        let wrapped = kms.encrypt_dek(&tx, tenant_0.as_str(), &dek).await?;
        assert_eq!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped).await?,
            dek
        );
        assert!(matches!(
            kms.decrypt_dek(&tx, tenant_1.as_str(), &wrapped).await,
            Err(Error::Vault { status: 400, .. })
        ));
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_wrong_token_is_refused_by_vault() {
    let f = fixture("wrong_token");
    let tenant_0 = f.tenant(0);
    let kms = VaultTransitService::new(
        std::env::var("ASCETIC_DDD_TEST_VAULT_ADDR")
            .unwrap_or_else(|_| "http://localhost:8200".to_owned()),
        "not-the-token",
    );
    let outcome = f
        .atomic_with(Arc::new(kms), |tx, kms| async move {
            kms.rotate_kek(&tx, tenant_0.as_str()).await
        })
        .await;
    assert!(
        matches!(outcome, Err(Error::Vault { status: 403, .. })),
        "{outcome:?}"
    );
}
