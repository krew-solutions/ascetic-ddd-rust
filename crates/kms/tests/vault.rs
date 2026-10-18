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

use ascetic_ddd_kms::{
    Algorithm, Error, KekOptions, Key, KeyCustody, KeyManagementService, VaultTransitService,
};
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
    addr: String,
    token: String,
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
        kms: Arc::new(VaultTransitService::new(addr.clone(), token.clone())),
        tenants: vec![format!("test-{name}-1"), format!("test-{name}-2")],
        addr,
        token,
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
        kms.create_kek(&tx, tenant_0.as_str(), &KekOptions::default())
            .await?;
        kms.rotate_kek(&tx, tenant_0.as_str()).await?;
        let dek = key();
        let wrapped = kms.encrypt_dek(&tx, tenant_0.as_str(), &dek).await?;
        assert!(wrapped.starts_with(b"vault:v2:"));
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
        kms.create_kek(&tx, tenant_0.as_str(), &KekOptions::default())
            .await?;
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
        assert_eq!(
            kms.create_kek(&tx, tenant_0.as_str(), &KekOptions::default())
                .await?,
            1
        );
        assert_eq!(kms.rotate_kek(&tx, tenant_0.as_str()).await?, 2);
        assert_eq!(kms.rotate_kek(&tx, tenant_0.as_str()).await?, 3);
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}

/// A rotation never makes the first version: Transit refuses one of a key
/// it has not got, and the adapter no longer makes it first (ADR-0020).
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_rotation_of_a_tenant_without_a_kek_makes_none() {
    let f = fixture("rotation_of_nothing");
    let tenant_0 = f.tenant(0);
    let outcome = f
        .atomic(|tx, kms| async move { kms.rotate_kek(&tx, tenant_0.as_str()).await })
        .await;
    assert!(
        matches!(
            outcome,
            Err(Error::KekNotFound {
                key_version: None,
                ..
            })
        ),
        "{outcome:?}"
    );
    f.cleanup().await;
}

/// A retirement reads the floor first: a stale bound writes nothing —
/// Transit would lower the floor and bring a retired version back — and a
/// bound above the latest is refused, nothing retired.
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_retirement_never_lowers_the_floor_nor_passes_the_latest() {
    let f = fixture("retire_bounds");
    let tenant_0 = f.tenant(0);
    let outcome = f
        .atomic(|tx, kms| async move {
            kms.create_kek(&tx, tenant_0.as_str(), &KekOptions::default())
                .await?;
            let wrapped_v1 = kms.encrypt_dek(&tx, tenant_0.as_str(), &key()).await?;
            kms.rotate_kek(&tx, tenant_0.as_str()).await?;
            kms.rotate_kek(&tx, tenant_0.as_str()).await?;
            kms.retire_kek_versions_before(&tx, tenant_0.as_str(), 2)
                .await?;
            kms.retire_kek_versions_before(&tx, tenant_0.as_str(), 1)
                .await?;
            let above = kms
                .retire_kek_versions_before(&tx, tenant_0.as_str(), 4)
                .await;
            Ok((
                kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped_v1).await,
                above,
            ))
        })
        .await
        .unwrap();
    assert!(
        matches!(outcome.0, Err(Error::NoKeyOfVersion(1))),
        "{:?}",
        outcome.0
    );
    assert!(
        matches!(outcome.1, Err(Error::NoKeyOfVersion(4))),
        "{:?}",
        outcome.1
    );
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn what_an_earlier_version_wrapped_still_unwraps() {
    let f = fixture("unwrap_after_rotation");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        kms.create_kek(&tx, tenant_0.as_str(), &KekOptions::default())
            .await?;
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
        kms.create_kek(&tx, tenant_0.as_str(), &KekOptions::default())
            .await?;
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
        kms.create_kek(&tx, tenant_0.as_str(), &KekOptions::default())
            .await?;
        let wrapped = kms.encrypt_dek(&tx, tenant_0.as_str(), &key()).await?;
        kms.delete_kek(&tx, tenant_0.as_str()).await?;
        // Transit's 400 for a gone key, read as such by one read of the key.
        assert!(matches!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped).await,
            Err(Error::KekNotFound { .. })
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
        kms.create_kek(&tx, tenant_0.as_str(), &KekOptions::default())
            .await?;
        kms.create_kek(&tx, tenant_1.as_str(), &KekOptions::default())
            .await?;
        let dek = key();
        let wrapped = kms.encrypt_dek(&tx, tenant_0.as_str(), &dek).await?;
        assert_eq!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped).await?,
            dek
        );
        // Another tenant's key, there and serving the version: Transit's
        // 400 is the ciphertext's own fault, read as such.
        assert!(matches!(
            kms.decrypt_dek(&tx, tenant_1.as_str(), &wrapped).await,
            Err(Error::Decrypt)
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

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_derived_key_binds_the_dek_to_its_context() {
    let f = fixture("derived_context");
    let tenant_0 = f.tenant(0);
    let lifecycle = Arc::new(
        VaultTransitService::new(f.addr.clone(), f.token.clone())
            .with_derivation_context("policy.lifecycle"),
    );
    let command = Arc::new(
        VaultTransitService::new(f.addr.clone(), f.token.clone())
            .with_derivation_context("integration.command"),
    );
    let tenant = tenant_0.clone();
    let (dek, wrapped) = f
        .atomic_with(Arc::clone(&lifecycle), |tx, kms| async move {
            kms.generate_dek(&tx, tenant.as_str()).await
        })
        .await
        .unwrap();
    // The other channel's instance, over the same key, does not open it.
    let tenant = tenant_0.clone();
    let other = wrapped.clone();
    let outcome = f
        .atomic_with(command, |tx, kms| async move {
            kms.decrypt_dek(&tx, tenant.as_str(), &other).await
        })
        .await;
    assert!(matches!(outcome, Err(Error::Decrypt)), "{outcome:?}");
    // Its own does.
    let tenant = tenant_0.clone();
    let opened = f
        .atomic_with(lifecycle, |tx, kms| async move {
            kms.decrypt_dek(&tx, tenant.as_str(), &wrapped).await
        })
        .await
        .unwrap();
    assert_eq!(opened, dek);
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn managed_keys_are_not_made_on_first_contact() {
    let f = fixture("managed_keys");
    let tenant_0 = f.tenant(0);
    let managed =
        Arc::new(VaultTransitService::new(f.addr.clone(), f.token.clone()).managed_keys());
    let tenant = tenant_0.clone();
    let outcome = f
        .atomic_with(managed, |tx, kms| async move {
            kms.generate_dek(&tx, tenant.as_str()).await
        })
        .await;
    assert!(
        matches!(outcome, Err(Error::KekNotFound { ref tenant_id, .. }) if *tenant_id == tenant_0),
        "{outcome:?}"
    );
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn custody_makes_the_key_once_and_retires_its_versions() {
    let f = fixture("custody");
    let tenant_0 = f.tenant(0);
    f.atomic(|tx, kms| async move {
        let options = KekOptions {
            derived: false,
            auto_rotate_period: None,
        };
        assert_eq!(kms.create_kek(&tx, tenant_0.as_str(), &options).await?, 1);
        assert_eq!(kms.create_kek(&tx, tenant_0.as_str(), &options).await?, 1);
        let derived = KekOptions {
            derived: true,
            auto_rotate_period: None,
        };
        assert!(matches!(
            kms.create_kek(&tx, tenant_0.as_str(), &derived).await,
            Err(Error::Conflict { .. })
        ));
        let wrapped_v1 = kms.encrypt_dek(&tx, tenant_0.as_str(), &key()).await?;
        assert_eq!(kms.rotate_kek(&tx, tenant_0.as_str()).await?, 2);
        kms.retire_kek_versions_before(&tx, tenant_0.as_str(), 2)
            .await?;
        assert!(matches!(
            kms.decrypt_dek(&tx, tenant_0.as_str(), &wrapped_v1).await,
            Err(Error::NoKeyOfVersion(1))
        ));
        Ok(())
    })
    .await
    .unwrap();
    f.cleanup().await;
}
