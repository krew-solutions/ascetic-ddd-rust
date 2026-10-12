//! The signing and MAC ports on Vault Transit, against a dev server with the
//! `transit` engine enabled, as `tests/vault.rs`.

#![cfg(feature = "reqwest")]

use std::sync::Arc;

use ascetic_ddd_kms::{
    Error, KeyLabel, MacService, Signature, SignatureAlgorithm, SigningService, VaultTransitService,
};
use ascetic_ddd_session::rest::RestSession;
use ascetic_ddd_session::{RestSessionPool, Session, SessionPool};

const ED25519: SignatureAlgorithm = SignatureAlgorithm::Ed25519;
const P256: SignatureAlgorithm = SignatureAlgorithm::EcdsaP256;

struct Fixture {
    sessions: RestSessionPool<reqwest::Client>,
    kms: Arc<VaultTransitService>,
    tenant: String,
}

/// A tenant named after the test, so tests run in parallel; its keys are
/// deleted by `cleanup`.
fn fixture(name: &str) -> Fixture {
    let addr = std::env::var("ASCETIC_DDD_TEST_VAULT_ADDR")
        .unwrap_or_else(|_| "http://localhost:8200".to_owned());
    let token = std::env::var("ASCETIC_DDD_TEST_VAULT_TOKEN")
        .unwrap_or_else(|_| "test-root-token".to_owned());
    Fixture {
        sessions: RestSessionPool::new(reqwest::Client::new()),
        kms: Arc::new(VaultTransitService::new(addr, token)),
        tenant: format!("test-sign-{name}"),
    }
}

impl Fixture {
    async fn atomic<T, Fut>(
        &self,
        scope: impl FnOnce(RestSession<reqwest::Client>, Arc<VaultTransitService>) -> Fut + Send,
    ) -> Result<T, Error>
    where
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let kms = Arc::clone(&self.kms);
        self.sessions
            .session(async |session| session.atomic(async |tx| scope(tx, kms).await).await)
            .await
    }

    async fn cleanup(&self) {
        let tenant = self.tenant.clone();
        self.atomic(|tx, kms| async move {
            kms.delete_signing_key(&tx, &tenant, ED25519).await?;
            kms.delete_signing_key(&tx, &tenant, P256).await?;
            kms.delete_mac_key(&tx, &tenant, &KeyLabel::new("etag").unwrap())
                .await
        })
        .await
        .unwrap();
    }
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_signature_verifies_and_a_changed_message_does_not() {
    let f = fixture("roundtrip");
    let tenant = f.tenant.clone();
    let signature = f
        .atomic(|tx, kms| async move { kms.sign(&tx, &tenant, ED25519, b"hello").await })
        .await
        .unwrap();
    assert_eq!(signature.algorithm(), ED25519);
    assert_eq!(signature.key_version(), 1);
    assert_eq!(signature.as_bytes().len(), 64);
    let tenant = f.tenant.clone();
    let good = signature.clone();
    f.atomic(|tx, kms| async move { kms.verify(&tx, &tenant, b"hello", &good).await })
        .await
        .unwrap();
    let tenant = f.tenant.clone();
    let outcome = f
        .atomic(|tx, kms| async move { kms.verify(&tx, &tenant, b"hellp", &signature).await })
        .await;
    assert!(
        matches!(outcome, Err(Error::InvalidSignature)),
        "{outcome:?}"
    );
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn an_ecdsa_signature_is_raw_r_and_s() {
    let f = fixture("p256");
    let tenant = f.tenant.clone();
    let signature = f
        .atomic(|tx, kms| async move { kms.sign(&tx, &tenant, P256, b"hello").await })
        .await
        .unwrap();
    assert_eq!(signature.as_bytes().len(), 64, "not DER");
    let tenant = f.tenant.clone();
    f.atomic(|tx, kms| async move { kms.verify(&tx, &tenant, b"hello", &signature).await })
        .await
        .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn what_an_earlier_version_signed_still_verifies_after_a_rotation() {
    let f = fixture("rotation");
    let tenant = f.tenant.clone();
    let first = f
        .atomic(|tx, kms| async move { kms.sign(&tx, &tenant, ED25519, b"hello").await })
        .await
        .unwrap();
    let tenant = f.tenant.clone();
    let version = f
        .atomic(|tx, kms| async move { kms.rotate_signing_key(&tx, &tenant, ED25519).await })
        .await
        .unwrap();
    assert_eq!(version, 2);
    let tenant = f.tenant.clone();
    let second = f
        .atomic(|tx, kms| async move { kms.sign(&tx, &tenant, ED25519, b"hello").await })
        .await
        .unwrap();
    assert_eq!(second.key_version(), 2);
    let tenant = f.tenant.clone();
    f.atomic(|tx, kms| async move { kms.verify(&tx, &tenant, b"hello", &first).await })
        .await
        .unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_signature_by_a_key_the_tenant_has_not_got_is_told_apart_from_a_bad_one() {
    let f = fixture("missing");
    let tenant = f.tenant.clone();
    let signature = Signature::new(ED25519, 1, vec![0u8; 64]);
    let outcome = f
        .atomic(|tx, kms| async move { kms.verify(&tx, &tenant, b"hello", &signature).await })
        .await;
    assert!(
        matches!(outcome, Err(Error::SigningKeyNotFound { ref tenant_id, algorithm: ED25519 }) if *tenant_id == f.tenant),
        "{outcome:?}"
    );
    // A version the key has not got, once the key exists.
    let tenant = f.tenant.clone();
    f.atomic(|tx, kms| async move { kms.sign(&tx, &tenant, ED25519, b"hello").await })
        .await
        .unwrap();
    let tenant = f.tenant.clone();
    let signature = Signature::new(ED25519, 9, vec![0u8; 64]);
    let outcome = f
        .atomic(|tx, kms| async move { kms.verify(&tx, &tenant, b"hello", &signature).await })
        .await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(9))),
        "{outcome:?}"
    );
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_mac_is_the_same_twice_and_another_message_s_differs() {
    let f = fixture("mac");
    let purpose = KeyLabel::new("etag").unwrap();
    let mac_of = |message: &'static [u8]| {
        let tenant = f.tenant.clone();
        let purpose = purpose.clone();
        f.atomic(move |tx, kms| async move { kms.mac(&tx, &tenant, &purpose, message).await })
    };
    let first = mac_of(b"hello").await.unwrap();
    let again = mac_of(b"hello").await.unwrap();
    let other = mac_of(b"hellp").await.unwrap();
    assert_eq!(first, again);
    assert_eq!(first.key_version(), 1);
    assert_eq!(first.as_bytes().len(), 32);
    assert_ne!(first.as_bytes(), other.as_bytes());
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn deleting_what_is_not_there_is_nothing() {
    let f = fixture("delete-nothing");
    f.cleanup().await;
}
