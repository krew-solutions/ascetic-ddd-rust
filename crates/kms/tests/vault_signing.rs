//! The signing and MAC ports on Vault Transit, against a dev server with the
//! `transit` engine enabled, as `tests/vault.rs`.

#![cfg(feature = "reqwest")]

use std::sync::Arc;

use ascetic_ddd_kms::{
    Error, KeyLabel, MacService, PublicKey, Signature, SignatureAlgorithm, SigningKey,
    SigningService, VaultTransitService,
};
use ascetic_ddd_session::rest::RestSession;
use ascetic_ddd_session::{RestSessionPool, Session, SessionPool};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use p256::pkcs8::DecodePublicKey as _;

const ED25519: SignatureAlgorithm = SignatureAlgorithm::Ed25519;
const P256: SignatureAlgorithm = SignatureAlgorithm::EcdsaP256;

struct Fixture {
    sessions: RestSessionPool<reqwest::Client>,
    kms: Arc<VaultTransitService>,
    tenant: String,
    addr: String,
    token: String,
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
        kms: Arc::new(VaultTransitService::new(addr.clone(), token.clone())),
        tenant: format!("test-sign-{name}"),
        addr,
        token,
    }
}

/// The public key of the tenant's key of the algorithm, as Transit
/// publishes it, read as the model's: the raw bytes in base64 for Ed25519,
/// a PEM SubjectPublicKeyInfo for P-256.
async fn public_key_transit_publishes(
    f: &Fixture,
    algorithm: SignatureAlgorithm,
    version: u32,
) -> PublicKey {
    let name = SigningKey::name_of(&f.tenant, algorithm);
    let answer: serde_json::Value = reqwest::Client::new()
        .get(format!("{}/v1/transit/keys/{name}", f.addr))
        .header("X-Vault-Token", &f.token)
        .send()
        .await
        .expect("vault answers")
        .json()
        .await
        .expect("vault answers json");
    let text = answer["data"]["keys"][version.to_string().as_str()]["public_key"]
        .as_str()
        .expect("a public key")
        .to_owned();
    let bytes = match algorithm {
        SignatureAlgorithm::Ed25519 => BASE64.decode(text).expect("the public key is base64"),
        SignatureAlgorithm::EcdsaP256 => {
            // The PEM without its armor is the DER of a SubjectPublicKeyInfo,
            // base64; the key inside is the SEC1 point the model holds.
            let body: String = text
                .lines()
                .filter(|line| !line.starts_with("-----"))
                .collect();
            let der = BASE64.decode(body).expect("the PEM body is base64");
            p256::ecdsa::VerifyingKey::from_public_key_der(&der)
                .expect("a P-256 key")
                .to_sec1_bytes()
                .to_vec()
        }
        // The enum is open: an algorithm this test does not know how to
        // read is not one it signs with.
        other => panic!("no reading of a {other} public key here"),
    };
    PublicKey::new(algorithm, bytes).expect("the model's key")
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

/// A signature Vault made, in the byte form the port fixes, verifies in this
/// process under the public key Transit publishes, read as the model's: the
/// two adapters' forms are one, for both algorithms.
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_signature_vault_made_verifies_under_the_public_key_it_publishes() {
    let f = fixture("cross-adapter");
    for algorithm in [ED25519, P256] {
        let tenant = f.tenant.clone();
        let signature = f
            .atomic(|tx, kms| async move { kms.sign(&tx, &tenant, algorithm, b"hello").await })
            .await
            .unwrap();
        let public = public_key_transit_publishes(&f, algorithm, signature.key_version()).await;
        public
            .verify(b"hello", &signature)
            .unwrap_or_else(|error| panic!("{algorithm}: {error}"));
        assert!(
            matches!(
                public.verify(b"hellp", &signature),
                Err(Error::InvalidSignature)
            ),
            "{algorithm}: another message"
        );
    }
    f.cleanup().await;
}
