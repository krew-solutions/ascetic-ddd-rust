//! The signing and MAC ports on Vault Transit, against a dev server with the
//! `transit` engine enabled, as `tests/vault.rs`; the custody's rules in the
//! body every adapter runs (`common/custody.rs`), and a holder's token that
//! signs and MACs and holds no custody.

#![cfg(feature = "reqwest")]

use std::sync::Arc;

use std::num::NonZeroU32;

use ascetic_ddd_kms::{
    Error, KeyLabel, KeyOptions, MacKeyCustody, MacService, PublicKey, PublicKeyService, Signature,
    SignatureAlgorithm, SigningKey, SigningKeyCustody, SigningService, VaultTransitService,
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

fn version(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

impl Fixture {
    /// One call to Vault under the fixture's (root) token, as an operator
    /// makes it outside the port; the answer's body, `null` for none.
    async fn operator(
        &self,
        method: reqwest::Method,
        path: &str,
        body: serde_json::Value,
    ) -> serde_json::Value {
        let response = reqwest::Client::new()
            .request(method, format!("{}/v1/{path}", self.addr))
            .header("X-Vault-Token", &self.token)
            .json(&body)
            .send()
            .await
            .expect("vault answers");
        assert!(
            response.status().is_success(),
            "{path}: {}",
            response.status()
        );
        response.json().await.unwrap_or(serde_json::Value::Null)
    }

    /// A token under `policy` alone, for ten minutes.
    async fn token_under(&self, policy: &str) -> String {
        self.operator(
            reqwest::Method::POST,
            "auth/token/create",
            serde_json::json!({ "policies": [policy], "ttl": "10m", "no_default_policy": true }),
        )
        .await["auth"]["client_token"]
            .as_str()
            .expect("a token")
            .to_owned()
    }

    /// The same service under another token.
    fn under(&self, token: &str) -> Arc<VaultTransitService> {
        Arc::new(VaultTransitService::new(
            self.addr.clone(),
            token.to_owned(),
        ))
    }

    async fn atomic_as<T, Fut>(
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
}

/// After a rotation the earlier version signs when it is named, and what
/// it signs verifies through the port and under its public half; `sign`
/// takes the newer.
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn after_a_rotation_the_earlier_version_signs_when_it_is_named() {
    let f = fixture("pinned");
    let tenant = f.tenant.clone();
    f.atomic(|tx, kms| async move {
        kms.create_signing_key(&tx, &tenant, P256, &KeyOptions::default())
            .await?;
        kms.rotate_signing_key(&tx, &tenant, P256).await
    })
    .await
    .unwrap();
    let tenant = f.tenant.clone();
    let (pinned, latest, keyring) = f
        .atomic(|tx, kms| async move {
            let pinned = kms
                .sign_with_version(&tx, &tenant, P256, version(1), b"hello")
                .await?;
            kms.verify(&tx, &tenant, b"hello", &pinned).await?;
            let latest = kms.sign(&tx, &tenant, P256, b"hello").await?;
            let keyring = kms.public_keys(&tx, &tenant, P256).await?;
            Ok((pinned, latest, keyring))
        })
        .await
        .unwrap();
    assert_eq!(pinned.key_version(), 1);
    assert_eq!(latest.key_version(), 2);
    keyring.get(1).unwrap().verify(b"hello", &pinned).unwrap();
    keyring.get(2).unwrap().verify(b"hello", &latest).unwrap();
    f.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_pinned_signature_makes_no_key() {
    let f = fixture("pinned-nothing");
    let tenant = f.tenant.clone();
    let outcome = f
        .atomic(|tx, kms| async move {
            kms.sign_with_version(&tx, &tenant, ED25519, version(1), b"hello")
                .await
        })
        .await;
    assert!(
        matches!(
            outcome,
            Err(Error::SigningKeyNotFound {
                algorithm: ED25519,
                ..
            })
        ),
        "{outcome:?}"
    );
    let tenant = f.tenant.clone();
    let outcome = f
        .atomic(|tx, kms| async move { kms.public_keys(&tx, &tenant, ED25519).await })
        .await;
    assert!(
        matches!(
            outcome,
            Err(Error::SigningKeyNotFound {
                algorithm: ED25519,
                ..
            })
        ),
        "the key is still not there: {outcome:?}"
    );
    f.cleanup().await;
}

/// A version beyond the latest, one retired, and one below the floor the
/// key signs from are each `NoKeyOfVersion`; what the newer version signed
/// before still verifies; the keyring is the versions that verify.
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_version_the_key_does_not_sign_with_is_named() {
    let f = fixture("pinned-refused");
    let name = SigningKey::name_of(&f.tenant, ED25519);
    let tenant = f.tenant.clone();
    let earlier = f
        .atomic(|tx, kms| async move {
            kms.create_signing_key(&tx, &tenant, ED25519, &KeyOptions::default())
                .await?;
            kms.rotate_signing_key(&tx, &tenant, ED25519).await?;
            kms.rotate_signing_key(&tx, &tenant, ED25519).await?;
            kms.sign_with_version(&tx, &tenant, ED25519, version(2), b"hello")
                .await
        })
        .await
        .unwrap();
    let refused = |n: u32| {
        let tenant = f.tenant.clone();
        f.atomic(move |tx, kms| async move {
            kms.sign_with_version(&tx, &tenant, ED25519, version(n), b"hello")
                .await
        })
    };
    let outcome = refused(4).await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(4))),
        "{outcome:?}"
    );
    f.operator(
        reqwest::Method::POST,
        &format!("transit/keys/{name}/config"),
        serde_json::json!({ "min_decryption_version": 2 }),
    )
    .await;
    let outcome = refused(1).await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(1))),
        "{outcome:?}"
    );
    f.operator(
        reqwest::Method::POST,
        &format!("transit/keys/{name}/config"),
        serde_json::json!({ "min_encryption_version": 3 }),
    )
    .await;
    let outcome = refused(2).await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(2))),
        "{outcome:?}"
    );
    let tenant = f.tenant.clone();
    let keyring = f
        .atomic(|tx, kms| async move {
            kms.verify(&tx, &tenant, b"hello", &earlier).await?;
            kms.public_keys(&tx, &tenant, ED25519).await
        })
        .await
        .unwrap();
    assert_eq!(keyring.iter().map(|(v, _)| v).collect::<Vec<_>>(), [2, 3]);
    f.cleanup().await;
}

/// The keyring is Transit's own publication, read into the model's form,
/// for both algorithms, and verifies what Vault signed.
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn the_keyring_is_what_transit_publishes_for_both_algorithms() {
    let f = fixture("keyring");
    for algorithm in [ED25519, P256] {
        let tenant = f.tenant.clone();
        let (signature, keyring) = f
            .atomic(|tx, kms| async move {
                let signature = kms.sign(&tx, &tenant, algorithm, b"hello").await?;
                let keyring = kms.public_keys(&tx, &tenant, algorithm).await?;
                Ok((signature, keyring))
            })
            .await
            .unwrap();
        let published = public_key_transit_publishes(&f, algorithm, 1).await;
        assert_eq!(keyring.get(1), Some(&published), "{algorithm}");
        published.verify(b"hello", &signature).unwrap();
    }
    f.cleanup().await;
}

/// ADR-0010's grant — `update` on the key's `sign` path and `read` on the
/// key, nothing else: the pinned signature and the keyring are served, the
/// latest-signing's ensure and a rotation are refused; a token that may
/// only read is `Forbidden` to sign.
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_token_that_may_only_sign_and_read_signs_at_the_version_named() {
    let f = fixture("least-privilege");
    let name = SigningKey::name_of(&f.tenant, P256);
    let tenant = f.tenant.clone();
    f.atomic(|tx, kms| async move {
        kms.create_signing_key(&tx, &tenant, P256, &KeyOptions::default())
            .await
    })
    .await
    .unwrap();
    let mint = format!("{}-mint", f.tenant);
    let read = format!("{}-read", f.tenant);
    f.operator(
        reqwest::Method::PUT,
        &format!("sys/policies/acl/{mint}"),
        serde_json::json!({ "policy": format!(
            "path \"transit/sign/{name}\" {{ capabilities = [\"update\"] }}\npath \"transit/keys/{name}\" {{ capabilities = [\"read\"] }}\n"
        ) }),
    )
    .await;
    f.operator(
        reqwest::Method::PUT,
        &format!("sys/policies/acl/{read}"),
        serde_json::json!({ "policy": format!(
            "path \"transit/keys/{name}\" {{ capabilities = [\"read\"] }}\n"
        ) }),
    )
    .await;
    let minting = f.token_under(&mint).await;
    let reading = f.token_under(&read).await;

    let tenant = f.tenant.clone();
    let (pinned, keyring) = f
        .atomic_as(f.under(&minting), |tx, kms| async move {
            let pinned = kms
                .sign_with_version(&tx, &tenant, P256, version(1), b"hello")
                .await?;
            let keyring = kms.public_keys(&tx, &tenant, P256).await?;
            Ok((pinned, keyring))
        })
        .await
        .unwrap();
    keyring.get(1).unwrap().verify(b"hello", &pinned).unwrap();
    let tenant = f.tenant.clone();
    let latest = f
        .atomic_as(f.under(&minting), |tx, kms| async move {
            kms.sign(&tx, &tenant, P256, b"hello").await
        })
        .await;
    assert!(
        matches!(latest, Err(Error::Vault { status: 403, .. })),
        "the ensure of `sign` is refused: {latest:?}"
    );
    let tenant = f.tenant.clone();
    let managed = f
        .atomic_as(
            Arc::new(VaultTransitService::new(f.addr.clone(), minting.clone()).managed_keys()),
            |tx, kms| async move { kms.sign(&tx, &tenant, P256, b"hello").await },
        )
        .await
        .unwrap();
    assert_eq!(
        managed.key_version(),
        1,
        "managed, the latest-signing runs under the minting token"
    );
    let tenant = f.tenant.clone();
    let rotation = f
        .atomic_as(f.under(&minting), |tx, kms| async move {
            kms.rotate_signing_key(&tx, &tenant, P256).await
        })
        .await;
    assert!(
        matches!(
            rotation,
            Err(Error::Forbidden {
                operation: "rotate",
                ..
            })
        ),
        "{rotation:?}"
    );
    let tenant = f.tenant.clone();
    let reader = f
        .atomic_as(f.under(&reading), |tx, kms| async move {
            kms.sign_with_version(&tx, &tenant, P256, version(1), b"hello")
                .await
        })
        .await;
    assert!(
        matches!(
            reader,
            Err(Error::Forbidden {
                operation: "sign",
                ..
            })
        ),
        "{reader:?}"
    );

    for token in [minting, reading] {
        f.operator(
            reqwest::Method::POST,
            "auth/token/revoke",
            serde_json::json!({ "token": token }),
        )
        .await;
    }
    for policy in [mint, read] {
        f.operator(
            reqwest::Method::DELETE,
            &format!("sys/policies/acl/{policy}"),
            serde_json::Value::Null,
        )
        .await;
    }
    f.cleanup().await;
}

#[path = "common/custody.rs"]
mod custody;

/// The fixture's Vault under a service built `managed_keys()`.
struct Managed(Fixture, Arc<VaultTransitService>);

impl Managed {
    fn of(f: Fixture) -> Self {
        let kms =
            Arc::new(VaultTransitService::new(f.addr.clone(), f.token.clone()).managed_keys());
        Managed(f, kms)
    }
}

impl custody::Scopes for Managed {
    type Session = RestSession<reqwest::Client>;
    type Keys = VaultTransitService;

    async fn atomic<T, Fut>(
        &self,
        scope: impl FnOnce(RestSession<reqwest::Client>, Arc<VaultTransitService>) -> Fut + Send,
    ) -> Result<T, Error>
    where
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        self.0.atomic_as(Arc::clone(&self.1), scope).await
    }
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn the_custody_of_signing_keys_keeps_the_rules() {
    let managed = Managed::of(fixture("custody-sign"));
    custody::signing_custody(&managed, &managed.0.tenant).await;
    managed.0.cleanup().await;
}

#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn the_custody_of_mac_keys_keeps_the_rules() {
    let managed = Managed::of(fixture("custody-mac"));
    custody::mac_custody(&managed, &managed.0.tenant).await;
    managed.0.cleanup().await;
}

/// A key at the name of another type is a conflict, not a key taken for
/// what was asked: Transit leaves it as it is and says nothing (measured,
/// Vault 1.21).
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_key_of_another_type_at_the_name_is_a_conflict() {
    let f = fixture("custody-conflict");
    let name = SigningKey::name_of(&f.tenant, ED25519);
    f.operator(
        reqwest::Method::POST,
        &format!("transit/keys/{name}"),
        serde_json::json!({ "type": "ecdsa-p256" }),
    )
    .await;
    let tenant = f.tenant.clone();
    let outcome = f
        .atomic(|tx, kms| async move {
            kms.create_signing_key(&tx, &tenant, ED25519, &KeyOptions::default())
                .await
        })
        .await;
    assert!(
        matches!(outcome, Err(Error::Conflict { .. })),
        "{outcome:?}"
    );
    f.cleanup().await;
}

/// A key's rotation period is set where it is made, and set again only when
/// asked for another.
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_rotation_period_asked_is_the_key_s() {
    let f = fixture("custody-period");
    let tenant = f.tenant.clone();
    let hours = |n: u64| KeyOptions {
        auto_rotate_period: Some(std::time::Duration::from_secs(n * 3600)),
    };
    let etag = KeyLabel::new("etag").unwrap();
    f.atomic(|tx, kms| async move {
        kms.create_mac_key(&tx, &tenant, &etag, &hours(720)).await?;
        kms.create_mac_key(&tx, &tenant, &etag, &hours(24)).await
    })
    .await
    .unwrap();
    let name = format!("{}.mac.etag", f.tenant);
    let answer = f
        .operator(
            reqwest::Method::GET,
            &format!("transit/keys/{name}"),
            serde_json::Value::Null,
        )
        .await;
    assert_eq!(answer["data"]["auto_rotate_period"], 24 * 3600);
    f.cleanup().await;
}

/// A holder's grant — `read` on the keys, `update` on `sign` and `hmac`:
/// built `managed_keys()`, it signs and MACs with the keys custody made; a
/// tenant without them is named, not a `403`; every custody operation is
/// `Forbidden`.
#[tokio::test]
#[ignore = "needs a Vault dev server"]
async fn a_holder_s_token_signs_and_macs_and_holds_no_custody() {
    let f = fixture("holder");
    let etag = KeyLabel::new("etag").unwrap();
    let tenant = f.tenant.clone();
    {
        let etag = etag.clone();
        f.atomic(|tx, kms| async move {
            kms.create_signing_key(&tx, &tenant, ED25519, &KeyOptions::default())
                .await?;
            kms.rotate_signing_key(&tx, &tenant, ED25519).await?;
            kms.create_mac_key(&tx, &tenant, &etag, &KeyOptions::default())
                .await
        })
        .await
        .unwrap();
    }
    let holder = format!("{}-holder", f.tenant);
    f.operator(
        reqwest::Method::PUT,
        &format!("sys/policies/acl/{holder}"),
        serde_json::json!({ "policy": concat!(
            "path \"transit/keys/+\" { capabilities = [\"read\"] }\n",
            "path \"transit/sign/+\" { capabilities = [\"update\"] }\n",
            "path \"transit/hmac/+\" { capabilities = [\"update\"] }\n",
        ) }),
    )
    .await;
    let token = f.token_under(&holder).await;
    let kms = Arc::new(VaultTransitService::new(f.addr.clone(), token.clone()).managed_keys());

    let tenant = f.tenant.clone();
    let (signature, used) = {
        let etag = etag.clone();
        f.atomic_as(Arc::clone(&kms), |tx, kms| async move {
            let signature = kms.sign(&tx, &tenant, ED25519, b"hello").await?;
            let mac = kms.mac(&tx, &tenant, &etag, b"hello").await?;
            let versions = (signature.key_version(), mac.key_version());
            Ok((signature, versions))
        })
        .await
        .unwrap()
    };
    assert_eq!(used, (2, 1));

    let nobody = format!("{}-nobody", f.tenant);
    let unsigned = {
        let nobody = nobody.clone();
        f.atomic_as(Arc::clone(&kms), |tx, kms| async move {
            kms.sign(&tx, &nobody, ED25519, b"hello").await
        })
        .await
    };
    assert!(
        matches!(unsigned, Err(Error::SigningKeyNotFound { .. })),
        "{unsigned:?}"
    );
    let unmade = {
        let (nobody, etag) = (nobody.clone(), etag.clone());
        f.atomic_as(Arc::clone(&kms), |tx, kms| async move {
            kms.mac(&tx, &nobody, &etag, b"hello").await
        })
        .await
    };
    assert!(
        matches!(unmade, Err(Error::MacKeyNotFound { .. })),
        "{unmade:?}"
    );

    let tenant = f.tenant.clone();
    let refused = {
        let etag = etag.clone();
        f.atomic_as(Arc::clone(&kms), |tx, kms| async move {
            let options = KeyOptions::default();
            Ok([
                // Verifying through the KMS is `verify`, which this policy
                // leaves out: a holder that signs holds no verify by that.
                kms.verify(&tx, &tenant, b"hello", &signature).await,
                kms.create_signing_key(&tx, &format!("{tenant}-new"), ED25519, &options)
                    .await
                    .map(|_| ()),
                kms.rotate_signing_key(&tx, &tenant, ED25519)
                    .await
                    .map(|_| ()),
                kms.retire_signing_versions_before(&tx, &tenant, ED25519, 2)
                    .await,
                kms.delete_signing_key(&tx, &tenant, ED25519).await,
                kms.rotate_mac_key(&tx, &tenant, &etag).await.map(|_| ()),
                kms.delete_mac_key(&tx, &tenant, &etag).await,
            ])
        })
        .await
        .unwrap()
    };
    assert!(
        matches!(
            refused[0],
            Err(Error::Forbidden {
                operation: "verify",
                ..
            })
        ),
        "verify under the holder's token: {:?}",
        refused[0]
    );
    for (n, outcome) in refused.iter().enumerate() {
        assert!(
            matches!(outcome, Err(Error::Forbidden { .. })),
            "operation {n} under the holder's token: {outcome:?}"
        );
    }

    f.operator(
        reqwest::Method::POST,
        "auth/token/revoke",
        serde_json::json!({ "token": token }),
    )
    .await;
    f.operator(
        reqwest::Method::DELETE,
        &format!("sys/policies/acl/{holder}"),
        serde_json::Value::Null,
    )
    .await;
    f.cleanup().await;
}
