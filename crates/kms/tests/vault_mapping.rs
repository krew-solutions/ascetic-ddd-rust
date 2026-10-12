//! How the Vault adapter reads Transit's answers and tells its `400`s
//! apart — against a scripted transport, no server.

#![cfg(feature = "vault")]

use std::sync::{Arc, Mutex};

use ascetic_ddd_kms::vault::{VaultRequest, VaultResponse, VaultTransport};
use ascetic_ddd_kms::{
    BoxError, Error, KeyLabel, MacService, Signature, SignatureAlgorithm, SigningService,
    VaultTransitService,
};
use ascetic_ddd_session::rest::RestSession;
use ascetic_ddd_session::{RestSessionPool, Session, SessionPool};
use serde_json::{Value, json};

/// Answers each call from a script of `(method, path under the mount)` →
/// response, and records what was asked.
#[derive(Default)]
struct Scripted {
    answers: Mutex<Vec<(&'static str, &'static str, u16, Value)>>,
    asked: Mutex<Vec<(String, String, Option<Value>)>>,
}

impl Scripted {
    fn answering(script: Vec<(&'static str, &'static str, u16, Value)>) -> Arc<Self> {
        Arc::new(Scripted {
            answers: Mutex::new(script),
            asked: Mutex::new(Vec::new()),
        })
    }

    fn asked(&self) -> Vec<(String, String, Option<Value>)> {
        self.asked.lock().unwrap().clone()
    }
}

impl VaultTransport for Scripted {
    async fn send(&self, request: VaultRequest<'_>) -> Result<VaultResponse, BoxError> {
        let path = request
            .url
            .strip_prefix("http://vault/v1/transit")
            .unwrap_or(&request.url)
            .to_owned();
        self.asked
            .lock()
            .unwrap()
            .push((request.method.to_owned(), path.clone(), request.body));
        let mut answers = self.answers.lock().unwrap();
        let at = answers
            .iter()
            .position(|(method, scripted, _, _)| *method == request.method && *scripted == path)
            .unwrap_or_else(|| panic!("no answer scripted for {} {path}", request.method));
        let (_, _, status, body) = answers.remove(at);
        Ok(VaultResponse {
            status,
            body: Some(body),
        })
    }
}

async fn with<T, Fut>(
    transport: &Arc<Scripted>,
    scope: impl FnOnce(RestSession<Scripted>, VaultTransitService) -> Fut + Send,
) -> Result<T, Error>
where
    Fut: Future<Output = Result<T, Error>> + Send,
    T: Send,
{
    let pool = RestSessionPool::from_shared(Arc::clone(transport));
    let kms = VaultTransitService::new("http://vault", "t");
    pool.session(async |session| session.atomic(async |tx| scope(tx, kms).await).await)
        .await
}

const ED25519: SignatureAlgorithm = SignatureAlgorithm::Ed25519;

#[tokio::test]
async fn a_signature_is_asked_in_jws_marshaling_and_parsed_from_its_versioned_text() {
    let transport = Scripted::answering(vec![
        ("POST", "/keys/t.sign.ed25519", 200, json!({})),
        (
            "POST",
            "/sign/t.sign.ed25519",
            200,
            json!({ "data": { "key_version": 3, "signature": "vault:v3:AQID" } }),
        ),
    ]);
    let signature = with(&transport, |tx, kms| async move {
        kms.sign(&tx, "t", ED25519, b"hi").await
    })
    .await
    .unwrap();
    assert_eq!(signature, Signature::new(ED25519, 3, vec![1, 2, 3]));
    let asked = transport.asked();
    assert_eq!(asked[0].2, Some(json!({ "type": "ed25519" })));
    assert_eq!(
        asked[1].2,
        Some(json!({ "input": "aGk=", "marshaling_algorithm": "jws" }))
    );
}

#[tokio::test]
async fn an_ecdsa_signature_names_its_hash_and_marshaling_on_both_sides() {
    let transport = Scripted::answering(vec![
        ("POST", "/keys/t.sign.ecdsa-p256", 200, json!({})),
        (
            "POST",
            "/sign/t.sign.ecdsa-p256",
            200,
            json!({ "data": { "signature": "vault:v1:AQID" } }),
        ),
        (
            "POST",
            "/verify/t.sign.ecdsa-p256",
            200,
            json!({ "data": { "valid": true } }),
        ),
    ]);
    with(&transport, |tx, kms| async move {
        let signature = kms
            .sign(&tx, "t", SignatureAlgorithm::EcdsaP256, b"hi")
            .await?;
        kms.verify(&tx, "t", b"hi", &signature).await
    })
    .await
    .unwrap();
    let asked = transport.asked();
    assert_eq!(asked[0].2, Some(json!({ "type": "ecdsa-p256" })));
    assert_eq!(
        asked[1].2,
        Some(
            json!({ "input": "aGk=", "marshaling_algorithm": "jws", "hash_algorithm": "sha2-256" })
        )
    );
    assert_eq!(
        asked[2].2,
        Some(json!({
            "input": "aGk=",
            "marshaling_algorithm": "jws",
            "hash_algorithm": "sha2-256",
            "signature": "vault:v1:AQID",
        }))
    );
}

#[tokio::test]
async fn a_400_on_verify_is_explained_by_one_read_of_the_key() {
    // No such key.
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/verify/t.sign.ed25519",
            400,
            json!({ "errors": ["signature verification key not found"] }),
        ),
        ("GET", "/keys/t.sign.ed25519", 404, json!({ "errors": [] })),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.verify(&tx, "t", b"hi", &Signature::new(ED25519, 1, vec![0; 64]))
            .await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::SigningKeyNotFound { ref tenant_id, algorithm: ED25519 }) if tenant_id == "t"),
        "{outcome:?}"
    );
    // A version the key no longer serves.
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/verify/t.sign.ed25519",
            400,
            json!({ "errors": ["ciphertext or signature version is disallowed by policy (too old)"] }),
        ),
        (
            "GET",
            "/keys/t.sign.ed25519",
            200,
            json!({ "data": { "latest_version": 3, "min_decryption_version": 2 } }),
        ),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.verify(&tx, "t", b"hi", &Signature::new(ED25519, 1, vec![0; 64]))
            .await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(1))),
        "{outcome:?}"
    );
    // Any other 400 stays Vault's.
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/verify/t.sign.ed25519",
            400,
            json!({ "errors": ["something else"] }),
        ),
        (
            "GET",
            "/keys/t.sign.ed25519",
            200,
            json!({ "data": { "latest_version": 3, "min_decryption_version": 1 } }),
        ),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.verify(&tx, "t", b"hi", &Signature::new(ED25519, 1, vec![0; 64]))
            .await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::Vault { status: 400, ref message, .. }) if message == "something else"),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_signature_that_does_not_verify_is_said_so() {
    let transport = Scripted::answering(vec![(
        "POST",
        "/verify/t.sign.ed25519",
        200,
        json!({ "data": { "valid": false } }),
    )]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.verify(&tx, "t", b"hi", &Signature::new(ED25519, 1, vec![0; 64]))
            .await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::InvalidSignature)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_mac_key_is_of_the_hmac_type_and_the_mac_is_parsed_from_its_versioned_text() {
    let transport = Scripted::answering(vec![
        ("POST", "/keys/t.mac.etag", 200, json!({})),
        (
            "POST",
            "/hmac/t.mac.etag",
            200,
            json!({ "data": { "hmac": "vault:v2:AQID" } }),
        ),
    ]);
    let mac = with(&transport, |tx, kms| async move {
        kms.mac(&tx, "t", &KeyLabel::new("etag").unwrap(), b"hi")
            .await
    })
    .await
    .unwrap();
    assert_eq!(mac.key_version(), 2);
    assert_eq!(mac.as_bytes(), &[1, 2, 3]);
    let asked = transport.asked();
    assert_eq!(asked[0].2, Some(json!({ "type": "hmac", "key_size": 32 })));
    assert_eq!(
        asked[1].2,
        Some(json!({ "input": "aGk=", "algorithm": "sha2-256" }))
    );
}

#[tokio::test]
async fn a_tenant_id_that_is_not_a_key_part_is_refused_before_any_call() {
    let transport = Scripted::answering(vec![]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.sign(&tx, "a/b", ED25519, b"hi").await
    })
    .await;
    assert!(matches!(outcome, Err(Error::Malformed(_))), "{outcome:?}");
    assert!(transport.asked().is_empty());
}
