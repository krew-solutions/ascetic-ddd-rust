//! How the Vault adapter reads Transit's answers and tells its `400`s
//! apart — against a scripted transport, no server.

#![cfg(feature = "vault")]

use std::sync::{Arc, Mutex};

use ascetic_ddd_kms::vault::{VaultRequest, VaultResponse, VaultTransport};
use std::num::NonZeroU32;

use ascetic_ddd_kms::{
    BoxError, Error, KekOptions, KeyCustody, KeyLabel, KeyManagementService, MacService,
    PublicKeyService, Signature, SignatureAlgorithm, SigningService, VaultTransitService,
};
use ascetic_ddd_session::rest::RestSession;
use ascetic_ddd_session::{RestSessionPool, Session, SessionPool};
use base64::Engine as _;
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

/// The same, with a service built by the test.
async fn with_service<T, Fut>(
    transport: &Arc<Scripted>,
    kms: VaultTransitService,
    scope: impl FnOnce(RestSession<Scripted>, VaultTransitService) -> Fut + Send,
) -> Result<T, Error>
where
    Fut: Future<Output = Result<T, Error>> + Send,
    T: Send,
{
    let pool = RestSessionPool::from_shared(Arc::clone(transport));
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
    let outcome = with(&transport, |tx, kms| async move {
        kms.sign_with_version(&tx, "a/b", ED25519, version(1), b"hi")
            .await
    })
    .await;
    assert!(matches!(outcome, Err(Error::Malformed(_))), "{outcome:?}");
    let outcome = with(&transport, |tx, kms| async move {
        kms.public_keys(&tx, "a/b", ED25519).await
    })
    .await;
    assert!(matches!(outcome, Err(Error::Malformed(_))), "{outcome:?}");
    assert!(transport.asked().is_empty());
}

fn version(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

/// A pinned signature, its refusal scripted as `status` and the key's
/// description as `key` (`None`: the read answers 404).
async fn pinned(n: u32, status: u16, key: Option<Value>) -> (Result<Signature, Error>, usize) {
    let mut script = vec![(
        "POST",
        "/sign/t.sign.ed25519",
        status,
        json!({ "errors": ["refused"] }),
    )];
    match key {
        Some(key) => script.push(("GET", "/keys/t.sign.ed25519", 200, key)),
        None => script.push(("GET", "/keys/t.sign.ed25519", 404, json!({ "errors": [] }))),
    }
    let transport = Scripted::answering(script);
    let outcome = with(&transport, |tx, kms| async move {
        kms.sign_with_version(&tx, "t", ED25519, version(n), b"hi")
            .await
    })
    .await;
    (outcome, transport.asked().len())
}

fn signing_key(latest: u32, min_decryption: u32, min_encryption: u32) -> Value {
    json!({ "data": {
        "type": "ed25519",
        "latest_version": latest,
        "min_decryption_version": min_decryption,
        "min_encryption_version": min_encryption,
    } })
}

#[tokio::test]
async fn a_pinned_signature_names_its_version_and_makes_no_key() {
    let transport = Scripted::answering(vec![(
        "POST",
        "/sign/t.sign.ed25519",
        200,
        json!({ "data": { "key_version": 2, "signature": "vault:v2:AQID" } }),
    )]);
    let signature = with(&transport, |tx, kms| async move {
        kms.sign_with_version(&tx, "t", ED25519, version(2), b"hi")
            .await
    })
    .await
    .unwrap();
    assert_eq!(signature, Signature::new(ED25519, 2, vec![1, 2, 3]));
    let asked = transport.asked();
    assert_eq!(asked.len(), 1, "no `POST keys/…`: {asked:?}");
    assert_eq!(
        asked[0].2,
        Some(json!({ "input": "aGk=", "marshaling_algorithm": "jws", "key_version": 2 }))
    );
}

#[tokio::test]
async fn a_pinned_signature_by_another_version_than_asked_is_refused() {
    let transport = Scripted::answering(vec![(
        "POST",
        "/sign/t.sign.ed25519",
        200,
        json!({ "data": { "key_version": 3, "signature": "vault:v3:AQID" } }),
    )]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.sign_with_version(&tx, "t", ED25519, version(2), b"hi")
            .await
    })
    .await;
    assert!(matches!(outcome, Err(Error::Malformed(_))), "{outcome:?}");
}

#[tokio::test]
async fn a_refusal_of_a_pinned_signature_is_explained_by_one_read_of_the_key() {
    // No such key: Transit's 400.
    let (outcome, _) = pinned(1, 400, None).await;
    assert!(
        matches!(outcome, Err(Error::SigningKeyNotFound { ref tenant_id, algorithm: ED25519 }) if tenant_id == "t"),
        "{outcome:?}"
    );
    // Above the latest: Transit's 500.
    let (outcome, _) = pinned(3, 500, Some(signing_key(2, 1, 0))).await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(3))),
        "{outcome:?}"
    );
    // Retired, the signing floor left at 0: a 500 too.
    let (outcome, _) = pinned(1, 500, Some(signing_key(3, 2, 0))).await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(1))),
        "{outcome:?}"
    );
    // Below the floor the key signs from, while it still verifies.
    let (outcome, _) = pinned(2, 500, Some(signing_key(3, 1, 3))).await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(2))),
        "{outcome:?}"
    );
    // A 500 the read does not explain stays Vault's, of the moment.
    let (outcome, _) = pinned(2, 500, Some(signing_key(3, 1, 0))).await;
    assert!(
        matches!(outcome, Err(Error::Vault { status: 500, .. })),
        "{outcome:?}"
    );
    // A 400 the read does not explain is the request's own fault.
    let (outcome, _) = pinned(2, 400, Some(signing_key(3, 1, 0))).await;
    assert!(matches!(outcome, Err(Error::Malformed(_))), "{outcome:?}");
    // A 403 with the key there: no retry under this identity mends it.
    let (outcome, _) = pinned(2, 403, Some(signing_key(3, 1, 0))).await;
    assert!(
        matches!(outcome, Err(Error::Forbidden { ref tenant_id, operation: "sign" }) if tenant_id == "t"),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_403_on_the_read_too_and_any_other_status_are_left_as_they_came() {
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/sign/t.sign.ed25519",
            403,
            json!({ "errors": ["permission denied"] }),
        ),
        (
            "GET",
            "/keys/t.sign.ed25519",
            403,
            json!({ "errors": ["permission denied"] }),
        ),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.sign_with_version(&tx, "t", ED25519, version(1), b"hi")
            .await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::Vault { status: 403, .. })),
        "{outcome:?}"
    );
    // A Vault unavailable is not read again.
    let transport = Scripted::answering(vec![(
        "POST",
        "/sign/t.sign.ed25519",
        503,
        json!({ "errors": ["Vault is sealed"] }),
    )]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.sign_with_version(&tx, "t", ED25519, version(1), b"hi")
            .await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::Vault { status: 503, .. })),
        "{outcome:?}"
    );
    assert_eq!(transport.asked().len(), 1, "no read on a 503");
}

/// A P-256 key Transit made, its public key as Transit published it, and a
/// signature Transit made over `hello` — verified by openssl, independently
/// of the code under test (`openssl dgst -sha256 -verify`, the point it
/// printed below).
const P256_PEM: &str = "-----BEGIN PUBLIC KEY-----\nMFkwEwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE9WuXrrF/FhVQHsYXXTu0j6DbT5yz\n9HglKvquboy6CfVASdzEUYdSCDev1ycaFZLIqJZMhEbJqhDNFObTr6vByg==\n-----END PUBLIC KEY-----\n";
const P256_SIGNATURE: &str =
    "phAtQ5-wbgiRc1M0pycij5MXUyg6hI635jVUGDFKVgaaHU-8W9AS744l83OB9OVKU2UeuOPgZlj8gXqqOjHayQ";
const P256_POINT: &str = "04f56b97aeb17f1615501ec6175d3bb48fa0db4f9cb3f478252afaae6e8cba09f54049dcc45187520837afd7271a1592c8a8964c8446c9aa10cd14e6d3afabc1ca";
/// The same for Ed25519 (`openssl pkeyutl -verify -rawin`).
const ED25519_PUBLIC: &str = "1sY8fbB3YT3rsSw7z4GJnLdGUPUlh8Yud6ocd687D/o=";
const ED25519_SIGNATURE: &str =
    "LYNckKjG1Xm7xx-U9Rlm_zaBqXfCoINTI90TPfBvFOLOjaifTfzNK9NT-ThKj-6KMR3IQ2ySCW1fcv5HWXy1BA";

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

fn described(kind: &str, oldest: u32, keys: Value) -> Value {
    json!({ "data": {
        "type": kind,
        "latest_version": 3,
        "min_decryption_version": oldest,
        "min_encryption_version": 0,
        "keys": keys,
    } })
}

#[tokio::test]
async fn the_public_keys_are_the_versions_served_read_into_the_model_s_form() {
    let p256 = SignatureAlgorithm::EcdsaP256;
    let transport = Scripted::answering(vec![(
        "GET",
        "/keys/t.sign.ecdsa-p256",
        200,
        // Version 1 is retired: Transit may still list it before a trim.
        described(
            "ecdsa-p256",
            2,
            json!({
                "1": { "public_key": P256_PEM },
                "2": { "public_key": P256_PEM },
                "3": { "public_key": P256_PEM.replace('\n', "\r\n") },
            }),
        ),
    )]);
    let keyring = with(&transport, |tx, kms| async move {
        kms.public_keys(&tx, "t", p256).await
    })
    .await
    .unwrap();
    assert_eq!(keyring.algorithm(), p256);
    assert_eq!(keyring.iter().map(|(v, _)| v).collect::<Vec<_>>(), [2, 3]);
    let key = keyring.get(2).unwrap();
    assert_eq!(key.as_bytes(), hex(P256_POINT).as_slice());
    assert_eq!(keyring.get(3), Some(key));
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(P256_SIGNATURE)
        .unwrap();
    key.verify(b"hello", &Signature::new(p256, 2, signature.clone()))
        .unwrap();
    assert!(matches!(
        key.verify(b"hellp", &Signature::new(p256, 2, signature)),
        Err(Error::InvalidSignature)
    ));

    let transport = Scripted::answering(vec![(
        "GET",
        "/keys/t.sign.ed25519",
        200,
        described(
            "ed25519",
            1,
            json!({ "1": { "public_key": ED25519_PUBLIC } }),
        ),
    )]);
    let keyring = with(&transport, |tx, kms| async move {
        kms.public_keys(&tx, "t", ED25519).await
    })
    .await
    .unwrap();
    let signature = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(ED25519_SIGNATURE)
        .unwrap();
    keyring
        .get(1)
        .unwrap()
        .verify(b"hello", &Signature::new(ED25519, 1, signature))
        .unwrap();
}

#[tokio::test]
async fn the_public_keys_of_a_missing_key_of_another_type_or_another_armor_are_refused() {
    let p256 = SignatureAlgorithm::EcdsaP256;
    let transport = Scripted::answering(vec![(
        "GET",
        "/keys/t.sign.ecdsa-p256",
        404,
        json!({ "errors": [] }),
    )]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.public_keys(&tx, "t", p256).await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::SigningKeyNotFound { algorithm, .. }) if algorithm == p256),
        "{outcome:?}"
    );
    for answer in [
        described("ecdsa-p384", 1, json!({ "1": { "public_key": P256_PEM } })),
        described(
            "ecdsa-p256",
            1,
            json!({ "1": { "public_key": P256_PEM.replace("PUBLIC KEY", "CERTIFICATE") } }),
        ),
        described("ecdsa-p256", 1, json!({ "1": {} })),
    ] {
        let transport = Scripted::answering(vec![("GET", "/keys/t.sign.ecdsa-p256", 200, answer)]);
        let outcome = with(&transport, |tx, kms| async move {
            kms.public_keys(&tx, "t", p256).await
        })
        .await;
        assert!(matches!(outcome, Err(Error::Malformed(_))), "{outcome:?}");
    }
}

fn key_description(latest: u32, oldest: u32, derived: bool) -> Value {
    json!({ "data": { "latest_version": latest, "min_decryption_version": oldest, "derived": derived } })
}

#[tokio::test]
async fn managed_keys_are_made_by_nobody_here_and_a_missing_one_is_said_so() {
    // A producer under managed keys asks for a data key and nothing else;
    // Transit's 400 for a missing key is read as such.
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/datakey/plaintext/t",
            400,
            json!({ "errors": ["encryption key not found"] }),
        ),
        ("GET", "/keys/t", 404, json!({ "errors": [] })),
    ]);
    let kms = VaultTransitService::new("http://vault", "t").managed_keys();
    let outcome = with_service(&transport, kms, |tx, kms| async move {
        kms.generate_dek(&tx, "t").await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::KekNotFound { ref tenant_id, key_version: None }) if tenant_id == "t"),
        "{outcome:?}"
    );
    let asked = transport.asked();
    assert!(
        asked
            .iter()
            .all(|(method, path, _)| !(method == "POST" && path == "/keys/t")),
        "no key was made: {asked:?}"
    );
}

#[tokio::test]
async fn the_context_travels_with_every_kek_operation_and_a_key_made_is_derived() {
    let context = base64::engine::general_purpose::STANDARD.encode("policy.lifecycle");
    let transport = Scripted::answering(vec![
        ("POST", "/keys/t", 204, json!({})),
        (
            "POST",
            "/datakey/plaintext/t",
            200,
            json!({ "data": { "plaintext": base64::engine::general_purpose::STANDARD.encode([7u8; 32]), "ciphertext": "vault:v1:abc" } }),
        ),
        (
            "POST",
            "/decrypt/t",
            200,
            json!({ "data": { "plaintext": base64::engine::general_purpose::STANDARD.encode([7u8; 32]) } }),
        ),
    ]);
    let kms =
        VaultTransitService::new("http://vault", "t").with_derivation_context("policy.lifecycle");
    with_service(&transport, kms, |tx, kms| async move {
        let (_, wrapped) = kms.generate_dek(&tx, "t").await?;
        kms.decrypt_dek(&tx, "t", &wrapped).await?;
        Ok::<_, Error>(())
    })
    .await
    .unwrap();
    let asked = transport.asked();
    let body_of = |path: &str| {
        asked
            .iter()
            .find(|(_, asked, _)| asked == path)
            .and_then(|(_, _, body)| body.clone())
            .unwrap_or_else(|| panic!("{path} was not asked: {asked:?}"))
    };
    assert_eq!(body_of("/keys/t")["derived"], json!(true));
    assert_eq!(body_of("/datakey/plaintext/t")["context"], json!(context));
    assert_eq!(body_of("/decrypt/t")["context"], json!(context));
}

#[tokio::test]
async fn a_400_on_decrypt_is_explained_by_one_read_of_the_key() {
    // The key is gone.
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/decrypt/t",
            400,
            json!({ "errors": ["encryption key not found"] }),
        ),
        ("GET", "/keys/t", 404, json!({ "errors": [] })),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.decrypt_dek(&tx, "t", b"vault:v1:abc").await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::KekNotFound { ref tenant_id, .. }) if tenant_id == "t"),
        "{outcome:?}"
    );
    // The version was retired.
    let transport = Scripted::answering(vec![
        ("POST", "/decrypt/t", 400, json!({ "errors": ["too old"] })),
        ("GET", "/keys/t", 200, key_description(3, 2, false)),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.decrypt_dek(&tx, "t", b"vault:v1:abc").await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(1))),
        "{outcome:?}"
    );
    // The key is there and serves the version: the request's own fault —
    // a wrong context, bytes changed — which no retry mends.
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/decrypt/t",
            400,
            json!({ "errors": ["cipher: message authentication failed"] }),
        ),
        ("GET", "/keys/t", 200, key_description(3, 1, true)),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.decrypt_dek(&tx, "t", b"vault:v1:abc").await
    })
    .await;
    assert!(matches!(outcome, Err(Error::Decrypt)), "{outcome:?}");
}

#[tokio::test]
async fn a_403_on_decrypt_is_forbidden_when_the_key_is_there() {
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/decrypt/t",
            403,
            json!({ "errors": ["permission denied"] }),
        ),
        ("GET", "/keys/t", 200, key_description(1, 1, true)),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.decrypt_dek(&tx, "t", b"vault:v1:abc").await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::Forbidden { ref tenant_id, operation: "decrypt" }) if tenant_id == "t"),
        "{outcome:?}"
    );
    // The key is not there: not found, whatever the policy says.
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/decrypt/t",
            403,
            json!({ "errors": ["permission denied"] }),
        ),
        ("GET", "/keys/t", 404, json!({ "errors": [] })),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.decrypt_dek(&tx, "t", b"vault:v1:abc").await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::KekNotFound { .. })),
        "{outcome:?}"
    );
    // The read is refused too: the 403 as it came, for the operator.
    let transport = Scripted::answering(vec![
        (
            "POST",
            "/decrypt/t",
            403,
            json!({ "errors": ["permission denied"] }),
        ),
        (
            "GET",
            "/keys/t",
            403,
            json!({ "errors": ["permission denied"] }),
        ),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.decrypt_dek(&tx, "t", b"vault:v1:abc").await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::Vault { status: 403, .. })),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn custody_makes_a_derived_key_with_its_rotation_and_refuses_one_that_exists_otherwise() {
    let options = KekOptions {
        derived: true,
        auto_rotate_period: Some(std::time::Duration::from_secs(30 * 24 * 3600)),
    };
    let transport = Scripted::answering(vec![
        ("POST", "/keys/t", 204, json!({})),
        ("GET", "/keys/t", 200, key_description(1, 1, true)),
        ("POST", "/keys/t/config", 204, json!({})),
    ]);
    let made = {
        let options = options.clone();
        with(&transport, |tx, kms| async move {
            kms.create_kek(&tx, "t", &options).await
        })
        .await
        .unwrap()
    };
    assert_eq!(made, 1);
    let asked = transport.asked();
    let (_, _, creation) = &asked[0];
    assert_eq!(creation.as_ref().unwrap()["derived"], json!(true));
    assert_eq!(
        creation.as_ref().unwrap()["auto_rotate_period"],
        json!(2_592_000)
    );
    let (_, _, config) = &asked[2];
    assert_eq!(
        config.as_ref().unwrap()["auto_rotate_period"],
        json!(2_592_000)
    );
    // Transit leaves a key that exists as it is and says nothing: the read
    // tells that it is not derived, and that cannot be changed.
    let transport = Scripted::answering(vec![
        ("POST", "/keys/t", 204, json!({})),
        ("GET", "/keys/t", 200, key_description(4, 1, false)),
    ]);
    let outcome = with(&transport, |tx, kms| async move {
        kms.create_kek(&tx, "t", &options).await
    })
    .await;
    assert!(
        matches!(outcome, Err(Error::Conflict { ref tenant_id, .. }) if tenant_id == "t"),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn retiring_versions_raises_the_key_s_minimum_decryption_version() {
    let transport = Scripted::answering(vec![("POST", "/keys/t/config", 204, json!({}))]);
    with(&transport, |tx, kms| async move {
        kms.retire_kek_versions_before(&tx, "t", 3).await
    })
    .await
    .unwrap();
    let asked = transport.asked();
    assert_eq!(
        asked[0].2.as_ref().unwrap()["min_decryption_version"],
        json!(3)
    );
}
