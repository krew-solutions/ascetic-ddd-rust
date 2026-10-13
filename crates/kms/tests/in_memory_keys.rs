//! The in-memory keys behave as the Vault adapter does, where the tests of
//! a consumer would notice a difference.

use ascetic_ddd_kms::testing::InMemoryKeys;
use ascetic_ddd_kms::{Error, KeyLabel, MacService, Signature, SignatureAlgorithm, SigningService};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use ascetic_ddd_session::{AsyncScope, Session, SessionPool};

const ED25519: SignatureAlgorithm = SignatureAlgorithm::Ed25519;

async fn try_in_scope<T, F>(scope: F) -> Result<T, Error>
where
    F: AsyncScope<MemorySession, Result<T, Error>, Fut: Send> + Send,
    T: Send,
{
    MemorySessionPool::new()
        .session(async |session: MemorySession| session.atomic(scope).await)
        .await
}

async fn in_scope<T, F>(scope: F) -> T
where
    F: AsyncScope<MemorySession, Result<T, Error>, Fut: Send> + Send,
    T: Send,
{
    try_in_scope(scope).await.expect("the scope answers")
}

#[tokio::test]
async fn a_registered_seed_signs_the_same_from_run_to_run() {
    let keys = InMemoryKeys::new();
    assert_eq!(keys.register("t", &[7u8; 32]), 1);
    let first = in_scope(async |tx| keys.sign(&tx, "t", ED25519, b"hello").await).await;
    let other = InMemoryKeys::new();
    other.register("t", &[7u8; 32]);
    let second = in_scope(async |tx| other.sign(&tx, "t", ED25519, b"hello").await).await;
    assert_eq!(first, second);
    assert_eq!(first.key_version(), 1);
    in_scope(async |tx| keys.verify(&tx, "t", b"hello", &first).await).await;
}

#[tokio::test]
async fn an_unregistered_tenant_gets_a_key_on_first_signing() {
    let keys = InMemoryKeys::new();
    let signature = in_scope(async |tx| keys.sign(&tx, "t", ED25519, b"hello").await).await;
    in_scope(async |tx| keys.verify(&tx, "t", b"hello", &signature).await).await;
    let outcome = try_in_scope(async |tx| keys.verify(&tx, "t", b"hellp", &signature).await).await;
    assert!(
        matches!(outcome, Err(Error::InvalidSignature)),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_missing_key_and_a_missing_version_are_named() {
    let keys = InMemoryKeys::new();
    let signature = Signature::new(ED25519, 1, vec![0u8; 64]);
    let outcome = try_in_scope(async |tx| keys.verify(&tx, "t", b"hello", &signature).await).await;
    assert!(
        matches!(outcome, Err(Error::SigningKeyNotFound { ref tenant_id, algorithm: ED25519 }) if tenant_id == "t"),
        "{outcome:?}"
    );
    keys.register("t", &[1u8; 32]);
    let signature = Signature::new(ED25519, 2, vec![0u8; 64]);
    let outcome = try_in_scope(async |tx| keys.verify(&tx, "t", b"hello", &signature).await).await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(2))),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn a_rotation_is_the_next_version_and_the_earlier_one_still_verifies() {
    let keys = InMemoryKeys::new();
    let first = in_scope(async |tx| keys.sign(&tx, "t", ED25519, b"hello").await).await;
    let version = in_scope(async |tx| keys.rotate_signing_key(&tx, "t", ED25519).await).await;
    assert_eq!(version, 2);
    let second = in_scope(async |tx| keys.sign(&tx, "t", ED25519, b"hello").await).await;
    assert_eq!(second.key_version(), 2);
    in_scope(async |tx| keys.verify(&tx, "t", b"hello", &first).await).await;
    in_scope(async |tx| keys.delete_signing_key(&tx, "t", ED25519).await).await;
    let outcome = try_in_scope(async |tx| keys.verify(&tx, "t", b"hello", &first).await).await;
    assert!(
        matches!(outcome, Err(Error::SigningKeyNotFound { .. })),
        "{outcome:?}"
    );
}

#[tokio::test]
async fn an_ecdsa_signature_is_raw_r_and_s_under_a_key_of_its_own() {
    let keys = InMemoryKeys::new();
    keys.register("t", &[1u8; 32]);
    let p256 = SignatureAlgorithm::EcdsaP256;
    let signature = in_scope(async |tx| keys.sign(&tx, "t", p256, b"hello").await).await;
    assert_eq!(signature.algorithm(), p256);
    assert_eq!(
        signature.key_version(),
        1,
        "the Ed25519 ring is another key"
    );
    assert_eq!(signature.as_bytes().len(), 64, "not DER");
    in_scope(async |tx| keys.verify(&tx, "t", b"hello", &signature).await).await;
}

#[tokio::test]
async fn a_mac_repeats_and_differs_by_message_tenant_and_purpose() {
    let keys = InMemoryKeys::new();
    let etag = KeyLabel::new("etag").unwrap();
    let other = KeyLabel::new("other").unwrap();
    let first = in_scope(async |tx| keys.mac(&tx, "t", &etag, b"hello").await).await;
    let again = in_scope(async |tx| keys.mac(&tx, "t", &etag, b"hello").await).await;
    assert_eq!(first, again);
    assert_eq!(first.key_version(), 1);
    assert_eq!(first.as_bytes().len(), 32);
    let by_message = in_scope(async |tx| keys.mac(&tx, "t", &etag, b"hellp").await).await;
    let by_tenant = in_scope(async |tx| keys.mac(&tx, "u", &etag, b"hello").await).await;
    let by_purpose = in_scope(async |tx| keys.mac(&tx, "t", &other, b"hello").await).await;
    for mac in [by_message, by_tenant, by_purpose] {
        assert_ne!(mac.as_bytes(), first.as_bytes());
    }
    in_scope(async |tx| keys.delete_mac_key(&tx, "t", &etag).await).await;
    let fresh = in_scope(async |tx| keys.mac(&tx, "t", &etag, b"hello").await).await;
    assert_ne!(
        fresh.as_bytes(),
        first.as_bytes(),
        "a new key after deletion"
    );
}
