//! The in-memory keys behave as the Vault adapter does, where the tests of
//! a consumer would notice a difference.

use std::num::NonZeroU32;

use ascetic_ddd_kms::testing::InMemoryKeys;
use ascetic_ddd_kms::{
    Error, KeyLabel, MacService, PublicKeyService, Signature, SignatureAlgorithm, SigningKey,
    SigningService,
};
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

fn version(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

#[tokio::test]
async fn a_refused_pinned_signature_leaves_no_key_behind() {
    let keys = InMemoryKeys::new();
    let outcome = try_in_scope(async |tx| {
        keys.sign_with_version(&tx, "t", ED25519, version(1), b"hello")
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
    let outcome = try_in_scope(async |tx| keys.public_keys(&tx, "t", ED25519).await).await;
    assert!(
        matches!(outcome, Err(Error::SigningKeyNotFound { .. })),
        "{outcome:?}"
    );
}

/// A version retired as an operator retires it neither signs, verifies nor
/// is published, and its number is not given again.
#[tokio::test]
async fn a_retired_version_neither_signs_verifies_nor_is_published() {
    let keys = InMemoryKeys::new();
    let first = in_scope(async |tx| keys.sign(&tx, "t", ED25519, b"hello").await).await;
    in_scope(async |tx| {
        keys.rotate_signing_key(&tx, "t", ED25519).await?;
        keys.rotate_signing_key(&tx, "t", ED25519).await
    })
    .await;
    let pinned = in_scope(async |tx| {
        keys.sign_with_version(&tx, "t", ED25519, version(1), b"hello")
            .await
    })
    .await;
    assert_eq!(pinned, first);
    keys.retire_signing_versions_before("t", ED25519, 2)
        .unwrap();
    let signed = try_in_scope(async |tx| {
        keys.sign_with_version(&tx, "t", ED25519, version(1), b"hello")
            .await
    })
    .await;
    assert!(
        matches!(signed, Err(Error::NoKeyOfVersion(1))),
        "{signed:?}"
    );
    let verified = try_in_scope(async |tx| keys.verify(&tx, "t", b"hello", &first).await).await;
    assert!(
        matches!(verified, Err(Error::NoKeyOfVersion(1))),
        "{verified:?}"
    );
    let keyring = in_scope(async |tx| keys.public_keys(&tx, "t", ED25519).await).await;
    assert_eq!(keyring.iter().map(|(v, _)| v).collect::<Vec<_>>(), [2, 3]);
    let next = in_scope(async |tx| keys.rotate_signing_key(&tx, "t", ED25519).await).await;
    assert_eq!(next, 4);
    assert!(matches!(
        keys.retire_signing_versions_before("t", ED25519, 9),
        Err(Error::NoKeyOfVersion(9))
    ));
    assert!(matches!(
        keys.retire_signing_versions_before("u", ED25519, 1),
        Err(Error::SigningKeyNotFound { .. })
    ));
}

#[tokio::test]
async fn a_registered_seed_publishes_its_key() {
    let keys = InMemoryKeys::new();
    keys.register("t", &[7u8; 32]);
    let keyring = in_scope(async |tx| keys.public_keys(&tx, "t", ED25519).await).await;
    let expected = SigningKey::from_seed("t", ED25519, 1, &[7u8; 32]).unwrap();
    assert_eq!(keyring.get(1), Some(expected.public()));
}
