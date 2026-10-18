//! One body of tests for the custody of signing and MAC keys, run on every
//! adapter that serves it — in memory, PostgreSQL, Vault — so that the rules
//! of ADR-0020 hold the same everywhere, and the in-memory keys a consumer's
//! tests run on behave as the adapters do. Each test file brings its
//! adapter, built `managed_keys()`, and the scope it runs in.

use std::future::Future;
use std::num::NonZeroU32;
use std::sync::Arc;

use ascetic_ddd_kms::{
    Error, KeyLabel, KeyOptions, MacKeyCustody, MacService, PublicKeyService, SignatureAlgorithm,
    SigningKeyCustody, SigningService,
};
use ascetic_ddd_session::Session;

const ED25519: SignatureAlgorithm = SignatureAlgorithm::Ed25519;

/// An adapter built `managed_keys()`, and the scope a test runs it in.
pub trait Scopes: Sync {
    /// The session the adapter runs in.
    type Session: Session + Send + Sync + 'static;
    /// The adapter.
    type Keys: Send + Sync + 'static;

    /// Runs `scope` in one transaction with the adapter.
    fn atomic<T, Fut>(
        &self,
        scope: impl FnOnce(Self::Session, Arc<Self::Keys>) -> Fut + Send,
    ) -> impl Future<Output = Result<T, Error>> + Send
    where
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send;
}

fn options() -> KeyOptions {
    KeyOptions::default()
}

fn version(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).expect("a version from one")
}

/// The versions the keyring publishes, or the refusal.
async fn published<C>(c: &C, tenant: &str) -> Result<Vec<u32>, Error>
where
    C: Scopes,
    C::Keys: PublicKeyService<C::Session>,
{
    let tenant = tenant.to_owned();
    c.atomic(|tx, keys| async move {
        let keyring = keys.public_keys(&tx, &tenant, ED25519).await?;
        Ok(keyring.iter().map(|(v, _)| v).collect())
    })
    .await
}

/// A signing key's custody, end to end: made once, rotated, retired —
/// never below the floor, never the latest — and deleted; and what signs
/// meanwhile makes no key.
pub async fn signing_custody<C>(c: &C, tenant: &str)
where
    C: Scopes,
    C::Keys:
        SigningKeyCustody<C::Session> + SigningService<C::Session> + PublicKeyService<C::Session>,
{
    let t = tenant.to_owned();

    // Managed, nothing signs before custody makes the key, and nothing is
    // made by trying.
    let unsigned = {
        let t = t.clone();
        c.atomic(|tx, keys| async move { keys.sign(&tx, &t, ED25519, b"hello").await })
            .await
    };
    assert!(
        matches!(
            unsigned,
            Err(Error::SigningKeyNotFound {
                algorithm: ED25519,
                ..
            })
        ),
        "{unsigned:?}"
    );
    assert!(
        matches!(
            published(c, &t).await,
            Err(Error::SigningKeyNotFound { .. })
        ),
        "a refused signature leaves no key behind"
    );
    let rotated = {
        let t = t.clone();
        c.atomic(|tx, keys| async move { keys.rotate_signing_key(&tx, &t, ED25519).await })
            .await
    };
    assert!(
        matches!(rotated, Err(Error::SigningKeyNotFound { .. })),
        "a rotation makes no first version: {rotated:?}"
    );

    // Made once: a second making answers the latest and makes nothing.
    let made = {
        let t = t.clone();
        c.atomic(|tx, keys| async move {
            let first = keys
                .create_signing_key(&tx, &t, ED25519, &options())
                .await?;
            let again = keys
                .create_signing_key(&tx, &t, ED25519, &options())
                .await?;
            Ok((first, again))
        })
        .await
        .expect("custody makes the key")
    };
    assert_eq!(made, (1, 1));
    let first = {
        let t = t.clone();
        c.atomic(|tx, keys| async move { keys.sign(&tx, &t, ED25519, b"hello").await })
            .await
            .expect("the key custody made signs")
    };
    assert_eq!(first.key_version(), 1);

    // Rotated twice; the earlier versions still verify.
    let versions = {
        let t = t.clone();
        c.atomic(|tx, keys| async move {
            let second = keys.rotate_signing_key(&tx, &t, ED25519).await?;
            let third = keys.rotate_signing_key(&tx, &t, ED25519).await?;
            let again = keys
                .create_signing_key(&tx, &t, ED25519, &options())
                .await?;
            Ok((second, third, again))
        })
        .await
        .expect("custody rotates the key")
    };
    assert_eq!(versions, (2, 3, 3));
    assert_eq!(published(c, &t).await.expect("a keyring"), [1, 2, 3]);

    // Retired below 2: version 1 neither signs, verifies nor is published.
    {
        let t = t.clone();
        c.atomic(|tx, keys| async move {
            keys.retire_signing_versions_before(&tx, &t, ED25519, 2)
                .await
        })
        .await
        .expect("custody retires the versions");
    }
    assert_eq!(published(c, &t).await.expect("a keyring"), [2, 3]);
    let pinned = {
        let t = t.clone();
        c.atomic(|tx, keys| async move {
            keys.sign_with_version(&tx, &t, ED25519, version(1), b"hello")
                .await
        })
        .await
    };
    assert!(
        matches!(pinned, Err(Error::NoKeyOfVersion(1))),
        "{pinned:?}"
    );
    let verified = {
        let t = t.clone();
        let first = first.clone();
        c.atomic(|tx, keys| async move { keys.verify(&tx, &t, b"hello", &first).await })
            .await
    };
    assert!(
        matches!(verified, Err(Error::NoKeyOfVersion(1))),
        "{verified:?}"
    );

    // A stale or a repeated retirement does nothing, the floor is never
    // lowered; a bound above the latest is refused and retires nothing.
    for stale in [0, 1, 2] {
        let t = t.clone();
        c.atomic(|tx, keys| async move {
            keys.retire_signing_versions_before(&tx, &t, ED25519, stale)
                .await
        })
        .await
        .expect("a stale retirement is not an error");
    }
    assert_eq!(published(c, &t).await.expect("a keyring"), [2, 3]);
    let above = {
        let t = t.clone();
        c.atomic(|tx, keys| async move {
            keys.retire_signing_versions_before(&tx, &t, ED25519, 4)
                .await
        })
        .await
    };
    assert!(matches!(above, Err(Error::NoKeyOfVersion(4))), "{above:?}");
    assert_eq!(published(c, &t).await.expect("a keyring"), [2, 3]);

    // A version that still signs may be retired, the latest stays.
    {
        let t = t.clone();
        c.atomic(|tx, keys| async move {
            keys.retire_signing_versions_before(&tx, &t, ED25519, 3)
                .await
        })
        .await
        .expect("the compromise path retires a signing version");
    }
    assert_eq!(published(c, &t).await.expect("a keyring"), [3]);
    let latest = {
        let t = t.clone();
        c.atomic(|tx, keys| async move { keys.sign(&tx, &t, ED25519, b"hello").await })
            .await
            .expect("the latest signs")
    };
    assert_eq!(latest.key_version(), 3);

    // Retirement of a key the tenant has not got is named.
    let absent = c
        .atomic(|tx, keys| async move {
            keys.retire_signing_versions_before(&tx, "zz-nobody", ED25519, 1)
                .await
        })
        .await;
    assert!(
        matches!(absent, Err(Error::SigningKeyNotFound { .. })),
        "{absent:?}"
    );

    // Deleted, twice; then nothing signs and a rotation makes nothing.
    {
        let t = t.clone();
        c.atomic(|tx, keys| async move {
            keys.delete_signing_key(&tx, &t, ED25519).await?;
            keys.delete_signing_key(&tx, &t, ED25519).await
        })
        .await
        .expect("a deletion of nothing is not an error");
    }
    assert!(matches!(
        published(c, &t).await,
        Err(Error::SigningKeyNotFound { .. })
    ));
    let rotated = {
        let t = t.clone();
        c.atomic(|tx, keys| async move { keys.rotate_signing_key(&tx, &t, ED25519).await })
            .await
    };
    assert!(
        matches!(rotated, Err(Error::SigningKeyNotFound { .. })),
        "{rotated:?}"
    );
}

/// A MAC key's custody, end to end, as the signing key's.
pub async fn mac_custody<C>(c: &C, tenant: &str)
where
    C: Scopes,
    C::Keys: MacKeyCustody<C::Session> + MacService<C::Session>,
{
    let t = tenant.to_owned();
    let purpose = KeyLabel::new("etag").expect("a label");

    let unmade = {
        let (t, purpose) = (t.clone(), purpose.clone());
        c.atomic(|tx, keys| async move { keys.mac(&tx, &t, &purpose, b"hello").await })
            .await
    };
    assert!(
        matches!(unmade, Err(Error::MacKeyNotFound { ref purpose, .. }) if purpose == "etag"),
        "{unmade:?}"
    );
    let rotated = {
        let (t, purpose) = (t.clone(), purpose.clone());
        c.atomic(|tx, keys| async move { keys.rotate_mac_key(&tx, &t, &purpose).await })
            .await
    };
    assert!(
        matches!(rotated, Err(Error::MacKeyNotFound { .. })),
        "{rotated:?}"
    );

    let made = {
        let (t, purpose) = (t.clone(), purpose.clone());
        c.atomic(|tx, keys| async move {
            let first = keys.create_mac_key(&tx, &t, &purpose, &options()).await?;
            let again = keys.create_mac_key(&tx, &t, &purpose, &options()).await?;
            let mac = keys.mac(&tx, &t, &purpose, b"hello").await?;
            Ok((first, again, mac.key_version()))
        })
        .await
        .expect("custody makes the key")
    };
    assert_eq!(made, (1, 1, 1));

    let after = {
        let (t, purpose) = (t.clone(), purpose.clone());
        c.atomic(|tx, keys| async move {
            let second = keys.rotate_mac_key(&tx, &t, &purpose).await?;
            let before = keys.mac(&tx, &t, &purpose, b"hello").await?;
            keys.retire_mac_versions_before(&tx, &t, &purpose, 2)
                .await?;
            keys.retire_mac_versions_before(&tx, &t, &purpose, 1)
                .await?;
            let after = keys.mac(&tx, &t, &purpose, b"hello").await?;
            Ok((second, before.key_version(), after == before))
        })
        .await
        .expect("custody rotates and retires the key")
    };
    assert_eq!(
        after,
        (2, 2, true),
        "a retirement changes nothing a MAC is made with"
    );
    let above = {
        let (t, purpose) = (t.clone(), purpose.clone());
        c.atomic(
            |tx, keys| async move { keys.retire_mac_versions_before(&tx, &t, &purpose, 3).await },
        )
        .await
    };
    assert!(matches!(above, Err(Error::NoKeyOfVersion(3))), "{above:?}");

    {
        let (t, purpose) = (t.clone(), purpose.clone());
        c.atomic(|tx, keys| async move {
            keys.delete_mac_key(&tx, &t, &purpose).await?;
            keys.delete_mac_key(&tx, &t, &purpose).await
        })
        .await
        .expect("a deletion of nothing is not an error");
    }
    let gone = {
        let (t, purpose) = (t.clone(), purpose.clone());
        c.atomic(|tx, keys| async move { keys.mac(&tx, &t, &purpose, b"hello").await })
            .await
    };
    assert!(
        matches!(gone, Err(Error::MacKeyNotFound { .. })),
        "{gone:?}"
    );
}
