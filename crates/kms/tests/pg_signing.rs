//! The signing and MAC ports on the PostgreSQL adapter, against a live
//! database, as `tests/pg.rs`; the same properties `tests/vault_signing.rs`
//! holds on Vault, so that the two adapters do not drift apart, and the
//! custody's rules in the body every adapter runs (`common/custody.rs`).

#![cfg(feature = "pg")]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use std::num::NonZeroU32;

use ascetic_ddd_kms::{
    Algorithm, Error, Key, KeyLabel, KeyOptions, MacKeyCustody, MacService, PgKeyManagementService,
    PublicKeyService, Signature, SignatureAlgorithm, SigningKey, SigningKeyCustody, SigningService,
};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, Session, SessionPool};
use tokio::sync::Notify;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";
const ED25519: SignatureAlgorithm = SignatureAlgorithm::Ed25519;
const P256: SignatureAlgorithm = SignatureAlgorithm::EcdsaP256;

fn pool() -> Pool {
    let url = std::env::var("ASCETIC_DDD_TEST_PG_URL").unwrap_or_else(|_| DEFAULT_URL.to_owned());
    let manager = Manager::from_config(
        Config::from_str(&url).expect("a valid PostgreSQL URL"),
        NoTls,
        ManagerConfig {
            recycling_method: RecyclingMethod::Fast,
        },
    );
    Pool::builder(manager)
        .max_size(8)
        .build()
        .expect("the pool can be built")
}

#[path = "common/custody.rs"]
mod custody;

impl custody::Scopes for Fixture {
    type Session = PgSession;
    type Keys = PgKeyManagementService;

    async fn atomic<T, Fut>(
        &self,
        scope: impl FnOnce(PgSession, Arc<PgKeyManagementService>) -> Fut + Send,
    ) -> Result<T, Error>
    where
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        Fixture::atomic(self, scope).await
    }
}

/// The fixture's tables under an adapter built `managed_keys()`.
async fn managed(name: &str) -> Fixture {
    let f = fixture(name).await;
    let kms = Arc::try_unwrap(f.kms).unwrap_or_else(|_| unreachable!("held once"));
    Fixture {
        kms: Arc::new(kms.managed_keys()),
        ..f
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_custody_of_signing_keys_keeps_the_rules() {
    custody::signing_custody(&managed("custody_sign").await, "1").await;
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_custody_of_mac_keys_keeps_the_rules() {
    custody::mac_custody(&managed("custody_mac").await, "1").await;
}

/// Two custodians making one tenant's key at once make one key.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_makings_at_once_make_one_key() {
    let f = managed("custody_race").await;
    let make = || {
        f.atomic(|tx, kms| async move {
            kms.create_signing_key(&tx, "1", ED25519, &KeyOptions::default())
                .await
        })
    };
    let (a, b) = tokio::join!(make(), make());
    assert_eq!((a.unwrap(), b.unwrap()), (1, 1));
    assert_eq!(f.signing_versions_of("1").await, [1]);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_rotation_period_is_refused_by_a_table() {
    let f = fixture("custody_period").await;
    let options = KeyOptions {
        auto_rotate_period: Some(Duration::from_secs(3600)),
    };
    let outcome = f
        .atomic(|tx, kms| async move {
            kms.create_mac_key(&tx, "1", &KeyLabel::new("etag").unwrap(), &options)
                .await
        })
        .await;
    assert!(matches!(outcome, Err(Error::Unsupported(_))), "{outcome:?}");
}

fn master_key() -> Key {
    Algorithm::Aes256Gcm.generate_key().unwrap()
}

struct Fixture {
    sessions: PgSessionPool,
    kms: Arc<PgKeyManagementService>,
    signing_table: String,
}

/// Tables named after the test, dropped and made afresh, so tests run in
/// parallel and start empty.
async fn fixture(name: &str) -> Fixture {
    fixture_with_master(name, master_key()).await
}

async fn fixture_with_master(name: &str, master_key: Key) -> Fixture {
    let (keks, signing, mac) = (
        format!("ks_{name}"),
        format!("ks_{name}_sign"),
        format!("ks_{name}_mac"),
    );
    let sessions = PgSessionPool::new(pool());
    let kms = PgKeyManagementService::new(master_key)
        .with_table(Identifier::new(&keks).unwrap())
        .with_signing_table(Identifier::new(&signing).unwrap())
        .with_mac_table(Identifier::new(&mac).unwrap());
    kms.teardown(&sessions).await.unwrap();
    kms.migrate(&sessions).await.unwrap();
    Fixture {
        sessions,
        kms: Arc::new(kms),
        signing_table: signing,
    }
}

impl Fixture {
    async fn atomic<T, Fut>(
        &self,
        scope: impl FnOnce(PgSession, Arc<PgKeyManagementService>) -> Fut + Send,
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

    async fn signing_versions_of(&self, tenant_id: &str) -> Vec<i32> {
        let sql = format!(
            "SELECT key_version FROM {} WHERE tenant_id = $1 ORDER BY key_version",
            self.signing_table
        );
        self.sessions
            .session(async |session| {
                let rows = session.connection().query(&sql, &[&tenant_id]).await?;
                Ok::<_, Error>(rows.iter().map(|row| row.get::<_, i32>(0)).collect())
            })
            .await
            .unwrap()
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_signature_verifies_and_a_changed_message_does_not() {
    let f = fixture("roundtrip").await;
    f.atomic(|tx, kms| async move {
        let signature = kms.sign(&tx, "1", ED25519, b"hello").await?;
        assert_eq!(signature.algorithm(), ED25519);
        assert_eq!(signature.key_version(), 1);
        assert_eq!(signature.as_bytes().len(), 64);
        kms.verify(&tx, "1", b"hello", &signature).await?;
        let outcome = kms.verify(&tx, "1", b"hellp", &signature).await;
        assert!(
            matches!(outcome, Err(Error::InvalidSignature)),
            "{outcome:?}"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn an_ecdsa_signature_is_raw_r_and_s() {
    let f = fixture("p256").await;
    f.atomic(|tx, kms| async move {
        let signature = kms.sign(&tx, "1", P256, b"hello").await?;
        assert_eq!(signature.as_bytes().len(), 64, "not DER");
        kms.verify(&tx, "1", b"hello", &signature).await
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn what_an_earlier_version_signed_still_verifies_after_a_rotation() {
    let f = fixture("rotation").await;
    f.atomic(|tx, kms| async move {
        let first = kms.sign(&tx, "1", ED25519, b"hello").await?;
        assert_eq!(kms.rotate_signing_key(&tx, "1", ED25519).await?, 2);
        let second = kms.sign(&tx, "1", ED25519, b"hello").await?;
        assert_eq!(second.key_version(), 2);
        assert_ne!(first.as_bytes(), second.as_bytes());
        kms.verify(&tx, "1", b"hello", &first).await?;
        kms.verify(&tx, "1", b"hello", &second).await
    })
    .await
    .unwrap();
    assert_eq!(f.signing_versions_of("1").await, [1, 2]);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_signature_by_a_key_the_tenant_has_not_got_is_told_apart_from_a_bad_one() {
    let f = fixture("missing").await;
    f.atomic(|tx, kms| async move {
        let signature = Signature::new(ED25519, 1, vec![0u8; 64]);
        let outcome = kms.verify(&tx, "1", b"hello", &signature).await;
        assert!(
            matches!(outcome, Err(Error::SigningKeyNotFound { ref tenant_id, algorithm: ED25519 }) if tenant_id == "1"),
            "{outcome:?}"
        );
        kms.sign(&tx, "1", ED25519, b"hello").await?;
        let signature = Signature::new(ED25519, 9, vec![0u8; 64]);
        let outcome = kms.verify(&tx, "1", b"hello", &signature).await;
        assert!(matches!(outcome, Err(Error::NoKeyOfVersion(9))), "{outcome:?}");
        // A key of another algorithm is another key.
        let signature = Signature::new(P256, 1, vec![0u8; 64]);
        let outcome = kms.verify(&tx, "1", b"hello", &signature).await;
        assert!(
            matches!(outcome, Err(Error::SigningKeyNotFound { algorithm: P256, .. })),
            "{outcome:?}"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn one_tenant_s_key_does_not_verify_another_s_signature() {
    let f = fixture("tenants").await;
    f.atomic(|tx, kms| async move {
        let signature = kms.sign(&tx, "1", ED25519, b"hello").await?;
        kms.sign(&tx, "2", ED25519, b"hello").await?;
        let outcome = kms.verify(&tx, "2", b"hello", &signature).await;
        assert!(
            matches!(outcome, Err(Error::InvalidSignature)),
            "{outcome:?}"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_key_wrapped_under_one_master_key_does_not_open_under_another() {
    let f = fixture("master").await;
    f.atomic(|tx, kms| async move { kms.sign(&tx, "1", ED25519, b"hello").await })
        .await
        .unwrap();
    let other = PgKeyManagementService::new(master_key())
        .with_table(Identifier::new("ks_master").unwrap())
        .with_signing_table(Identifier::new("ks_master_sign").unwrap())
        .with_mac_table(Identifier::new("ks_master_mac").unwrap());
    let outcome = f
        .sessions
        .session(async |session| {
            session
                .atomic(async |tx| other.sign(&tx, "1", ED25519, b"hello").await)
                .await
        })
        .await;
    assert!(matches!(outcome, Err(Error::Decrypt)), "{outcome:?}");
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn deleting_the_key_ends_what_it_signed() {
    let f = fixture("delete").await;
    f.atomic(|tx, kms| async move {
        let signature = kms.sign(&tx, "1", ED25519, b"hello").await?;
        kms.delete_signing_key(&tx, "1", ED25519).await?;
        let outcome = kms.verify(&tx, "1", b"hello", &signature).await;
        assert!(
            matches!(outcome, Err(Error::SigningKeyNotFound { .. })),
            "{outcome:?}"
        );
        // Nothing to delete is nothing.
        kms.delete_signing_key(&tx, "1", ED25519).await?;
        kms.delete_mac_key(&tx, "1", &KeyLabel::new("etag").unwrap())
            .await
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_mac_is_the_same_twice_and_another_s_differs() {
    let f = fixture("mac").await;
    f.atomic(|tx, kms| async move {
        let etag = KeyLabel::new("etag").unwrap();
        let other = KeyLabel::new("other").unwrap();
        let first = kms.mac(&tx, "1", &etag, b"hello").await?;
        assert_eq!(first, kms.mac(&tx, "1", &etag, b"hello").await?);
        assert_eq!(first.key_version(), 1);
        assert_eq!(first.as_bytes().len(), 32);
        for mac in [
            kms.mac(&tx, "1", &etag, b"hellp").await?,
            kms.mac(&tx, "2", &etag, b"hello").await?,
            kms.mac(&tx, "1", &other, b"hello").await?,
        ] {
            assert_ne!(mac.as_bytes(), first.as_bytes());
        }
        kms.delete_mac_key(&tx, "1", &etag).await?;
        let fresh = kms.mac(&tx, "1", &etag, b"hello").await?;
        assert_ne!(
            fresh.as_bytes(),
            first.as_bytes(),
            "a new key after deletion"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_first_signings_at_once_share_one_key() {
    let f = Arc::new(fixture("first_at_once").await);
    let (made, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let first = {
        let (f, made, release) = (Arc::clone(&f), Arc::clone(&made), Arc::clone(&release));
        tokio::spawn(async move {
            f.atomic(|tx, kms| async move {
                let signature = kms.sign(&tx, "1", ED25519, b"hello").await?;
                made.notify_one();
                release.notified().await;
                Ok(signature)
            })
            .await
        })
    };
    made.notified().await;
    let mut second = {
        let f = Arc::clone(&f);
        tokio::spawn(async move {
            f.atomic(|tx, kms| async move { kms.sign(&tx, "1", ED25519, b"hello").await })
                .await
        })
    };
    // While the first holds the lock in its open transaction, the second waits.
    tokio::select! {
        outcome = &mut second => panic!("the second did not wait: {:?}", outcome.unwrap().map(|_| ())),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    release.notify_one();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!(first.key_version(), 1);
    assert_eq!(second.key_version(), 1);
    assert_eq!(first, second, "one key, one deterministic signature");
    assert_eq!(f.signing_versions_of("1").await, [1]);
}

fn version(n: u32) -> NonZeroU32 {
    NonZeroU32::new(n).unwrap()
}

impl Fixture {
    /// One statement as an operator, or an attacker, runs it on the table.
    async fn execute(&self, sql: &str, params: &[&(dyn tokio_postgres::types::ToSql + Sync)]) {
        self.sessions
            .session(async |session| {
                session.connection().execute(sql, params).await?;
                Ok::<_, Error>(())
            })
            .await
            .unwrap();
    }
}

/// After a rotation the earlier version signs when it is named — byte for
/// byte what it signed as the latest, Ed25519 being deterministic.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_earlier_version_signs_when_it_is_named() {
    let f = fixture("pinned").await;
    f.atomic(|tx, kms| async move {
        let first = kms.sign(&tx, "1", ED25519, b"hello").await?;
        kms.rotate_signing_key(&tx, "1", ED25519).await?;
        let pinned = kms
            .sign_with_version(&tx, "1", ED25519, version(1), b"hello")
            .await?;
        assert_eq!(pinned, first);
        assert_eq!(
            kms.sign(&tx, "1", ED25519, b"hello").await?.key_version(),
            2
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_pinned_signature_makes_nothing() {
    let f = fixture("pinned_nothing").await;
    let outcome = f
        .atomic(|tx, kms| async move {
            kms.sign_with_version(&tx, "1", P256, version(1), b"hello")
                .await
        })
        .await;
    assert!(
        matches!(
            outcome,
            Err(Error::SigningKeyNotFound {
                algorithm: P256,
                ..
            })
        ),
        "{outcome:?}"
    );
    assert!(f.signing_versions_of("1").await.is_empty());
    f.atomic(|tx, kms| async move { kms.sign(&tx, "1", P256, b"hello").await })
        .await
        .unwrap();
    // A refusal rolls back what its transaction did; the key was made in
    // one of its own, and the refusal adds nothing to it.
    let outcome = f
        .atomic(|tx, kms| async move {
            kms.sign_with_version(&tx, "1", P256, version(5), b"hello")
                .await
        })
        .await;
    assert!(
        matches!(outcome, Err(Error::NoKeyOfVersion(5))),
        "{outcome:?}"
    );
    assert_eq!(f.signing_versions_of("1").await, [1]);
}

/// The keyring is the versions there are; a version an operator deleted
/// neither signs, verifies nor is published.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_keyring_is_the_versions_there_are() {
    let f = fixture("keyring").await;
    let (first, keyring) = f
        .atomic(|tx, kms| async move {
            let first = kms.sign(&tx, "1", P256, b"hello").await?;
            kms.rotate_signing_key(&tx, "1", P256).await?;
            kms.rotate_signing_key(&tx, "1", P256).await?;
            let keyring = kms.public_keys(&tx, "1", P256).await?;
            Ok((first, keyring))
        })
        .await
        .unwrap();
    assert_eq!(
        keyring.iter().map(|(v, _)| v).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    keyring.get(1).unwrap().verify(b"hello", &first).unwrap();
    f.execute(
        &format!(
            "DELETE FROM {} WHERE tenant_id = '1' AND key_version < 2",
            f.signing_table
        ),
        &[],
    )
    .await;
    f.atomic(|tx, kms| async move {
        let keyring = kms.public_keys(&tx, "1", P256).await?;
        assert_eq!(keyring.iter().map(|(v, _)| v).collect::<Vec<_>>(), [2, 3]);
        let signed = kms
            .sign_with_version(&tx, "1", P256, version(1), b"hello")
            .await;
        assert!(
            matches!(signed, Err(Error::NoKeyOfVersion(1))),
            "{signed:?}"
        );
        let verified = kms.verify(&tx, "1", b"hello", &first).await;
        assert!(
            matches!(verified, Err(Error::NoKeyOfVersion(1))),
            "{verified:?}"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_keyring_does_not_open_under_another_master_key() {
    let f = fixture("keyring_master").await;
    f.atomic(|tx, kms| async move { kms.sign(&tx, "1", ED25519, b"hello").await })
        .await
        .unwrap();
    let other = PgKeyManagementService::new(master_key())
        .with_table(Identifier::new("ks_keyring_master").unwrap())
        .with_signing_table(Identifier::new("ks_keyring_master_sign").unwrap())
        .with_mac_table(Identifier::new("ks_keyring_master_mac").unwrap());
    let outcome = f
        .sessions
        .session(async |session| {
            session
                .atomic(async |tx| other.public_keys(&tx, "1", ED25519).await)
                .await
        })
        .await;
    assert!(matches!(outcome, Err(Error::Decrypt)), "{outcome:?}");
}

/// A writer of the table who has not the master key can replace the stored
/// public half, never the wrapped private one: the replacement is refused,
/// neither published nor verified under.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_public_half_replaced_in_the_table_is_refused() {
    let f = fixture("tampered").await;
    let signed = f
        .atomic(|tx, kms| async move {
            kms.sign(&tx, "1", ED25519, b"hello").await?;
            kms.rotate_signing_key(&tx, "1", ED25519).await?;
            kms.sign(&tx, "1", ED25519, b"hello").await
        })
        .await
        .unwrap();
    let forged = SigningKey::generate("1", ED25519, 2).unwrap();
    let forged_signature = forged.sign(b"hello").unwrap();
    f.execute(
        &format!(
            "UPDATE {} SET public_key = $1 WHERE tenant_id = '1' AND key_version = 2",
            f.signing_table
        ),
        &[&forged.public().as_bytes()],
    )
    .await;
    f.atomic(|tx, kms| async move {
        let keyring = kms.public_keys(&tx, "1", ED25519).await;
        assert!(matches!(keyring, Err(Error::Malformed(_))), "{keyring:?}");
        for signature in [&signed, &forged_signature] {
            let verified = kms.verify(&tx, "1", b"hello", signature).await;
            assert!(matches!(verified, Err(Error::Malformed(_))), "{verified:?}");
        }
        Ok(())
    })
    .await
    .unwrap();
}
