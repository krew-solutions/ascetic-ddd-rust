//! Integration tests for the PostgreSQL key management service. They need a
//! live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-kms --features pg -- --ignored
//! ```

#![cfg(feature = "pg")]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ascetic_ddd_kms::{
    Algorithm, Ciphertext, Error, Key, KeyManagementService, PgKeyManagementService,
};
use ascetic_ddd_session::pg::Identifier;
use ascetic_ddd_session::pg::deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use ascetic_ddd_session::pg::tokio_postgres::{Config, NoTls};
use ascetic_ddd_session::{PgAccess, PgSession, PgSessionPool, Session, SessionPool};
use tokio::sync::Notify;

const DEFAULT_URL: &str = "postgresql://devel:devel@localhost:5432/devel_karmabot_test";

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

fn key() -> Key {
    Algorithm::Aes256Gcm.generate_key().unwrap()
}

struct Fixture {
    sessions: PgSessionPool,
    kms: Arc<PgKeyManagementService>,
    table: String,
}

/// A table of its own per test, so tests run in parallel; a fresh master key.
async fn fixture(name: &str) -> Fixture {
    fixture_with_master(name, key()).await
}

async fn fixture_with_master(name: &str, master_key: Key) -> Fixture {
    let table = format!("kms_keys_{name}");
    let sessions = PgSessionPool::new(pool());
    let kms = PgKeyManagementService::new(master_key).with_table(Identifier::new(&table).unwrap());
    sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
                .await
                .unwrap();
            kms.setup(&session).await
        })
        .await
        .unwrap();
    Fixture {
        sessions,
        kms: Arc::new(kms),
        table,
    }
}

impl Fixture {
    /// Runs `scope` in one transaction. The session goes by value, as
    /// `atomic` hands it out; a scope borrowing it could not be shown `Send`
    /// (ADR-0004).
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

    async fn versions_of(&self, tenant_id: &str) -> Vec<i32> {
        let sql = format!(
            "SELECT key_version FROM {} WHERE tenant_id = $1 ORDER BY key_version",
            self.table
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
async fn a_dek_wrapped_after_a_rotation_unwraps() {
    let f = fixture("rotate_and_wrap").await;
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, "1").await?;
        let dek = key();
        let wrapped = kms.encrypt_dek(&tx, "1", &dek).await?;
        assert_eq!(kms.decrypt_dek(&tx, "1", &wrapped).await?, dek);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_generated_dek_is_thirty_two_bytes_and_unwraps() {
    let f = fixture("generate_dek").await;
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, "1").await?;
        let (dek, wrapped) = kms.generate_dek(&tx, "1").await?;
        assert_eq!(dek.as_bytes().len(), 32);
        assert_eq!(kms.decrypt_dek(&tx, "1", &wrapped).await?, dek);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn each_rotation_is_the_next_version() {
    let f = fixture("rotation_versions").await;
    f.atomic(|tx, kms| async move {
        assert_eq!(kms.rotate_kek(&tx, "1").await?, 1);
        assert_eq!(kms.rotate_kek(&tx, "1").await?, 2);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(f.versions_of("1").await, [1, 2]);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn what_an_earlier_version_wrapped_still_unwraps() {
    let f = fixture("unwrap_after_rotation").await;
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, "1").await?;
        let dek = key();
        let wrapped_v1 = kms.encrypt_dek(&tx, "1", &dek).await?;
        kms.rotate_kek(&tx, "1").await?;
        assert_eq!(kms.decrypt_dek(&tx, "1", &wrapped_v1).await?, dek);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_rewrap_moves_a_dek_to_the_current_version() {
    let f = fixture("rewrap").await;
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, "1").await?;
        let dek = key();
        let wrapped_v1 = kms.encrypt_dek(&tx, "1", &dek).await?;
        kms.rotate_kek(&tx, "1").await?;
        let wrapped_v2 = kms.rewrap_dek(&tx, "1", &wrapped_v1).await?;
        assert_ne!(wrapped_v1, wrapped_v2);
        assert_eq!(Ciphertext::parse(&wrapped_v1)?.key_version(), 1);
        assert_eq!(Ciphertext::parse(&wrapped_v2)?.key_version(), 2);
        assert_eq!(kms.decrypt_dek(&tx, "1", &wrapped_v2).await?, dek);
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn deleting_the_keks_shreds_what_they_wrapped() {
    let f = fixture("crypto_shredding").await;
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, "1").await?;
        let wrapped = kms.encrypt_dek(&tx, "1", &key()).await?;
        kms.delete_kek(&tx, "1").await?;
        assert!(matches!(
            kms.decrypt_dek(&tx, "1", &wrapped).await,
            Err(Error::KekNotFound { tenant_id, key_version: Some(1) }) if tenant_id == "1"
        ));
        Ok(())
    })
    .await
    .unwrap();
    assert!(f.versions_of("1").await.is_empty());
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn one_tenant_s_key_does_not_unwrap_another_s_dek() {
    let f = fixture("tenant_isolation").await;
    f.atomic(|tx, kms| async move {
        kms.rotate_kek(&tx, "1").await?;
        kms.rotate_kek(&tx, "2").await?;
        let dek = key();
        let wrapped = kms.encrypt_dek(&tx, "1", &dek).await?;
        assert_eq!(kms.decrypt_dek(&tx, "1", &wrapped).await?, dek);
        assert!(matches!(
            kms.decrypt_dek(&tx, "2", &wrapped).await,
            Err(Error::Decrypt)
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_first_contact_makes_the_key() {
    let f = fixture("first_contact").await;
    f.atomic(|tx, kms| async move {
        let dek = key();
        let wrapped = kms.encrypt_dek(&tx, "1", &dek).await?;
        assert_eq!(Ciphertext::parse(&wrapped)?.key_version(), 1);
        assert_eq!(kms.decrypt_dek(&tx, "1", &wrapped).await?, dek);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(f.versions_of("1").await, [1]);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_key_made_in_a_transaction_rolled_back_is_not_there() {
    let f = fixture("rolled_back").await;
    let outcome = f
        .atomic(|tx, kms| async move {
            kms.rotate_kek(&tx, "1").await?;
            Err::<(), _>(Error::Malformed(
                "the command failed after the key was made".into(),
            ))
        })
        .await;
    assert!(outcome.is_err());
    assert!(f.versions_of("1").await.is_empty());
}

/// Two transactions meet a new tenant: the first makes the key and holds
/// its transaction open; the second waits on the tenant's lock — it neither
/// fails on the primary key nor makes a version of its own — and, once the
/// first commits, uses version 1.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_first_contacts_at_once_share_one_key() {
    let f = Arc::new(fixture("first_contacts_at_once").await);
    let (made, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let first = {
        let (f, made, release) = (Arc::clone(&f), Arc::clone(&made), Arc::clone(&release));
        tokio::spawn(async move {
            f.atomic(|tx, kms| async move {
                let wrapped = kms.encrypt_dek(&tx, "1", &key()).await?;
                made.notify_one();
                release.notified().await;
                Ok(wrapped)
            })
            .await
        })
    };
    made.notified().await;
    let second = {
        let f = Arc::clone(&f);
        tokio::spawn(async move {
            f.atomic(|tx, kms| async move {
                let dek = key();
                let wrapped = kms.encrypt_dek(&tx, "1", &dek).await?;
                Ok((dek, wrapped))
            })
            .await
        })
    };
    // While the first holds the lock in its open transaction, the second waits.
    let mut second = second;
    tokio::select! {
        outcome = &mut second => panic!("the second did not wait: {:?}", outcome.unwrap().map(|_| ())),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    release.notify_one();
    let first = first.await.unwrap().unwrap();
    let (dek, wrapped) = second.await.unwrap().unwrap();
    assert_eq!(Ciphertext::parse(&first).unwrap().key_version(), 1);
    assert_eq!(Ciphertext::parse(&wrapped).unwrap().key_version(), 1);
    assert_eq!(f.versions_of("1").await, [1]);
    f.atomic(|tx, kms| async move {
        assert_eq!(kms.decrypt_dek(&tx, "1", &wrapped).await?, dek);
        Ok(())
    })
    .await
    .unwrap();
}

/// Two rotations at once: the second waits for the first to commit, then
/// makes the version after it.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_rotations_at_once_make_versions_one_and_two() {
    let f = Arc::new(fixture("rotations_at_once").await);
    let (made, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let first = {
        let (f, made, release) = (Arc::clone(&f), Arc::clone(&made), Arc::clone(&release));
        tokio::spawn(async move {
            f.atomic(|tx, kms| async move {
                let version = kms.rotate_kek(&tx, "1").await?;
                made.notify_one();
                release.notified().await;
                Ok(version)
            })
            .await
        })
    };
    made.notified().await;
    let mut second = {
        let f = Arc::clone(&f);
        tokio::spawn(async move {
            f.atomic(|tx, kms| async move { kms.rotate_kek(&tx, "1").await })
                .await
        })
    };
    tokio::select! {
        outcome = &mut second => panic!("the second did not wait: {:?}", outcome.unwrap()),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    release.notify_one();
    let first = first.await.unwrap().unwrap();
    let second = second.await.unwrap().unwrap();
    assert_eq!((first, second), (1, 2));
    assert_eq!(f.versions_of("1").await, [1, 2]);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_setups_at_once_agree() {
    let f = fixture("setups_at_once").await;
    f.sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!("DROP TABLE IF EXISTS {}", f.table))
                .await
                .unwrap();
            Ok::<_, Error>(())
        })
        .await
        .unwrap();
    let setup = || {
        f.sessions
            .session(async |session| f.kms.setup(&session).await)
    };
    let (a, b) = tokio::join!(setup(), setup());
    a.unwrap();
    b.unwrap();
    f.atomic(|tx, kms| async move { kms.rotate_kek(&tx, "1").await.map(|_| ()) })
        .await
        .unwrap();
}

/// A row the Python port wrote — its first KEK for `tenant-1` under the
/// master key `00 01 … 1f` — is read here, and unwraps the DEK it wrapped.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_key_the_python_port_wrote_is_read() {
    const KEK_V1: &str = "00000001a81366893e77f9851f6b13b9f2e4181704bcc43283f37b81741bc0fd0137c70d02cf28faaa22ac0a7b228367770147acc3f14d68debe95405cac4fc7";
    const DEK_UNDER_V1: &str = "00000001b3d9d1393913407b952682e4e96b8c8c21d18b59211e65daf7a6060c07a8d24fed1899f548955ca16f07d75ce27ba7d225f03e83ae932f8c348a3628";
    let hex = |text: &str| -> Vec<u8> {
        (0..text.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
            .collect()
    };
    let f = fixture_with_master("python_row", Key::new((0..32).collect::<Vec<u8>>())).await;
    let insert = format!(
        "INSERT INTO {} (tenant_id, key_version, encrypted_key, master_algorithm, key_algorithm) \
         VALUES ('tenant-1', 1, $1, 'AES-256-GCM', 'AES-256-GCM')",
        f.table
    );
    f.sessions
        .session(async |session| {
            session
                .connection()
                .execute(&insert, &[&hex(KEK_V1)])
                .await?;
            Ok::<_, Error>(())
        })
        .await
        .unwrap();
    f.atomic(|tx, kms| async move {
        let dek = kms.decrypt_dek(&tx, "tenant-1", &hex(DEK_UNDER_V1)).await?;
        assert_eq!(dek.as_bytes(), (0x40..0x60).collect::<Vec<u8>>());
        // and the row is the current key: nothing new is made
        assert_eq!(
            Ciphertext::parse(&kms.encrypt_dek(&tx, "tenant-1", &dek).await?)?.key_version(),
            1
        );
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(f.versions_of("tenant-1").await, [1]);
}
