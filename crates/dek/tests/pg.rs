//! Integration tests for the PostgreSQL DEK store: what the Python
//! `test_dek_store` checks, plus the lock on a new resource. They need a
//! live database:
//!
//! ```text
//! ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
//!     cargo test -p ascetic-ddd-dek --features pg -- --ignored
//! ```

#![cfg(feature = "pg")]

use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use ascetic_ddd_dek::{DekStore, Error, PgDekStore, Resource};
use ascetic_ddd_kms::domain::VERSION_SIZE;
use ascetic_ddd_kms::{Algorithm, Cipher, KeyManagementService, PgKeyManagementService};
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

type Store = PgDekStore<PgKeyManagementService>;

struct Fixture {
    sessions: PgSessionPool,
    deks: Arc<Store>,
    tables: (String, String),
}

fn order(tenant_id: &str, id: &str) -> Resource {
    Resource::new(tenant_id, "Order", id)
}

fn version_of(sealed: &[u8]) -> u32 {
    u32::from_be_bytes(sealed[..VERSION_SIZE].try_into().unwrap())
}

/// Tables of its own per test, so tests run in parallel; a fresh master key.
async fn fixture(name: &str) -> Fixture {
    let tables = (format!("deks_{name}"), format!("kms_keys_dek_{name}"));
    let sessions = PgSessionPool::new(pool());
    let kms = PgKeyManagementService::new(Algorithm::Aes256Gcm.generate_key().unwrap())
        .with_table(Identifier::new(&tables.1).unwrap());
    let deks = PgDekStore::new(kms).with_table(Identifier::new(&tables.0).unwrap());
    sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!(
                    "DROP TABLE IF EXISTS {}; DROP TABLE IF EXISTS {};",
                    tables.0, tables.1
                ))
                .await
                .unwrap();
            deks.kms().setup(&session).await.unwrap();
            deks.setup(&session).await
        })
        .await
        .unwrap();
    Fixture {
        sessions,
        deks: Arc::new(deks),
        tables,
    }
}

impl Fixture {
    /// Runs `scope` in one transaction; the session goes by value (ADR-0004).
    async fn atomic<T, Fut>(
        &self,
        scope: impl FnOnce(PgSession, Arc<Store>) -> Fut + Send,
    ) -> Result<T, Error>
    where
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let deks = Arc::clone(&self.deks);
        self.sessions
            .session(async |session| session.atomic(async |tx| scope(tx, deks).await).await)
            .await
    }

    async fn versions_of(&self, resource: &Resource) -> Vec<i32> {
        let sql = format!(
            "SELECT version FROM {} WHERE tenant_id = $1 AND kind = $2 AND resource_id = $3 ORDER BY version",
            self.tables.0
        );
        self.sessions
            .session(async |session| {
                let rows = session
                    .connection()
                    .query(
                        &sql,
                        &[&resource.tenant_id(), &resource.kind(), resource.id()],
                    )
                    .await?;
                Ok::<_, Error>(rows.iter().map(|row| row.get::<_, i32>(0)).collect())
            })
            .await
            .unwrap()
    }
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_first_contact_makes_version_one() {
    let f = fixture("first_contact").await;
    f.atomic(|tx, deks| async move {
        let cipher = deks.get_or_create(&tx, &order("1", "order-1")).await?;
        let sealed = cipher.encrypt(b"hello world")?;
        assert_ne!(sealed, b"hello world");
        assert_eq!(version_of(&sealed), 1);
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(f.versions_of(&order("1", "order-1")).await, [1]);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_second_contact_gives_the_same_key() {
    let f = fixture("second_contact").await;
    f.atomic(|tx, deks| async move {
        let first = deks.get_or_create(&tx, &order("1", "order-1")).await?;
        let second = deks.get_or_create(&tx, &order("1", "order-1")).await?;
        assert_eq!(second.decrypt(&first.encrypt(b"hello")?)?, b"hello");
        Ok(())
    })
    .await
    .unwrap();
    assert_eq!(f.versions_of(&order("1", "order-1")).await, [1]);
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_known_version_is_fetched_by_it() {
    let f = fixture("known_version").await;
    f.atomic(|tx, deks| async move {
        let cipher = deks.get_or_create(&tx, &order("1", "order-1")).await?;
        let sealed = cipher.encrypt(b"hello")?;
        let loaded = deks
            .get(&tx, &order("1", "order-1"), version_of(&sealed))
            .await?;
        assert_eq!(loaded.version(), 1);
        assert_eq!(loaded.decrypt(&sealed)?, b"hello");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_missing_dek_is_reported() {
    let f = fixture("missing").await;
    f.atomic(|tx, deks| async move {
        assert!(matches!(
            deks.get(&tx, &order("1", "order-1"), 1).await,
            Err(Error::DekNotFound {
                version: Some(1),
                ..
            })
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_resources_get_two_keys() {
    let f = fixture("two_resources").await;
    f.atomic(|tx, deks| async move {
        let one = deks.get_or_create(&tx, &order("1", "order-1")).await?;
        let two = deks.get_or_create(&tx, &order("1", "order-2")).await?;
        assert!(matches!(
            two.decrypt(&one.encrypt(b"hello")?),
            Err(ascetic_ddd_kms::Error::Decrypt)
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_tenants_get_two_keys() {
    let f = fixture("two_tenants").await;
    f.atomic(|tx, deks| async move {
        let one = deks.get_or_create(&tx, &order("1", "order-1")).await?;
        let two = deks.get_or_create(&tx, &order("2", "order-1")).await?;
        assert!(matches!(
            two.decrypt(&one.encrypt(b"hello")?),
            Err(ascetic_ddd_kms::Error::Decrypt)
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_dek_survives_a_kek_rotation() {
    let f = fixture("kek_rotation").await;
    f.atomic(|tx, deks| async move {
        let cipher = deks.get_or_create(&tx, &order("1", "order-1")).await?;
        let sealed = cipher.encrypt(b"hello")?;
        deks.kms().rotate_kek(&tx, "1").await?;
        let loaded = deks
            .get(&tx, &order("1", "order-1"), version_of(&sealed))
            .await?;
        assert_eq!(loaded.decrypt(&sealed)?, b"hello");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn deleting_the_dek_forgets_the_resource() {
    let f = fixture("delete").await;
    f.atomic(|tx, deks| async move {
        deks.get_or_create(&tx, &order("1", "order-1")).await?;
        deks.delete(&tx, &order("1", "order-1")).await?;
        assert!(matches!(
            deks.get(&tx, &order("1", "order-1"), 1).await,
            Err(Error::DekNotFound { .. })
        ));
        Ok(())
    })
    .await
    .unwrap();
    assert!(f.versions_of(&order("1", "order-1")).await.is_empty());
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_rewrap_after_a_rotation_moves_every_dek_of_the_tenant() {
    let f = fixture("rewrap").await;
    f.atomic(|tx, deks| async move {
        let one = deks.get_or_create(&tx, &order("1", "order-1")).await?;
        let two = deks.get_or_create(&tx, &order("1", "order-2")).await?;
        let sealed_one = one.encrypt(b"hello")?;
        let sealed_two = two.encrypt(b"hello")?;
        deks.kms().rotate_kek(&tx, "1").await?;
        assert_eq!(deks.rewrap(&tx, "1").await?, 2);
        let one = deks
            .get(&tx, &order("1", "order-1"), version_of(&sealed_one))
            .await?;
        let two = deks
            .get(&tx, &order("1", "order-2"), version_of(&sealed_two))
            .await?;
        assert_eq!(one.decrypt(&sealed_one)?, b"hello");
        assert_eq!(two.decrypt(&sealed_two)?, b"hello");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn the_keyring_opens_every_version_and_seals_under_the_newest() {
    let f = fixture("keyring").await;
    let insert = format!(
        "INSERT INTO {} (tenant_id, kind, resource_id, version, encrypted_dek, algorithm) \
         VALUES ($1, $2, $3, 2, $4, 'AES-256-GCM')",
        f.tables.0
    );
    f.atomic(|tx, deks| async move {
        let resource = order("1", "order-1");
        let v1 = deks.get_or_create(&tx, &resource).await?;
        let sealed_v1 = v1.encrypt(b"hello")?;
        // A second version, as an algorithm migration would write it.
        let (_, wrapped) = deks.kms().generate_dek(&tx, "1").await?;
        tx.connection()
            .execute(
                &insert,
                &[
                    &resource.tenant_id(),
                    &resource.kind(),
                    resource.id(),
                    &wrapped,
                ],
            )
            .await?;

        let keyring = deks.get_all(&tx, &resource).await?;
        assert_eq!(keyring.versions().collect::<Vec<_>>(), [1, 2]);
        assert_eq!(keyring.decrypt(&sealed_v1)?, b"hello");
        let sealed_v2 = keyring.encrypt(b"hello")?;
        assert_eq!(version_of(&sealed_v2), 2);
        assert_eq!(keyring.decrypt(&sealed_v2)?, b"hello");
        assert_eq!(
            deks.get(&tx, &resource, 2).await?.decrypt(&sealed_v2)?,
            b"hello"
        );
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn a_keyring_of_nothing_is_reported() {
    let f = fixture("empty_keyring").await;
    f.atomic(|tx, deks| async move {
        assert!(matches!(
            deks.get_all(&tx, &order("1", "order-1")).await,
            Err(Error::DekNotFound { version: None, .. })
        ));
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn shredding_the_tenant_s_kek_shreds_its_deks() {
    let f = fixture("crypto_shredding").await;
    f.atomic(|tx, deks| async move {
        deks.get_or_create(&tx, &order("1", "order-1")).await?;
        deks.kms().delete_kek(&tx, "1").await?;
        assert!(matches!(
            deks.get(&tx, &order("1", "order-1"), 1).await,
            Err(Error::Kms(ascetic_ddd_kms::Error::KekNotFound { .. }))
        ));
        Ok(())
    })
    .await
    .unwrap();
}

/// Two transactions meet a new resource: the first makes the key and holds
/// its transaction open; the second waits on the resource's lock, and once
/// the first commits, uses the same key.
#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_first_contacts_at_once_share_one_dek() {
    let f = Arc::new(fixture("first_contacts_at_once").await);
    let (made, release) = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let first = {
        let (f, made, release) = (Arc::clone(&f), Arc::clone(&made), Arc::clone(&release));
        tokio::spawn(async move {
            f.atomic(|tx, deks| async move {
                let cipher = deks.get_or_create(&tx, &order("1", "order-1")).await?;
                let sealed = cipher.encrypt(b"first")?;
                made.notify_one();
                release.notified().await;
                Ok(sealed)
            })
            .await
        })
    };
    made.notified().await;
    let mut second = {
        let f = Arc::clone(&f);
        tokio::spawn(async move {
            f.atomic(|tx, deks| async move {
                let cipher = deks.get_or_create(&tx, &order("1", "order-1")).await?;
                Ok(cipher.encrypt(b"second")?)
            })
            .await
        })
    };
    tokio::select! {
        outcome = &mut second => panic!("the second did not wait: {:?}", outcome.unwrap().map(|_| ())),
        _ = tokio::time::sleep(Duration::from_millis(300)) => {}
    }
    release.notify_one();
    let sealed_first = first.await.unwrap().unwrap();
    let sealed_second = second.await.unwrap().unwrap();
    assert_eq!(f.versions_of(&order("1", "order-1")).await, [1]);
    f.atomic(|tx, deks| async move {
        let cipher = deks.get(&tx, &order("1", "order-1"), 1).await?;
        assert_eq!(cipher.decrypt(&sealed_first)?, b"first");
        assert_eq!(cipher.decrypt(&sealed_second)?, b"second");
        Ok(())
    })
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "needs a live PostgreSQL"]
async fn two_setups_at_once_agree() {
    let f = fixture("setups_at_once").await;
    f.sessions
        .session(async |session| {
            session
                .connection()
                .batch_execute(&format!("DROP TABLE IF EXISTS {}", f.tables.0))
                .await
                .unwrap();
            Ok::<_, Error>(())
        })
        .await
        .unwrap();
    let setup = || {
        f.sessions
            .session(async |session| f.deks.setup(&session).await)
    };
    let (a, b) = tokio::join!(setup(), setup());
    a.unwrap();
    b.unwrap();
    f.atomic(|tx, deks| async move {
        deks.get_or_create(&tx, &order("1", "order-1"))
            .await
            .map(|_| ())
    })
    .await
    .unwrap();
}
