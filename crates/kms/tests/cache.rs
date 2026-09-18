//! The cache in front of a key management service: what it asks the
//! service, and what it answers from memory.

use std::sync::Mutex;
use std::time::Duration;

use ascetic_ddd_kms::{Cached, Error, Key, KeyManagementService};
use ascetic_ddd_session::testing::{MemorySession, MemorySessionPool};
use ascetic_ddd_session::{Session, SessionPool};

/// Counts what it is asked; "unwraps" by taking the wrapped bytes as the
/// key, and "wraps" by taking the key's bytes.
#[derive(Default)]
struct Counting {
    unwraps: Mutex<u32>,
    deletes: Mutex<u32>,
}

impl Counting {
    fn unwraps(&self) -> u32 {
        *self.unwraps.lock().unwrap()
    }
}

impl<S: Session> KeyManagementService<S> for Counting {
    async fn encrypt_dek(&self, _: &S, _: &str, dek: &Key) -> Result<Vec<u8>, Error> {
        Ok(dek.as_bytes().to_vec())
    }

    async fn decrypt_dek(
        &self,
        _: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Key, Error> {
        *self.unwraps.lock().unwrap() += 1;
        if tenant_id == "gone" {
            return Err(Error::KekNotFound {
                tenant_id: tenant_id.to_owned(),
                key_version: None,
            });
        }
        Ok(Key::new(encrypted_dek.to_vec()))
    }

    async fn generate_dek(&self, _: &S, _: &str) -> Result<(Key, Vec<u8>), Error> {
        Ok((Key::new(vec![7; 32]), vec![7; 32]))
    }

    async fn rotate_kek(&self, _: &S, _: &str) -> Result<u32, Error> {
        Ok(1)
    }

    async fn rewrap_dek(&self, _: &S, _: &str, encrypted_dek: &[u8]) -> Result<Vec<u8>, Error> {
        Ok(encrypted_dek.to_vec())
    }

    async fn delete_kek(&self, _: &S, _: &str) -> Result<(), Error> {
        *self.deletes.lock().unwrap() += 1;
        Ok(())
    }
}

/// Runs `scope` with a session; the session goes by value, as a scope
/// borrowing it could not be shown `Send` (ADR-0004).
async fn with_session<T, Fut>(scope: impl FnOnce(MemorySession) -> Fut + Send) -> T
where
    Fut: Future<Output = T> + Send,
    T: Send,
{
    MemorySessionPool::new()
        .session(async |session| Ok::<_, ascetic_ddd_session::SessionError>(scope(session).await))
        .await
        .unwrap()
}

#[tokio::test]
async fn the_same_wrapped_key_is_unwrapped_once() {
    let cached = Cached::new(Counting::default());
    with_session(|session| async move {
        let first = cached
            .decrypt_dek(&session, "t1", b"wrapped-1")
            .await
            .unwrap();
        let again = cached
            .decrypt_dek(&session, "t1", b"wrapped-1")
            .await
            .unwrap();
        assert_eq!(first, again);
        assert_eq!(cached.inner().unwraps(), 1);
        // Another wrapped key, and the same bytes for another tenant, are asked.
        cached
            .decrypt_dek(&session, "t1", b"wrapped-2")
            .await
            .unwrap();
        cached
            .decrypt_dek(&session, "t2", b"wrapped-1")
            .await
            .unwrap();
        assert_eq!(cached.inner().unwraps(), 3);
        assert_eq!(cached.len(), 3);
    })
    .await;
}

#[tokio::test]
async fn a_zero_time_to_live_keeps_nothing() {
    let cached = Cached::new(Counting::default()).with_ttl(Duration::ZERO);
    with_session(|session| async move {
        cached
            .decrypt_dek(&session, "t1", b"wrapped-1")
            .await
            .unwrap();
        cached
            .decrypt_dek(&session, "t1", b"wrapped-1")
            .await
            .unwrap();
        assert_eq!(cached.inner().unwraps(), 2);
        assert!(cached.is_empty());
    })
    .await;
}

#[tokio::test]
async fn the_oldest_key_goes_when_the_cache_is_full() {
    let cached = Cached::new(Counting::default()).with_capacity(2);
    with_session(|session| async move {
        cached
            .decrypt_dek(&session, "t1", b"wrapped-1")
            .await
            .unwrap();
        cached
            .decrypt_dek(&session, "t1", b"wrapped-2")
            .await
            .unwrap();
        cached
            .decrypt_dek(&session, "t1", b"wrapped-3")
            .await
            .unwrap();
        assert_eq!(cached.len(), 2);
        cached
            .decrypt_dek(&session, "t1", b"wrapped-3")
            .await
            .unwrap();
        assert_eq!(cached.inner().unwraps(), 3, "the newest is still held");
        cached
            .decrypt_dek(&session, "t1", b"wrapped-1")
            .await
            .unwrap();
        assert_eq!(cached.inner().unwraps(), 4, "the oldest was let go");
    })
    .await;
}

#[tokio::test]
async fn deleting_a_tenant_s_kek_forgets_its_keys_here() {
    let cached = Cached::new(Counting::default());
    with_session(|session| async move {
        cached
            .decrypt_dek(&session, "t1", b"wrapped-1")
            .await
            .unwrap();
        cached
            .decrypt_dek(&session, "t2", b"wrapped-1")
            .await
            .unwrap();
        cached.delete_kek(&session, "t1").await.unwrap();
        assert_eq!(cached.len(), 1);
        cached
            .decrypt_dek(&session, "t2", b"wrapped-1")
            .await
            .unwrap();
        assert_eq!(
            cached.inner().unwraps(),
            2,
            "the other tenant's key is still held"
        );
    })
    .await;
}

#[tokio::test]
async fn a_refusal_is_not_kept() {
    let cached = Cached::new(Counting::default());
    with_session(|session| async move {
        assert!(
            cached
                .decrypt_dek(&session, "gone", b"wrapped-1")
                .await
                .is_err()
        );
        assert!(
            cached
                .decrypt_dek(&session, "gone", b"wrapped-1")
                .await
                .is_err()
        );
        assert_eq!(cached.inner().unwraps(), 2);
        assert!(cached.is_empty());
    })
    .await;
}
