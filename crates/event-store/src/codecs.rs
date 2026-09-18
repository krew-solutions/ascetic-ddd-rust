//! Where a stream's payload codec comes from.
//!
//! The write path seals under the stream's current data-encryption key,
//! made on first contact; the read path opens with every version the
//! stream has. Both come from the DEK store, in the caller's session —
//! [`Encrypted`]. [`Plain`] keeps JSON and no keys, for development and
//! for the tests of what is not encryption.

use ascetic_ddd_dek::{DekStore, Resource};
use ascetic_ddd_session::Session;
use futures::future::BoxFuture;

use crate::domain::{EncryptionCodec, JsonCodec, PayloadCodec, StreamId, ZlibCodec};
use crate::error::Error;

/// The codec a stream is written with, and the one it is read with.
pub trait Codecs<S>: Send + Sync {
    /// The codec to write `stream` with.
    fn for_writing<'a>(
        &'a self,
        session: &'a S,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Box<dyn PayloadCodec>, Error>>;

    /// The codec to read `stream` with.
    fn for_reading<'a>(
        &'a self,
        session: &'a S,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Box<dyn PayloadCodec>, Error>>;
}

/// JSON, and nothing else: the payload rests in the clear.
#[derive(Clone, Copy, Debug, Default)]
pub struct Plain;

impl<S: Sync> Codecs<S> for Plain {
    fn for_writing<'a>(
        &'a self,
        _: &'a S,
        _: &'a StreamId,
    ) -> BoxFuture<'a, Result<Box<dyn PayloadCodec>, Error>> {
        Box::pin(async { Ok(Box::new(JsonCodec) as Box<dyn PayloadCodec>) })
    }

    fn for_reading<'a>(
        &'a self,
        _: &'a S,
        _: &'a StreamId,
    ) -> BoxFuture<'a, Result<Box<dyn PayloadCodec>, Error>> {
        Box::pin(async { Ok(Box::new(JsonCodec) as Box<dyn PayloadCodec>) })
    }
}

/// JSON, compressed, sealed with the stream's data-encryption key from
/// `D`: the chain of the sources.
pub struct Encrypted<D> {
    deks: D,
}

impl<D> Encrypted<D> {
    /// Codecs over the keys of `deks`.
    pub fn new(deks: D) -> Self {
        Encrypted { deks }
    }

    /// The store the keys come from.
    pub fn deks(&self) -> &D {
        &self.deks
    }
}

impl<S, D> Codecs<S> for Encrypted<D>
where
    S: Session + Sync,
    D: DekStore<S>,
{
    /// Under the stream's current key, made if the stream has none.
    fn for_writing<'a>(
        &'a self,
        session: &'a S,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Box<dyn PayloadCodec>, Error>> {
        Box::pin(async move {
            let cipher = self
                .deks
                .get_or_create(session, &Resource::from(stream))
                .await?;
            Ok(Box::new(EncryptionCodec::new(
                Box::new(cipher),
                ZlibCodec::new(JsonCodec),
            )) as Box<dyn PayloadCodec>)
        })
    }

    /// With every version of the stream's key.
    fn for_reading<'a>(
        &'a self,
        session: &'a S,
        stream: &'a StreamId,
    ) -> BoxFuture<'a, Result<Box<dyn PayloadCodec>, Error>> {
        Box::pin(async move {
            let keyring = self.deks.get_all(session, &Resource::from(stream)).await?;
            Ok(Box::new(EncryptionCodec::new(
                Box::new(keyring),
                ZlibCodec::new(JsonCodec),
            )) as Box<dyn PayloadCodec>)
        })
    }
}

impl<D> std::fmt::Debug for Encrypted<D> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Encrypted").finish_non_exhaustive()
    }
}
