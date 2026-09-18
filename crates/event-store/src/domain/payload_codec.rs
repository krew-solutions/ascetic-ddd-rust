//! The payload as bytes, and back: JSON, compressed, sealed — one inside
//! the other, as the sources chain `EncryptionCodec(cipher,
//! ZlibCodec(JsonCodec()))`.

use std::io::{Read, Write};

use ascetic_ddd_kms::Cipher;
use flate2::Compression;
use flate2::read::ZlibDecoder;
use flate2::write::ZlibEncoder;
use serde_json::Value;

use crate::error::BoxError;

/// Turns a payload into the bytes the table keeps, and back.
pub trait PayloadCodec: Send + Sync {
    /// The bytes of `payload`.
    fn encode(&self, payload: &Value) -> Result<Vec<u8>, BoxError>;

    /// The payload of `bytes`.
    fn decode(&self, bytes: &[u8]) -> Result<Value, BoxError>;
}

/// The payload as JSON text.
#[derive(Clone, Copy, Debug, Default)]
pub struct JsonCodec;

impl PayloadCodec for JsonCodec {
    fn encode(&self, payload: &Value) -> Result<Vec<u8>, BoxError> {
        Ok(serde_json::to_vec(payload)?)
    }

    fn decode(&self, bytes: &[u8]) -> Result<Value, BoxError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// What the delegate makes, compressed with zlib.
#[derive(Clone, Debug)]
pub struct ZlibCodec<C> {
    delegate: C,
}

impl<C> ZlibCodec<C> {
    /// Compresses what `delegate` makes.
    pub fn new(delegate: C) -> Self {
        ZlibCodec { delegate }
    }
}

impl<C: PayloadCodec> PayloadCodec for ZlibCodec<C> {
    fn encode(&self, payload: &Value) -> Result<Vec<u8>, BoxError> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(&self.delegate.encode(payload)?)?;
        Ok(encoder.finish()?)
    }

    fn decode(&self, bytes: &[u8]) -> Result<Value, BoxError> {
        let mut inflated = Vec::new();
        ZlibDecoder::new(bytes).read_to_end(&mut inflated)?;
        self.delegate.decode(&inflated)
    }
}

/// What the delegate makes, sealed with a cipher — a stream's key, so that
/// the bytes at rest are ciphertext.
pub struct EncryptionCodec<C> {
    cipher: Box<dyn Cipher>,
    delegate: C,
}

impl<C> EncryptionCodec<C> {
    /// Seals what `delegate` makes with `cipher`.
    pub fn new(cipher: Box<dyn Cipher>, delegate: C) -> Self {
        EncryptionCodec { cipher, delegate }
    }
}

impl<C: PayloadCodec> PayloadCodec for EncryptionCodec<C> {
    fn encode(&self, payload: &Value) -> Result<Vec<u8>, BoxError> {
        Ok(self.cipher.encrypt(&self.delegate.encode(payload)?)?)
    }

    fn decode(&self, bytes: &[u8]) -> Result<Value, BoxError> {
        self.delegate.decode(&self.cipher.decrypt(bytes)?)
    }
}

impl<C> std::fmt::Debug for EncryptionCodec<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionCodec").finish_non_exhaustive()
    }
}
