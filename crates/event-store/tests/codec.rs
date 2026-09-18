//! The payload codecs, without a store: what the Python `test_codec`
//! checks.

use ascetic_ddd_event_store::{EncryptionCodec, JsonCodec, PayloadCodec, ZlibCodec};
use ascetic_ddd_kms::Algorithm;
use serde_json::json;

fn cipher() -> Box<dyn ascetic_ddd_kms::Cipher> {
    let key = Algorithm::Aes256Gcm.generate_key().unwrap();
    Algorithm::Aes256Gcm.cipher(&key, "stream").unwrap()
}

#[test]
fn json_round_trips() {
    let payload = json!({ "name": "test", "value": 42 });
    let encoded = JsonCodec.encode(&payload).unwrap();
    assert_eq!(JsonCodec.decode(&encoded).unwrap(), payload);
}

#[test]
fn zlib_round_trips_and_differs_from_plain_json() {
    let payload = json!({ "name": "test", "value": 42 });
    let compressed = ZlibCodec::new(JsonCodec);
    let encoded = compressed.encode(&payload).unwrap();
    assert_ne!(encoded, JsonCodec.encode(&payload).unwrap());
    assert_eq!(compressed.decode(&encoded).unwrap(), payload);
}

#[test]
fn encryption_round_trips_and_differs_from_plain_json() {
    let payload = json!({ "name": "test", "value": 42 });
    let sealed = EncryptionCodec::new(cipher(), JsonCodec);
    let encoded = sealed.encode(&payload).unwrap();
    assert_ne!(encoded, JsonCodec.encode(&payload).unwrap());
    assert_eq!(sealed.decode(&encoded).unwrap(), payload);
}

#[test]
fn every_encoding_takes_a_fresh_nonce() {
    let sealed = EncryptionCodec::new(cipher(), JsonCodec);
    let payload = json!({ "name": "test" });
    assert_ne!(
        sealed.encode(&payload).unwrap(),
        sealed.encode(&payload).unwrap()
    );
}

#[test]
fn the_chain_of_the_sources_round_trips() {
    let chain = EncryptionCodec::new(cipher(), ZlibCodec::new(JsonCodec));
    let payload = json!({ "order": "order-7", "lines": [1, 2, 3] });
    assert_eq!(
        chain.decode(&chain.encode(&payload).unwrap()).unwrap(),
        payload
    );
}

#[test]
fn another_key_does_not_open_it() {
    let payload = json!({ "name": "test" });
    let encoded = EncryptionCodec::new(cipher(), JsonCodec)
        .encode(&payload)
        .unwrap();
    assert!(
        EncryptionCodec::new(cipher(), JsonCodec)
            .decode(&encoded)
            .is_err()
    );
}
