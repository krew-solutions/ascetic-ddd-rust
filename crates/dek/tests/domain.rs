//! The model, without a store: the resource's canonical text, and the
//! ciphers that carry the version along.

use ascetic_ddd_dek::{Keyring, Resource, VersionedCipher};
use ascetic_ddd_kms::domain::VERSION_SIZE;
use ascetic_ddd_kms::{Algorithm, Cipher, Error, Key};
use serde_json::json;

fn key() -> Key {
    Algorithm::Aes256Gcm.generate_key().unwrap()
}

fn cipher(key: &Key) -> Box<dyn Cipher> {
    Algorithm::Aes256Gcm.cipher(key, "aad").unwrap()
}

// The resource.

#[test]
fn a_resource_has_one_text_however_its_id_was_built() {
    let one = Resource::new("tenant-1", "Order", json!({ "shop": 7, "number": "A-1" }));
    let other = Resource::new("tenant-1", "Order", json!({ "number": "A-1", "shop": 7 }));
    assert_eq!(one.canonical(), other.canonical());
    assert_eq!(
        one.canonical(),
        r#"["tenant-1","Order",{"number":"A-1","shop":7}]"#
    );
}

#[test]
fn a_string_id_and_a_number_id_are_two_resources() {
    let text = Resource::new("t", "Order", "1");
    let number = Resource::new("t", "Order", 1);
    assert_ne!(text.canonical(), number.canonical());
    assert_eq!(text.to_string(), r#"["t","Order","1"]"#);
    assert_eq!(number.to_string(), r#"["t","Order",1]"#);
}

#[test]
fn the_parts_cannot_run_into_each_other() {
    let one = Resource::new("a:b", "c", "d");
    let other = Resource::new("a", "b:c", "d");
    assert_ne!(one.canonical(), other.canonical());
}

// One version.

#[test]
fn a_versioned_cipher_names_its_version_and_refuses_another() {
    let key = key();
    let v3 = VersionedCipher::new(3, cipher(&key));
    let sealed = v3.encrypt(b"plain").unwrap();
    assert_eq!(&sealed[..VERSION_SIZE], &[0, 0, 0, 3]);
    assert_eq!(v3.decrypt(&sealed).unwrap(), b"plain");
    let v4 = VersionedCipher::new(4, cipher(&key));
    assert!(matches!(
        v4.decrypt(&sealed),
        Err(Error::WrongKeyVersion {
            expected: 4,
            found: 3
        })
    ));
}

#[test]
fn bytes_too_short_to_be_versioned_are_refused() {
    let v1 = VersionedCipher::new(1, cipher(&key()));
    assert!(matches!(v1.decrypt(&[0, 0, 1]), Err(Error::Malformed(_))));
}

#[test]
fn a_versioned_cipher_makes_keys_of_its_kind() {
    let v1 = VersionedCipher::new(1, cipher(&key()));
    assert_eq!(v1.generate_key().unwrap().as_bytes().len(), 32);
}

// Every version.

#[test]
fn a_keyring_seals_under_the_newest_and_opens_every_version() {
    let (k1, k2, k3) = (key(), key(), key());
    let ring = Keyring::new(2, cipher(&k2))
        .with(1, cipher(&k1))
        .with(3, cipher(&k3));
    assert_eq!(ring.newest(), 3);
    assert_eq!(ring.versions().collect::<Vec<_>>(), [1, 2, 3]);
    assert_eq!(
        &ring.encrypt(b"new").unwrap()[..VERSION_SIZE],
        &[0, 0, 0, 3]
    );
    for (version, key) in [(1, &k1), (2, &k2), (3, &k3)] {
        let sealed = VersionedCipher::new(version, cipher(key))
            .encrypt(b"old")
            .unwrap();
        assert_eq!(ring.decrypt(&sealed).unwrap(), b"old", "version {version}");
    }
    let sealed = VersionedCipher::new(9, cipher(&k1)).encrypt(b"?").unwrap();
    assert!(matches!(
        ring.decrypt(&sealed),
        Err(Error::NoKeyOfVersion(9))
    ));
}

#[test]
fn a_version_given_again_replaces_the_one_there() {
    let (old, new) = (key(), key());
    let sealed_by_old = VersionedCipher::new(1, cipher(&old)).encrypt(b"x").unwrap();
    let ring = Keyring::new(1, cipher(&old)).with(1, cipher(&new));
    assert_eq!(ring.versions().collect::<Vec<_>>(), [1]);
    assert!(matches!(ring.decrypt(&sealed_by_old), Err(Error::Decrypt)));
}

#[test]
fn what_a_keyring_seals_its_newest_version_opens() {
    let k = key();
    let ring = Keyring::new(1, cipher(&k)).with(2, cipher(&key()));
    let sealed = ring.encrypt(b"plain").unwrap();
    assert!(matches!(
        VersionedCipher::new(1, cipher(&k)).decrypt(&sealed),
        Err(Error::WrongKeyVersion {
            expected: 1,
            found: 2
        })
    ));
    assert_eq!(ring.decrypt(&sealed).unwrap(), b"plain");
}
