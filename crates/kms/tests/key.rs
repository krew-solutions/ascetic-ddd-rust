//! The keys, without a service: what the Python `test_models` and Go
//! `models_test` check, plus what the Python port wrapped.

use ascetic_ddd_kms::{Algorithm, Ciphertext, Error, Key, MASTER_KEY_VERSION, MasterKey};

fn key() -> Key {
    Algorithm::Aes256Gcm.generate_key().unwrap()
}

fn master(tenant_id: &str, key: &Key) -> MasterKey {
    MasterKey::new(tenant_id, key, Algorithm::Aes256Gcm).unwrap()
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

// The master key as a cipher.

#[test]
fn a_ciphertext_opens_under_the_key_that_sealed_it() {
    let master = master("t1", &key());
    let ciphertext = master.encrypt(b"hello world").unwrap();
    assert_eq!(ciphertext.key_version(), MASTER_KEY_VERSION);
    assert_eq!(master.decrypt(&ciphertext).unwrap(), b"hello world");
}

#[test]
fn sealing_twice_gives_two_ciphertexts() {
    let master = master("t1", &key());
    let first = master.encrypt(b"hello world").unwrap();
    let second = master.encrypt(b"hello world").unwrap();
    assert_ne!(first, second);
}

#[test]
fn another_tenant_does_not_open_it() {
    let key = key();
    let ciphertext = master("t1", &key).encrypt(b"secret").unwrap();
    assert!(matches!(
        master("t2", &key).decrypt(&ciphertext),
        Err(Error::Decrypt)
    ));
}

// The master key over KEKs.

#[test]
fn the_first_kek_is_version_one() {
    let master = master("tenant-1", &key());
    let kek = master.generate_kek(Algorithm::Aes256Gcm).unwrap();
    assert_eq!(kek.tenant_id(), "tenant-1");
    assert_eq!(kek.version(), 1);
    assert_eq!(kek.algorithm(), Algorithm::Aes256Gcm);
    assert_eq!(kek.encrypted_key().key_version(), MASTER_KEY_VERSION);
    assert_eq!(master.decrypt(kek.encrypted_key()).unwrap().len(), 32);
}

#[test]
fn a_kek_loads_back_from_its_wrapped_form() {
    let master = master("tenant-1", &key());
    let kek = master.generate_kek(Algorithm::Aes256Gcm).unwrap();
    let dek = key();
    let wrapped = kek.encrypt(dek.as_bytes()).unwrap();

    let loaded = master
        .load_kek(kek.encrypted_key().clone(), kek.version(), kek.algorithm())
        .unwrap();
    assert_eq!(loaded.tenant_id(), kek.tenant_id());
    assert_eq!(loaded.version(), kek.version());
    assert_eq!(loaded.encrypted_key(), kek.encrypted_key());
    assert_eq!(loaded.decrypt(&wrapped).unwrap(), dek.as_bytes());
}

#[test]
fn a_kek_does_not_load_under_another_tenant() {
    let key = key();
    let kek = master("t1", &key)
        .generate_kek(Algorithm::Aes256Gcm)
        .unwrap();
    assert!(matches!(
        master("t2", &key).load_kek(kek.encrypted_key().clone(), 1, Algorithm::Aes256Gcm),
        Err(Error::Decrypt)
    ));
}

#[test]
fn a_kek_does_not_load_under_another_master_key() {
    let kek = master("t1", &key())
        .generate_kek(Algorithm::Aes256Gcm)
        .unwrap();
    assert!(matches!(
        master("t1", &key()).load_kek(kek.encrypted_key().clone(), 1, Algorithm::Aes256Gcm),
        Err(Error::Decrypt)
    ));
}

#[test]
fn rotation_makes_the_next_version() {
    let master = master("tenant-1", &key());
    let kek = master.generate_kek(Algorithm::Aes256Gcm).unwrap();
    let rotated = master.rotate_kek(&kek).unwrap();
    assert_eq!(rotated.version(), kek.version() + 1);
    assert_eq!(rotated.tenant_id(), kek.tenant_id());
    assert_eq!(rotated.algorithm(), kek.algorithm());
    assert_ne!(rotated.encrypted_key(), kek.encrypted_key());
}

// The KEK over DEKs.

#[test]
fn a_kek_wraps_a_dek_and_unwraps_it() {
    let kek = master("tenant-1", &key())
        .generate_kek(Algorithm::Aes256Gcm)
        .unwrap();
    let dek = key();
    let wrapped = kek.encrypt(dek.as_bytes()).unwrap();
    assert_eq!(kek.decrypt(&wrapped).unwrap(), dek.as_bytes());
}

#[test]
fn the_ciphertext_names_the_kek_version_on_the_wire() {
    let master = master("tenant-1", &key());
    let kek = master
        .rotate_kek(&master.generate_kek(Algorithm::Aes256Gcm).unwrap())
        .unwrap();
    let wrapped = kek.encrypt(key().as_bytes()).unwrap();
    let bytes = wrapped.to_bytes();
    assert_eq!(&bytes[..4], &[0, 0, 0, 2]);
    assert_eq!(Ciphertext::parse(&bytes).unwrap(), wrapped);
}

#[test]
fn a_rotated_kek_refuses_what_the_old_one_wrapped() {
    let master = master("tenant-1", &key());
    let kek = master.generate_kek(Algorithm::Aes256Gcm).unwrap();
    let dek = key();
    let wrapped = kek.encrypt(dek.as_bytes()).unwrap();
    let rotated = master.rotate_kek(&kek).unwrap();
    assert_eq!(kek.decrypt(&wrapped).unwrap(), dek.as_bytes());
    assert!(matches!(
        rotated.decrypt(&wrapped),
        Err(Error::WrongKeyVersion {
            expected: 2,
            found: 1
        })
    ));
}

#[test]
fn keks_of_two_tenants_do_not_open_each_other_s() {
    let key = key();
    let kek1 = master("tenant-1", &key)
        .generate_kek(Algorithm::Aes256Gcm)
        .unwrap();
    let kek2 = master("t2", &key)
        .generate_kek(Algorithm::Aes256Gcm)
        .unwrap();
    let wrapped = kek1.encrypt(b"a dek").unwrap();
    assert!(matches!(kek2.decrypt(&wrapped), Err(Error::Decrypt)));
}

// The edges.

#[test]
fn a_master_key_of_the_wrong_length_is_refused() {
    assert!(matches!(
        MasterKey::new("t1", &Key::new(vec![0; 16]), Algorithm::Aes256Gcm),
        Err(Error::Malformed(_))
    ));
}

#[test]
fn bytes_too_short_to_name_a_version_are_refused() {
    assert!(matches!(
        Ciphertext::parse(&[0, 0, 7]),
        Err(Error::Malformed(_))
    ));
    assert_eq!(
        Ciphertext::parse(&[0, 0, 0, 7]).unwrap(),
        Ciphertext::new(7, vec![])
    );
}

/// Made by the Python port's `MasterKey` and `Kek` with the master key
/// `00 01 … 1f` for `tenant-1`: the first KEK, its rotation, and one DEK,
/// `40 41 … 5f`, wrapped under each.
mod wrapped_by_the_python_port {
    use super::*;

    const KEK_V1: &str = "00000001a81366893e77f9851f6b13b9f2e4181704bcc43283f37b81741bc0fd0137c70d02cf28faaa22ac0a7b228367770147acc3f14d68debe95405cac4fc7";
    const KEK_V2: &str = "000000010e09c2809b2663cdd8af35a7b9d86e4303611a2067f005639a47b3a44aa5e985ef4a3cadca09a598b91648fb4fe440760e0c84d7f4a8cfc151eed8af";
    const DEK_UNDER_V1: &str = "00000001b3d9d1393913407b952682e4e96b8c8c21d18b59211e65daf7a6060c07a8d24fed1899f548955ca16f07d75ce27ba7d225f03e83ae932f8c348a3628";
    const DEK_UNDER_V2: &str = "000000026a4bace0f5c2c7047cd2041010aab91c913a5a389c0158660ac23518007f2e612c8bb3a84209ee4f2f1f2eb58c04acb27fb358f6cc54fd16e82da757";

    fn python_master() -> MasterKey {
        master("tenant-1", &Key::new((0..32).collect::<Vec<u8>>()))
    }

    #[test]
    fn its_keks_load_here_and_unwrap_its_deks() {
        let master = python_master();
        let dek: Vec<u8> = (0x40..0x60).collect();
        for (version, kek, wrapped) in [(1, KEK_V1, DEK_UNDER_V1), (2, KEK_V2, DEK_UNDER_V2)] {
            let kek = master
                .load_kek(
                    Ciphertext::parse(&hex(kek)).unwrap(),
                    version,
                    Algorithm::Aes256Gcm,
                )
                .unwrap();
            let wrapped = Ciphertext::parse(&hex(wrapped)).unwrap();
            assert_eq!(wrapped.key_version(), version);
            assert_eq!(kek.decrypt(&wrapped).unwrap(), dek, "version {version}");
        }
    }

    #[test]
    fn its_second_kek_refuses_what_its_first_wrapped() {
        let kek2 = python_master()
            .load_kek(
                Ciphertext::parse(&hex(KEK_V2)).unwrap(),
                2,
                Algorithm::Aes256Gcm,
            )
            .unwrap();
        assert!(matches!(
            kek2.decrypt(&Ciphertext::parse(&hex(DEK_UNDER_V1)).unwrap()),
            Err(Error::WrongKeyVersion {
                expected: 2,
                found: 1
            })
        ));
    }
}
