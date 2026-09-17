//! The model, without a service: what the Python `test_models` and Go
//! `models_test` check, plus what the Python port wrapped.

use ascetic_ddd_kms::{Algorithm, Error, Key, MASTER_KEY_VERSION, MasterKey, WrappedKey};

fn key() -> Key {
    Algorithm::Aes256Gcm.generate_key().unwrap()
}

fn master(key: &Key) -> MasterKey {
    MasterKey::new(key.clone(), Algorithm::Aes256Gcm).unwrap()
}

fn hex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).unwrap())
        .collect()
}

// The master key wraps for a tenant.

#[test]
fn a_wrapped_key_unwraps_under_the_key_that_wrapped_it() {
    let master = master(&key());
    let dek = key();
    let wrapped = master.wrap("t1", &dek).unwrap();
    assert_eq!(wrapped.key_version(), MASTER_KEY_VERSION);
    assert_eq!(master.unwrap("t1", &wrapped).unwrap(), dek);
}

#[test]
fn wrapping_twice_gives_two_wrapped_forms() {
    let master = master(&key());
    let dek = key();
    assert_ne!(
        master.wrap("t1", &dek).unwrap(),
        master.wrap("t1", &dek).unwrap()
    );
}

#[test]
fn another_tenant_does_not_unwrap_it() {
    let master = master(&key());
    let wrapped = master.wrap("t1", &key()).unwrap();
    assert!(matches!(master.unwrap("t2", &wrapped), Err(Error::Decrypt)));
}

// The master key over KEKs.

#[test]
fn the_first_kek_is_version_one() {
    let master = master(&key());
    let kek = master.generate_kek("tenant-1").unwrap();
    assert_eq!(kek.tenant_id(), "tenant-1");
    assert_eq!(kek.version(), 1);
    assert_eq!(kek.algorithm(), master.algorithm());
    assert_eq!(kek.wrapped().key_version(), MASTER_KEY_VERSION);
    assert_eq!(
        master
            .unwrap("tenant-1", kek.wrapped())
            .unwrap()
            .as_bytes()
            .len(),
        32
    );
}

#[test]
fn a_kek_loads_back_from_its_wrapped_form() {
    let master = master(&key());
    let kek = master.generate_kek("tenant-1").unwrap();
    let dek = key();
    let wrapped = kek.wrap(&dek).unwrap();

    let loaded = master
        .load_kek(
            "tenant-1",
            kek.wrapped().clone(),
            kek.version(),
            kek.algorithm(),
        )
        .unwrap();
    assert_eq!(loaded.tenant_id(), kek.tenant_id());
    assert_eq!(loaded.version(), kek.version());
    assert_eq!(loaded.wrapped(), kek.wrapped());
    assert_eq!(loaded.unwrap(&wrapped).unwrap(), dek);
}

#[test]
fn a_kek_does_not_load_under_another_tenant() {
    let master = master(&key());
    let kek = master.generate_kek("t1").unwrap();
    assert!(matches!(
        master.load_kek("t2", kek.wrapped().clone(), 1, Algorithm::Aes256Gcm),
        Err(Error::Decrypt)
    ));
}

#[test]
fn a_kek_does_not_load_under_another_master_key() {
    let kek = master(&key()).generate_kek("t1").unwrap();
    assert!(matches!(
        master(&key()).load_kek("t1", kek.wrapped().clone(), 1, Algorithm::Aes256Gcm),
        Err(Error::Decrypt)
    ));
}

#[test]
fn rotation_makes_the_next_version() {
    let master = master(&key());
    let kek = master.generate_kek("tenant-1").unwrap();
    let rotated = master.rotate_kek(&kek).unwrap();
    assert_eq!(rotated.version(), kek.version() + 1);
    assert_eq!(rotated.tenant_id(), kek.tenant_id());
    assert_eq!(rotated.algorithm(), kek.algorithm());
    assert_ne!(rotated.wrapped(), kek.wrapped());
}

// The KEK over DEKs.

#[test]
fn a_kek_wraps_a_dek_and_unwraps_it() {
    let kek = master(&key()).generate_kek("tenant-1").unwrap();
    let dek = key();
    let wrapped = kek.wrap(&dek).unwrap();
    assert_eq!(kek.unwrap(&wrapped).unwrap(), dek);
}

#[test]
fn a_kek_makes_a_dek_of_its_kind_and_wraps_it() {
    let kek = master(&key()).generate_kek("tenant-1").unwrap();
    let (dek, wrapped) = kek.generate_dek().unwrap();
    assert_eq!(dek.as_bytes().len(), 32);
    assert_eq!(wrapped.key_version(), kek.version());
    assert_eq!(kek.unwrap(&wrapped).unwrap(), dek);
}

#[test]
fn the_wrapped_key_names_the_kek_version_on_the_wire() {
    let master = master(&key());
    let kek = master
        .rotate_kek(&master.generate_kek("tenant-1").unwrap())
        .unwrap();
    let wrapped = kek.wrap(&key()).unwrap();
    let bytes = wrapped.to_bytes();
    assert_eq!(&bytes[..4], &[0, 0, 0, 2]);
    assert_eq!(WrappedKey::parse(&bytes).unwrap(), wrapped);
}

#[test]
fn a_rotated_kek_refuses_what_the_old_one_wrapped_and_rewraps_it() {
    let master = master(&key());
    let kek = master.generate_kek("tenant-1").unwrap();
    let dek = key();
    let wrapped = kek.wrap(&dek).unwrap();
    let rotated = master.rotate_kek(&kek).unwrap();
    assert_eq!(kek.unwrap(&wrapped).unwrap(), dek);
    assert!(matches!(
        rotated.unwrap(&wrapped),
        Err(Error::WrongKeyVersion {
            expected: 2,
            found: 1
        })
    ));
    let rewrapped = rotated.rewrap(&wrapped, &kek).unwrap();
    assert_eq!(rewrapped.key_version(), 2);
    assert_eq!(rotated.unwrap(&rewrapped).unwrap(), dek);
}

#[test]
fn keks_of_two_tenants_do_not_unwrap_each_other_s() {
    let master = master(&key());
    let kek1 = master.generate_kek("tenant-1").unwrap();
    let kek2 = master.generate_kek("t2").unwrap();
    let wrapped = kek1.wrap(&key()).unwrap();
    assert!(matches!(kek2.unwrap(&wrapped), Err(Error::Decrypt)));
}

// The edges.

#[test]
fn a_master_key_of_the_wrong_length_is_refused() {
    assert!(matches!(
        MasterKey::new(Key::new(vec![0; 16]), Algorithm::Aes256Gcm),
        Err(Error::Malformed(_))
    ));
}

#[test]
fn bytes_too_short_to_name_a_version_are_refused() {
    assert!(matches!(
        WrappedKey::parse(&[0, 0, 7]),
        Err(Error::Malformed(_))
    ));
    assert_eq!(
        WrappedKey::parse(&[0, 0, 0, 7]).unwrap(),
        WrappedKey::new(7, vec![])
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
        master(&Key::new((0..32).collect::<Vec<u8>>()))
    }

    fn wrapped(hex_text: &str) -> WrappedKey {
        WrappedKey::parse(&hex(hex_text)).unwrap()
    }

    #[test]
    fn its_keks_load_here_and_unwrap_its_deks() {
        let master = python_master();
        let dek = Key::new((0x40..0x60).collect::<Vec<u8>>());
        for (version, kek, dek_wrapped) in [(1, KEK_V1, DEK_UNDER_V1), (2, KEK_V2, DEK_UNDER_V2)] {
            let kek = master
                .load_kek("tenant-1", wrapped(kek), version, Algorithm::Aes256Gcm)
                .unwrap();
            assert_eq!(wrapped(dek_wrapped).key_version(), version);
            assert_eq!(
                kek.unwrap(&wrapped(dek_wrapped)).unwrap(),
                dek,
                "version {version}"
            );
        }
    }

    #[test]
    fn its_second_kek_refuses_what_its_first_wrapped() {
        let kek2 = python_master()
            .load_kek("tenant-1", wrapped(KEK_V2), 2, Algorithm::Aes256Gcm)
            .unwrap();
        assert!(matches!(
            kek2.unwrap(&wrapped(DEK_UNDER_V1)),
            Err(Error::WrongKeyVersion {
                expected: 2,
                found: 1
            })
        ));
    }
}
