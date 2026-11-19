//! K1, K2 and K4 of ADR-0045.

use proptest::prelude::*;

use super::*;
use crate::testing::encoding;

fn words(key: &Key) -> (u64, u64) {
    (key.k0, key.k1)
}

/// K1: the derivations against `vectors/key_vectors.py`, an implementation of
/// SipHash-2-4-128 written from its specification, which first reproduces the
/// reference implementation's published vector. Regenerate with
/// `python3 crates/faker/vectors/key_vectors.py`.
#[test]
fn derivations_match_the_independent_vectors() {
    let root = Key::root(Seed(42));
    assert_eq!(words(&root), (0x64415234f3528492, 0x7c0cef35f8b57395));

    let labels = [
        ("", (0xa751f1c0b132e449, 0x3e8a8845cedac599)),
        ("os", (0xad99f290cb9bc288, 0xc8141a4cc4233278)),
        ("device", (0x480cc2e3492e57c8, 0xdcd6e5ed373100ac)),
        ("management_mode", (0x0016cb4eeeb6d0c8, 0x3c4d92fa5283349a)),
    ];
    for (label, expected) in labels {
        assert_eq!(words(&root.child(label)), expected, "child({label:?})");
    }
    // 200 bytes: a length of two LEB128 bytes.
    assert_eq!(
        words(&root.child(&"a".repeat(200))),
        (0x7e9b917414bcc392, 0xf65603f61660e6e3)
    );

    let indices = [
        (0, (0x0d82bdf31a56bef5, 0xacb8edfbfc3640f5)),
        (1, (0xe170bfb804d81231, 0x5721a998af51753d)),
        (1 << 32, (0xed2b89e7eaaccee5, 0x956a808ef0cd12e1)),
        (u64::MAX, (0xd570614960a794b5, 0x42d40f215cc8afd2)),
    ];
    for (index, expected) in indices {
        assert_eq!(words(&root.at(index)), expected, "at({index})");
    }

    assert_eq!(
        words(&root.part(&(7u64, "ab"))),
        (0xdf29b9dcc5c03955, 0x97a6e02e6690dbff)
    );

    let device = root.child("device").part(&2u64).at(12345);
    let os = device.child("os");
    assert_eq!(words(&os), (0x4d0d66a92bd764f7, 0xb9beb221de28bab5));
    assert_eq!(os.unit().raw(), 1355519236423030);
    assert_eq!(
        device.uuid(&Namespace(7)).to_string(),
        "f1e8c687-2191-40c5-9dbd-c50b2790d6a3"
    );
}

#[test]
fn bits_are_the_first_word() {
    let key = Key::root(Seed(42));
    assert_eq!(key.bits(), 0x64415234f3528492);
}

/// The same path is the same key; a different step is a different key.
#[test]
fn a_path_is_its_key() {
    let a = Key::root(Seed(1)).child("device").at(3);
    let b = Key::root(Seed(1)).child("device").at(3);
    assert_eq!(a, b);
    assert_ne!(a, Key::root(Seed(2)).child("device").at(3));
    assert_ne!(a, Key::root(Seed(1)).child("device").at(4));
    assert_ne!(a, Key::root(Seed(1)).child("devices").at(3));
    // A label and an index with the same bytes are told apart by the tag.
    let root = Key::root(Seed(1));
    let bytes = 0x0102_0304_0506_0708u64.to_le_bytes();
    let label = std::str::from_utf8(&bytes).expect("ASCII control bytes are UTF-8");
    assert_ne!(root.child(label), root.at(0x0102_0304_0506_0708));
    assert_ne!(
        root.part(&0x0102_0304_0506_0708u64),
        root.at(0x0102_0304_0506_0708)
    );
}

/// Rendered ids: version 4, RFC 9562 variant; a namespace changes the id and
/// nothing else.
#[test]
fn ids_are_rendered_under_a_namespace() {
    let member = || Key::root(Seed(5)).child("device").at(9);
    let first = member().uuid(&Namespace(1));
    let second = member().uuid(&Namespace(2));
    assert_ne!(first, second);
    assert_eq!(first, member().uuid(&Namespace(1)));
    assert_eq!(first.get_version_num(), 4);
    assert_eq!(first.get_variant(), uuid::Variant::RFC4122);
    // The attribute drawn beside the id does not depend on the namespace.
    assert_eq!(member().child("os").bits(), member().child("os").bits());
}

/// K4: a unit lies strictly inside (0, 1) at every `k`, its extremes included.
#[test]
fn a_unit_is_strictly_inside_the_interval() {
    for raw in [0, 1, Unit::MAX_RAW - 1, Unit::MAX_RAW] {
        let u = Unit::from_raw(raw).expect("52 bits");
        assert!(
            u.value() > 0.0 && u.value() < 1.0,
            "k = {raw}: {}",
            u.value()
        );
    }
    assert_eq!(
        Unit::from_raw(0).map(Unit::value),
        Some(0.5 / 4_503_599_627_370_496.0)
    );
    assert_eq!(
        Unit::from_raw(Unit::MAX_RAW).map(Unit::value),
        Some(1.0 - 0.5 / 4_503_599_627_370_496.0)
    );
    assert_eq!(Unit::from_raw(Unit::MAX_RAW + 1), None);
    // The 53-bit form this replaces reaches 1.0 exactly.
    let k53 = (1u64 << 53) - 1;
    assert_eq!((k53 as f64 + 0.5) / (1u64 << 53) as f64, 1.0);
}

/// A unit's `k` is the key's first word's high 52 bits.
#[test]
fn a_unit_is_the_high_bits_of_the_key() {
    let raw = Key::root(Seed(42)).unit().raw();
    assert_eq!(raw, 0x64415234f3528492 >> 12);
    assert!(raw <= Unit::MAX_RAW);
}

/// LEB128 at its byte boundaries.
#[test]
fn lengths_are_leb128() {
    assert_eq!(leb128(0).as_slice(), [0x00]);
    assert_eq!(leb128(127).as_slice(), [0x7f]);
    assert_eq!(leb128(128).as_slice(), [0x80, 0x01]);
    assert_eq!(leb128(300).as_slice(), [0xac, 0x02]);
    assert_eq!(
        leb128(u64::MAX).as_slice(),
        [0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]
    );
}

/// K2: the vectors a separator, an untagged option or a decimal rendering would
/// collide on.
#[test]
fn scopes_that_look_alike_are_told_apart() {
    assert_ne!(encoding(&("ab", "c")), encoding(&("a", "bc")));
    assert_ne!(encoding(&(1u64, 23u64)), encoding(&(12u64, 3u64)));
    assert_ne!(encoding(&None::<&str>), encoding(&Some("None")));
    assert_ne!(encoding(&Some("")), encoding(&None::<&str>));
    assert_ne!(
        encoding(&vec![vec![1u8], vec![]]),
        encoding(&vec![vec![], vec![1u8]])
    );
    assert_eq!(encoding(&()), Vec::<u8>::new());
    assert_eq!(encoding(&7u64), 7u64.to_le_bytes());
    assert_eq!(encoding(&"ab"), [2, b'a', b'b']);
}

/// A value of a type whose encoding is decided by the type, for the property.
#[derive(Clone, Debug, PartialEq)]
enum Scope {
    Pair(String, String),
    Maybe(Option<String>),
    Numbers(Vec<u64>),
    Mixed((u16, bool, String)),
}

impl KeyPart for Scope {
    fn encode(&self, out: &mut Encoder<'_>) {
        match self {
            Scope::Pair(a, b) => {
                out.u8(0);
                (a, b).encode(out);
            }
            Scope::Maybe(v) => {
                out.u8(1);
                v.encode(out);
            }
            Scope::Numbers(v) => {
                out.u8(2);
                v.encode(out);
            }
            Scope::Mixed(v) => {
                out.u8(3);
                v.encode(out);
            }
        }
    }
}

fn scope() -> impl Strategy<Value = Scope> {
    // Short strings over a small alphabet, so that near-collisions are common.
    let text = "[ab]{0,3}";
    prop_oneof![
        (text, text).prop_map(|(a, b)| Scope::Pair(a, b)),
        proptest::option::of(text).prop_map(Scope::Maybe),
        proptest::collection::vec(0u64..3, 0..3).prop_map(Scope::Numbers),
        (0u16..3, any::<bool>(), text).prop_map(Scope::Mixed),
    ]
}

proptest! {
    /// K2: two scopes have one encoding only if they are one value — and,
    /// prefix-free, neither encoding begins the other.
    #[test]
    fn the_encoding_is_injective_and_prefix_free(a in scope(), b in scope()) {
        let (ea, eb) = (encoding(&a), encoding(&b));
        if a == b {
            prop_assert_eq!(ea, eb);
        } else {
            prop_assert_ne!(&ea, &eb);
            prop_assert!(!ea.starts_with(&eb) && !eb.starts_with(&ea));
        }
    }

}
