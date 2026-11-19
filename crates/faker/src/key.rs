//! The key: the identity a value is drawn from (ADR-0045 decision 2).
//!
//! A key is a path, seed → kind → scope → index → label, and every step is a
//! derivation of a new key from its parent:
//!
//! ```text
//! child = SipHash-2-4-128(key = parent, message = tag ‖ bytes of the step)
//! ```
//!
//! The tag keeps the kinds of step apart: a label, an index, a scope, a
//! derivation internal to the crate, a rendered id. A label is preceded by its
//! length in LEB128, and a scope is written by its [`KeyPart`] encoding, which
//! is prefix-free; so two different paths give two different messages at every
//! step.
//!
//! # A key is drawn from once
//!
//! `Key` is neither `Copy` nor `Clone`; the draws take it by value. A key that
//! has been drawn from cannot be handed on:
//!
//! ```compile_fail,E0382
//! use ascetic_ddd_faker::{Key, Seed};
//!
//! let key = Key::root(Seed(1)).child("present");
//! let coin = key.unit();
//! let value = key.child("value"); // the coin's key, used again
//! ```
//!
//! Its twin derives first and draws last, and compiles:
//!
//! ```
//! use ascetic_ddd_faker::{Key, Seed};
//!
//! let key = Key::root(Seed(1)).child("present");
//! let value = key.child("value");
//! let coin = key.unit();
//! # let _ = (value, coin);
//! ```

use std::hash::Hasher;

use siphasher::sip128::{Hasher128, SipHasher24};
use uuid::Uuid;

mod part;
#[cfg(test)]
mod tests;

pub use self::part::{Encoder, KeyPart};

/// Domain tags: the first byte of every derivation's message.
mod tag {
    pub(super) const ROOT: u8 = 0x00;
    pub(super) const LABEL: u8 = 0x01;
    pub(super) const INDEX: u8 = 0x02;
    pub(super) const PART: u8 = 0x03;
    pub(super) const INTERNAL: u8 = 0x04;
    pub(super) const ID: u8 = 0x05;
}

/// The seed of a Plan: the root of every key.
///
/// Changing it changes every generated value. Two runs that must offer the
/// same load share it; an experiment varies a law, not the seed.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seed(pub u64);

/// The namespace ids are rendered under.
///
/// Kept apart from the [`Seed`]: two runs of one Plan on one stand use two
/// namespaces, so that their ids are fresh while every attribute stays the
/// same.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Namespace(pub u64);

/// The identity a value is drawn from.
///
/// See the [module documentation](self).
#[derive(Debug, PartialEq, Eq)]
pub struct Key {
    k0: u64,
    k1: u64,
}

impl Key {
    /// The root of a Plan's keys.
    pub fn root(seed: Seed) -> Key {
        derive(0, 0, tag::ROOT, |out| out.write(&seed.0.to_le_bytes()))
    }

    /// The key of a labelled part of this one: a field, a step.
    ///
    /// Two labels give two keys, whatever their lengths: the label is preceded
    /// by its length.
    pub fn child(&self, label: &str) -> Key {
        self.labelled(tag::LABEL, label)
    }

    /// The key of the `index`th of a sequence under this one: a member, an
    /// item.
    pub fn at(&self, index: u64) -> Key {
        derive(self.k0, self.k1, tag::INDEX, |out| {
            out.write(&index.to_le_bytes())
        })
    }

    /// The key of a typed scope under this one: a Tenant, a pair of owners.
    ///
    /// The scope is written by its [`KeyPart`] encoding, never by `Hash` or
    /// `Debug`, which are not stable.
    pub fn part<P: KeyPart + ?Sized>(&self, part: &P) -> Key {
        derive(self.k0, self.k1, tag::PART, |out| {
            part.encode(&mut Encoder::new(out))
        })
    }

    /// A key the crate derives for itself — a permutation's rounds, a coin, a
    /// count — under a tag a host's label cannot reach.
    pub(crate) fn internal(&self, label: &'static str) -> Key {
        self.labelled(tag::INTERNAL, label)
    }

    /// 64 bits drawn from this key.
    pub fn bits(self) -> u64 {
        self.k0
    }

    /// A uniform number drawn from this key, strictly inside (0, 1).
    pub fn unit(self) -> Unit {
        Unit(self.k0 >> (u64::BITS - Unit::BITS))
    }

    /// An id rendered from this key under `namespace`: a version 4 UUID whose
    /// random bits are drawn from the key.
    pub fn uuid(self, namespace: &Namespace) -> Uuid {
        let rendered = derive(self.k0, self.k1, tag::ID, |out| {
            out.write(&namespace.0.to_le_bytes())
        });
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&rendered.k0.to_le_bytes());
        bytes[8..].copy_from_slice(&rendered.k1.to_le_bytes());
        uuid::Builder::from_random_bytes(bytes).into_uuid()
    }

    fn labelled(&self, tag: u8, label: &str) -> Key {
        derive(self.k0, self.k1, tag, |out| {
            out.write(leb128(label.len() as u64).as_slice());
            out.write(label.as_bytes());
        })
    }
}

/// A uniform number strictly inside (0, 1): `(k + ½) / 2⁵²` for a 52-bit `k`.
///
/// Its extremes are 2⁻⁵³ and 1 − 2⁻⁵³, both exact in an `f64`, so a quantile
/// is never asked for 0 or 1. With 53 bits the largest `k` would round to 1.0,
/// and a heavy tail's quantile there is infinite.
///
/// The integer `k` is kept: a discrete law compares it with integer
/// thresholds, so its choice does not depend on floating point at all.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Unit(u64);

impl Unit {
    /// The bits of `k`.
    pub const BITS: u32 = 52;

    /// The largest `k`.
    pub const MAX_RAW: u64 = (1 << Unit::BITS) - 1;

    /// 2⁻⁵², exact.
    const SCALE: f64 = 1.0 / (1u64 << Unit::BITS) as f64;

    /// The unit of a given `k`, or `None` if `k` has more than 52 bits.
    ///
    /// For tests that need a law at its boundaries; generation draws units
    /// from keys.
    pub fn from_raw(raw: u64) -> Option<Unit> {
        (raw <= Unit::MAX_RAW).then_some(Unit(raw))
    }

    /// The integer `k`.
    pub fn raw(self) -> u64 {
        self.0
    }

    /// The number `(k + ½) / 2⁵²`.
    pub fn value(self) -> f64 {
        // Exact: `k` has 52 bits, `k + ½` has 53, and the scale is a power
        // of two.
        (self.0 as f64 + 0.5) * Unit::SCALE
    }

    /// `1 − value()`: `(2⁵² − 1 − k + ½) / 2⁵²`. Exact, as `1.0 - value()` is
    /// too — a unit is an odd multiple of 2⁻⁵³ — but written as what a quantile
    /// of the upper tail means.
    pub fn complement(self) -> f64 {
        (Unit::MAX_RAW.saturating_sub(self.0) as f64 + 0.5) * Unit::SCALE // k ≤ MAX_RAW
    }
}

/// Where a derivation writes its message: a hasher, or bytes for a test.
pub(crate) trait Out {
    fn write(&mut self, bytes: &[u8]);
}

impl Out for SipHasher24 {
    fn write(&mut self, bytes: &[u8]) {
        Hasher::write(self, bytes);
    }
}

impl Out for Vec<u8> {
    fn write(&mut self, bytes: &[u8]) {
        self.extend_from_slice(bytes);
    }
}

fn derive(k0: u64, k1: u64, tag: u8, message: impl FnOnce(&mut dyn Out)) -> Key {
    let mut hasher = SipHasher24::new_with_keys(k0, k1);
    Out::write(&mut hasher, &[tag]);
    message(&mut hasher);
    let hash = hasher.finish128();
    Key {
        k0: hash.h1,
        k1: hash.h2,
    }
}

/// An unsigned integer in LEB128: seven bits a byte, the high bit set on every
/// byte but the last. Prefix-free, and one byte for anything under 128.
pub(crate) fn leb128(mut n: u64) -> Leb128 {
    let mut bytes = [0u8; 10];
    let mut len: usize = 0;
    for byte in bytes.iter_mut() {
        let low = (n & 0x7f) as u8;
        n >>= 7;
        len = len.saturating_add(1); // at most 10: no overflow, and no lint
        if n == 0 {
            *byte = low;
            break;
        }
        *byte = low | 0x80;
    }
    Leb128 { bytes, len }
}

/// The bytes of [`leb128`].
pub(crate) struct Leb128 {
    bytes: [u8; 10],
    len: usize,
}

impl Leb128 {
    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes[..self.len]
    }
}
