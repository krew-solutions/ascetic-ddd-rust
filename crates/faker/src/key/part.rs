//! The canonical bytes of a scope (ADR-0045 decision 2).

use uuid::Uuid;

use super::{Out, leb128};

/// A value that can name a part of a key: a Tenant, a pair of owners, a
/// region.
///
/// The encoding must be **prefix-free** within the type: no value's bytes may
/// begin another's. Every method of [`Encoder`] writes a prefix-free
/// encoding — integers at a fixed width, byte strings after their length — and
/// a sequence of prefix-free encodings, decided by the type, is again
/// prefix-free. So an implementation that writes its fields in order, and an
/// enum that writes its variant's number first, is injective: two scopes are
/// one key only if they are one value.
///
/// `Hash` and `Debug` are never used: the first depends on the platform and
/// the compiler, the second changes with a rename.
///
/// ```
/// use ascetic_ddd_faker::{Encoder, KeyPart};
///
/// enum Region { Eu, Us(u16) }
///
/// impl KeyPart for Region {
///     fn encode(&self, out: &mut Encoder<'_>) {
///         match self {
///             Region::Eu => out.u8(0),
///             Region::Us(zone) => { out.u8(1); out.u16(*zone); }
///         }
///     }
/// }
/// ```
pub trait KeyPart {
    /// Writes the value's canonical bytes.
    fn encode(&self, out: &mut Encoder<'_>);
}

/// Writes the canonical bytes of a [`KeyPart`].
pub struct Encoder<'a> {
    out: &'a mut dyn Out,
}

impl<'a> Encoder<'a> {
    pub(crate) fn new(out: &'a mut dyn Out) -> Self {
        Encoder { out }
    }

    /// One byte.
    pub fn u8(&mut self, v: u8) {
        self.out.write(&[v]);
    }

    /// Two bytes, little-endian.
    pub fn u16(&mut self, v: u16) {
        self.out.write(&v.to_le_bytes());
    }

    /// Four bytes, little-endian.
    pub fn u32(&mut self, v: u32) {
        self.out.write(&v.to_le_bytes());
    }

    /// Eight bytes, little-endian.
    pub fn u64(&mut self, v: u64) {
        self.out.write(&v.to_le_bytes());
    }

    /// Sixteen bytes, little-endian.
    pub fn u128(&mut self, v: u128) {
        self.out.write(&v.to_le_bytes());
    }

    /// Eight bytes, little-endian two's complement.
    pub fn i64(&mut self, v: i64) {
        self.out.write(&v.to_le_bytes());
    }

    /// One byte: 0 or 1.
    pub fn bool(&mut self, v: bool) {
        self.u8(u8::from(v));
    }

    /// A byte string: its length in LEB128, then its bytes.
    pub fn bytes(&mut self, v: &[u8]) {
        self.out.write(leb128(v.len() as u64).as_slice());
        self.out.write(v);
    }

    /// A string: its UTF-8 bytes, as [`bytes`](Self::bytes).
    pub fn str(&mut self, v: &str) {
        self.bytes(v.as_bytes());
    }

    /// A fixed number of bytes, as they are: prefix-free because every value
    /// of the type has the same length.
    pub fn array<const N: usize>(&mut self, v: &[u8; N]) {
        self.out.write(v);
    }

    /// The number of items of a sequence, before them.
    pub fn len(&mut self, n: usize) {
        self.out.write(leb128(n as u64).as_slice());
    }
}

macro_rules! fixed_width {
    ($($t:ty => $method:ident),* $(,)?) => {
        $(impl KeyPart for $t {
            fn encode(&self, out: &mut Encoder<'_>) {
                out.$method(*self);
            }
        })*
    };
}

fixed_width!(u8 => u8, u16 => u16, u32 => u32, u64 => u64, u128 => u128, i64 => i64, bool => bool);

/// A platform-scoped population's scope: no bytes.
impl KeyPart for () {
    fn encode(&self, _out: &mut Encoder<'_>) {}
}

impl KeyPart for str {
    fn encode(&self, out: &mut Encoder<'_>) {
        out.str(self);
    }
}

impl KeyPart for String {
    fn encode(&self, out: &mut Encoder<'_>) {
        out.str(self);
    }
}

impl KeyPart for Uuid {
    fn encode(&self, out: &mut Encoder<'_>) {
        out.array(self.as_bytes());
    }
}

impl<T: KeyPart + ?Sized> KeyPart for &T {
    fn encode(&self, out: &mut Encoder<'_>) {
        (**self).encode(out);
    }
}

/// `None` is 0; `Some(v)` is 1, then `v`. So `None` is never `Some("None")`.
impl<T: KeyPart> KeyPart for Option<T> {
    fn encode(&self, out: &mut Encoder<'_>) {
        match self {
            None => out.u8(0),
            Some(v) => {
                out.u8(1);
                v.encode(out);
            }
        }
    }
}

/// The number of items, then each.
impl<T: KeyPart> KeyPart for [T] {
    fn encode(&self, out: &mut Encoder<'_>) {
        out.len(self.len());
        self.iter().for_each(|item| item.encode(out));
    }
}

impl<T: KeyPart> KeyPart for Vec<T> {
    fn encode(&self, out: &mut Encoder<'_>) {
        self.as_slice().encode(out);
    }
}

macro_rules! tuple {
    ($($name:ident),+) => {
        /// Each component in order: prefix-free, because each is.
        impl<$($name: KeyPart),+> KeyPart for ($($name,)+) {
            #[allow(non_snake_case)]
            fn encode(&self, out: &mut Encoder<'_>) {
                let ($($name,)+) = self;
                $($name.encode(out);)+
            }
        }
    };
}

tuple!(A);
tuple!(A, B);
tuple!(A, B, C);
tuple!(A, B, C, D);
