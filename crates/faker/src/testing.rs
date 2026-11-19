//! Helpers for the crate's tests and for a host's.

use crate::key::{Encoder, KeyPart};

/// The canonical bytes a scope is written as: for a host's test that its own
/// [`KeyPart`] is injective, as the crate's are.
///
/// ```
/// use ascetic_ddd_faker::testing::encoding;
///
/// assert_ne!(encoding(&("ab", "c")), encoding(&("a", "bc")));
/// assert_ne!(encoding(&None::<&str>), encoding(&Some("None")));
/// ```
pub fn encoding<P: KeyPart + ?Sized>(part: &P) -> Vec<u8> {
    let mut bytes = Vec::new();
    part.encode(&mut Encoder::new(&mut bytes));
    bytes
}
