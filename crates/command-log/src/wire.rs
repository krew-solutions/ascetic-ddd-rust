//! How the record types meet JSON (ADR-0046 decision 6): integers that can pass
//! 2⁵³ as decimal strings, so that no reader rounds them; bytes as standard
//! padded base64. Public for the streams sharing the log's framing
//! ([`frame`](crate::frame)): the scheduler's journal writes its numbers alike.
//!
//! Each text is read where the deserializer lends it — the line's bytes, or
//! serde's scratch for an escaped string — and checked canonical in place: no
//! copy of it is held (ADR-0047 decision 22, K2b-7e-2b-2), so a line decodes
//! holding nothing but its value.

use std::fmt;

use serde::Deserializer;
use serde::de::{self, Visitor};

/// The value `f` makes of a string `d` lends, never copied.
fn text<'de, D, T, F>(d: D, f: F) -> Result<T, D::Error>
where
    D: Deserializer<'de>,
    F: FnOnce(&str) -> Result<T, String>,
{
    d.deserialize_str(Text(f))
}

/// [`text`], or none for `null`.
fn text_option<'de, D, T, F>(d: D, f: F) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    F: FnOnce(&str) -> Result<T, String>,
{
    d.deserialize_option(TextOption(f))
}

struct Text<F>(F);

impl<'de, T, F: FnOnce(&str) -> Result<T, String>> Visitor<'de> for Text<F> {
    type Value = T;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a string")
    }

    fn visit_str<E: de::Error>(self, v: &str) -> Result<T, E> {
        (self.0)(v).map_err(E::custom)
    }
}

struct TextOption<F>(F);

impl<'de, T, F: FnOnce(&str) -> Result<T, String>> Visitor<'de> for TextOption<F> {
    type Value = Option<T>;

    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a string or null")
    }

    fn visit_none<E: de::Error>(self) -> Result<Option<T>, E> {
        Ok(None)
    }

    fn visit_unit<E: de::Error>(self) -> Result<Option<T>, E> {
        Ok(None)
    }

    fn visit_some<D: Deserializer<'de>>(self, d: D) -> Result<Option<T>, D::Error> {
        text(d, self.0).map(Some)
    }
}

/// A `u64` as a decimal string.
pub mod u64s {
    use serde::{Deserializer, Serializer};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        super::text(d, parse)
    }

    pub(super) fn parse(text: &str) -> Result<u64, String> {
        canonical_u64(text).ok_or_else(|| format!("not a canonical u64: {text:?}"))
    }

    /// Decimal digits, no sign, no leading zero but for zero itself: one text a
    /// value, so a re-encoding is the same bytes.
    pub fn canonical_u64(text: &str) -> Option<u64> {
        let digits = text.bytes().all(|b| b.is_ascii_digit());
        let leading_zero = text.len() > 1 && text.starts_with('0');
        if text.is_empty() || !digits || leading_zero {
            return None;
        }
        text.parse().ok()
    }
}

/// An optional `u64` as a decimal string or `null`.
pub mod u64s_option {
    use serde::{Deserializer, Serializer};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(v) => s.collect_str(v),
            None => s.serialize_none(),
        }
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        super::text_option(d, super::u64s::parse)
    }
}

/// A list of `u64`s, each a decimal string.
pub mod u64s_vec {
    use serde::ser::SerializeSeq;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &[u64], s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(v.len()))?;
        for x in v {
            seq.serialize_element(&x.to_string())?;
        }
        seq.end()
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u64>, D::Error> {
        Vec::<String>::deserialize(d)?
            .iter()
            .map(|text| {
                super::u64s::canonical_u64(text)
                    .ok_or_else(|| D::Error::custom(format!("not a canonical u64: {text:?}")))
            })
            .collect()
    }
}

/// An `i64` as a decimal string.
pub mod i64s {
    use serde::{Deserializer, Serializer};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        super::text(d, parse)
    }

    /// The canonical text is the one `i64` writes: no `+`, no leading zero,
    /// no `-0`.
    pub(super) fn parse(text: &str) -> Result<i64, String> {
        let value = match text.strip_prefix('-') {
            Some("0") => None,
            Some(digits) => super::u64s::canonical_u64(digits)
                .and_then(|u| i128::from(u).checked_neg())
                .and_then(|n| i64::try_from(n).ok()),
            None => super::u64s::canonical_u64(text).and_then(|u| i64::try_from(u).ok()),
        };
        value.ok_or_else(|| format!("not a canonical i64: {text:?}"))
    }
}

/// An optional `i64` as a decimal string or `null`.
pub mod i64s_option {
    use serde::{Deserializer, Serializer};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &Option<i64>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(v) => s.collect_str(v),
            None => s.serialize_none(),
        }
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<i64>, D::Error> {
        super::text_option(d, super::i64s::parse)
    }
}

/// Bytes as standard padded base64.
pub mod b64 {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserializer, Serializer};

    /// Writes it, encoded as it is written: no copy of its text.
    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(&base64::display::Base64Display::new(v, &STANDARD))
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        super::text(d, |text| {
            let bytes = STANDARD
                .decode(text)
                .map_err(|e| format!("not base64: {e}"))?;
            // One text a value: the encoding of what was decoded must be the
            // text — compared a group of 768 bytes at a time, encoded on the
            // stack.
            let mut rest = text.as_bytes();
            let mut group = [0u8; 1_024];
            for chunk in bytes.chunks(768) {
                let n = STANDARD
                    .encode_slice(chunk, &mut group)
                    .map_err(|e| format!("not base64: {e}"))?;
                match (group.get(..n), rest.get(..n)) {
                    (Some(a), Some(b)) if a == b => rest = rest.get(n..).unwrap_or(&[]),
                    _ => return Err("not canonical base64".to_owned()),
                }
            }
            if rest.is_empty() {
                Ok(bytes)
            } else {
                Err("not canonical base64".to_owned())
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    #[derive(Debug, Deserialize)]
    struct Fields {
        #[serde(with = "super::u64s")]
        u: u64,
        #[serde(with = "super::i64s")]
        i: i64,
    }

    fn read(u: &str, i: &str) -> Option<(u64, i64)> {
        serde_json::from_str::<Fields>(&format!(r#"{{"u":"{u}","i":"{i}"}}"#))
            .ok()
            .map(|f| (f.u, f.i))
    }

    /// A field read alone, without the reader's re-encoding check — as a host
    /// decoding one line would — accepts only its canonical text.
    #[test]
    fn a_number_field_takes_only_its_canonical_text() {
        assert_eq!(read("0", "0"), Some((0, 0)));
        assert_eq!(
            read("18446744073709551615", "-9223372036854775808"),
            Some((u64::MAX, i64::MIN))
        );
        for u in [
            "",
            "00",
            "01",
            "+1",
            "-1",
            " 1",
            "1 ",
            "1e2",
            "18446744073709551616",
        ] {
            assert_eq!(read(u, "0"), None, "u64 {u:?}");
        }
        for i in ["", "-0", "+1", "01", "-01", "1.0", "9223372036854775808"] {
            assert_eq!(read("0", i), None, "i64 {i:?}");
        }
    }
}
