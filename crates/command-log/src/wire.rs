//! How the record types meet JSON (ADR-0046 decision 6): integers that can pass
//! 2⁵³ as decimal strings, so that no reader rounds them; bytes as standard
//! padded base64. Public for the streams sharing the log's framing
//! ([`frame`](crate::frame)): the scheduler's journal writes its numbers alike.

/// A `u64` as a decimal string.
pub mod u64s {
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        let text = String::deserialize(d)?;
        canonical_u64(&text)
            .ok_or_else(|| D::Error::custom(format!("not a canonical u64: {text:?}")))
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
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &Option<u64>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(v) => s.collect_str(v),
            None => s.serialize_none(),
        }
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
        match Option::<String>::deserialize(d)? {
            None => Ok(None),
            Some(text) => super::u64s::canonical_u64(&text)
                .map(Some)
                .ok_or_else(|| D::Error::custom(format!("not a canonical u64: {text:?}"))),
        }
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
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &i64, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(v)
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
        let text = String::deserialize(d)?;
        let parsed: Option<i64> = text.parse().ok();
        // The canonical text is the one `i64` writes: no `+`, no leading zero, no `-0`.
        match parsed {
            Some(v) if v.to_string() == text => Ok(v),
            _ => Err(D::Error::custom(format!("not a canonical i64: {text:?}"))),
        }
    }
}

/// Bytes as standard padded base64.
pub mod b64 {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    use serde::{Deserialize, Deserializer, Serializer, de::Error};

    /// Writes it.
    pub fn serialize<S: Serializer>(v: &[u8], s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&STANDARD.encode(v))
    }

    /// Reads it, only in its canonical text.
    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        let text = String::deserialize(d)?;
        let bytes = STANDARD
            .decode(&text)
            .map_err(|e| D::Error::custom(format!("not base64: {e}")))?;
        // One text a value: the encoding of what was decoded must be the text.
        if STANDARD.encode(&bytes) == text {
            Ok(bytes)
        } else {
            Err(D::Error::custom("not canonical base64"))
        }
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
