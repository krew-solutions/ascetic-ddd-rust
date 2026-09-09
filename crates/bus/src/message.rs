//! What crosses the wire.

/// A wire message: an opaque payload and, optionally, a key.
///
/// The bus never looks inside the payload. Producers encode their values into
/// it and consumers decode it back, each with its own function, so two
/// consumers of one topic may read the same bytes as different types — the
/// wire format is the contract, the types are local to each side.
///
/// The key is for transports that partition: messages with equal keys stay in
/// order relative to each other (Kafka). An integration event is keyed by the
/// id of the aggregate it is about. The in-memory transport keeps every
/// message in order and ignores the key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Message {
    key: Option<Vec<u8>>,
    payload: Vec<u8>,
    headers: Vec<(String, Vec<u8>)>,
}

impl Message {
    /// A message carrying `payload` and no key.
    pub fn new(payload: impl Into<Vec<u8>>) -> Self {
        Message {
            key: None,
            payload: payload.into(),
            headers: Vec::new(),
        }
    }

    /// The same message, keyed.
    pub fn with_key(self, key: impl Into<Vec<u8>>) -> Self {
        Message {
            key: Some(key.into()),
            ..self
        }
    }

    /// The same message with one more header. Headers are flat, name to
    /// bytes, as Kafka's are; anything nested travels as a JSON string.
    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }

    /// The first header of that name.
    pub fn header(&self, name: &str) -> Option<&[u8]> {
        self.headers
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, value)| value.as_slice())
    }

    /// Every header, in order.
    pub fn headers(&self) -> &[(String, Vec<u8>)] {
        &self.headers
    }

    /// The key, if the message has one.
    pub fn key(&self) -> Option<&[u8]> {
        self.key.as_deref()
    }

    /// The payload.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }

    /// The payload, owned.
    pub fn into_payload(self) -> Vec<u8> {
        self.payload
    }
}
