//! The identity a message carries in its headers (ADR-0003, ADR-0030): the
//! Tenant it is under, the stream it belongs to and its position there.
//!
//! The inbox stores a message under this identity and takes a second one
//! under it for a redelivery of the first, ignoring it; it orders a stream's
//! messages by their positions. So a producer owes one message per identity
//! on a channel, and the outbox refuses a transaction that writes two
//! (ADR-0030). The names are the headers', and the outbox's `metadata`
//! fields of the same names.

/// The Tenant the message is under.
pub const TENANT_ID: &str = "tenant_id";
/// The stream's type.
pub const STREAM_TYPE: &str = "stream_type";
/// The stream's id, as JSON text: the inbox compares it as JSON.
pub const STREAM_ID: &str = "stream_id";
/// The message's position in its stream.
pub const STREAM_POSITION: &str = "stream_position";

/// The identity's headers, in the order the inbox's key names them.
pub const HEADERS: [&str; 4] = [TENANT_ID, STREAM_TYPE, STREAM_ID, STREAM_POSITION];
