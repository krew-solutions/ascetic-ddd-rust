//! The model: what this crate does, in one place.
//!
//! An aggregate's history is a stream of events, named by a [`StreamId`] —
//! the tenant, the aggregate's type, its id — and numbered from one. The
//! store appends events after the position the writer expects and refuses
//! when another writer got there first, which is how two decisions taken
//! on the same history cannot both stand; and it reads a stream back in
//! order, from a position, for the fold that rebuilds the aggregate. Every
//! event carries its [`EventMeta`]: an id of its own, what caused it, what
//! it belongs to, when, and the events elsewhere it depends on.
//!
//! An event is a value of the application's own type. An [`EventCodec`]
//! turns one into a record — a type name, a version, a JSON payload — and
//! back; a [`PayloadCodec`] turns the payload into bytes and back: JSON,
//! compressed, sealed with the stream's key, one inside the other. What the
//! sources did with exporters and reconstitutors over encapsulated objects
//! is two functions over values here.

mod event;
mod event_codec;
mod payload_codec;
mod stream;

pub use self::event::{Appended, CausalDependency, EventMeta, Recorded};
pub use self::event_codec::{Encoded, EventCodec};
pub use self::payload_codec::{EncryptionCodec, JsonCodec, PayloadCodec, ZlibCodec};
pub use self::stream::StreamId;
