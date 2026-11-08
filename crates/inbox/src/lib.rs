//! Transactional Inbox on PostgreSQL.
//!
//! A message from outside may arrive twice, and may arrive before one it
//! depends on. The inbox stores every message under its identity, so a
//! second arrival is the same row; processes each one in a transaction that
//! also marks it processed, so the subscriber's writes and the mark commit
//! together; holds a message back until the messages it names as causal
//! dependencies have been processed; and, when the subscriber fails, records
//! the attempt, lets the message wait for its backoff without letting newer
//! ones pass it, and parks it after the attempts allowed (ADR-0005).
//!
//! ```ignore
//! // at the edge: a bus subscriber hands the message over
//! inbox.publish(&InboxMessage::new(tenant, "orders.Order", json!({"id": 7}), 3, uri, payload)).await?;
//!
//! // a processing loop
//! inbox.run(
//!     |tx, message| handle(tx, message),   // `tx` is the transaction the mark commits in
//!     Loops::default(),
//!     ctrl_c,
//! ).await?;
//! ```
//!
//! The edge sees [`Inbox`], one method; processing is on the adapter,
//! [`PgInbox`]. As a channel of the bus — a bridge for the intake, a
//! transactional consumer for the processing — see [`channel`]. A port of
//! `ascetic_ddd.inbox` (Python); see the `pg` module for what the table
//! and queries guarantee.
//!
//! # Logs
//!
//! What the inbox writes to the `log` facade names a row by its channel and
//! its place in the inbox, a message set aside by its digest, and an error
//! by the library's words and the server's codes: never a stream id, a
//! Tenant, a `message_id`, a header's value or a subscriber's text, which
//! may be personal data or the very value refused. That text is kept on the
//! row and handed to the [`InboxObserver`] (ADR-0036 R9).
//!
//! The driver is the host's to configure: `tokio_postgres` logs every
//! statement with its parameters, a message's values among them, at
//! `Debug`. Keep the target `tokio_postgres` at `Info` or above wherever
//! real data runs; `tests/logs.rs` holds that nothing it logs above `Debug`
//! carries a value.
//!
//! # Deviations from the Python source
//!
//! * A causal dependency is a type, [`CausalDependency`], not a dictionary
//!   with agreed keys; entries in the metadata that are not dependencies are
//!   ignored rather than raising.
//! * The slot of a row is `hashtext(<partition key>) % slots` with the sign
//!   bit cleared, stored with the row, and a dispatcher takes whichever slot
//!   has work under a lock on that slot (ADR-0008); in the source workers
//!   were told their share at start-up, and a negative hash matched no
//!   worker.
//! * `run` stops cooperatively, between messages, on a future the caller
//!   passes; a subscriber returns a `Result`. There is no async iterator.
//! * `payload` is bytes, not JSONB, mirroring the outbox (ADR-0002).
//! * `tenant_id` is a `String`; the source leaves its type to the schema.
//!   `uri` is 255 characters, as in the outbox, rather than 60.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod channel;
mod error;
pub mod intake;
mod message;
pub mod observer;
mod partition;
pub mod pg;
mod port;
mod snapshot;

pub use crate::channel::{INBOX_SCHEME, InboxChannel};
pub use crate::error::{BoxError, Error, Failure};
pub use crate::intake::{Bounds, Reason};
pub use crate::message::{CausalDependency, InboxMessage, MAX_DEPENDENCIES, MAX_IDENTITY_BYTES};
pub use crate::observer::{InboxObserver, Receipt};
pub use crate::partition::{ByStream, ByUri, PartitionKey};
pub use crate::pg::{
    Admission, InboxRetention, Loops, Outcome, PgInbox, Readmission, Readmitted, Retries,
    SetAsideCount, SetAsideCursor, SetAsidePage, SetAsideRow,
};
pub use crate::port::Inbox;
pub use crate::snapshot::Snapshot;
