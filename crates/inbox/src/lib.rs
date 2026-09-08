//! Transactional Inbox on PostgreSQL.
//!
//! A message from outside may arrive twice, and may arrive before one it
//! depends on. The inbox stores every message under its identity, so a
//! second arrival is the same row; processes each one in a transaction that
//! also marks it processed, so the subscriber's writes and the mark commit
//! together; and holds a message back until the messages it names as causal
//! dependencies have been processed.
//!
//! ```ignore
//! // at the edge: a bus subscriber hands the message over
//! inbox.publish(&InboxMessage::new(tenant, "orders.Order", json!({"id": 7}), 3, uri, payload)).await?;
//!
//! // a processing loop
//! inbox.run(
//!     |tx, message| handle(tx, message),   // `tx` is the transaction the mark commits in
//!     Workers::default(),
//!     ctrl_c,
//! ).await?;
//! ```
//!
//! The edge sees [`Inbox`], one method; processing is on the adapter,
//! [`PgInbox`]. A port of `ascetic_ddd.inbox` (Python); see the `pg` module
//! for what the table and queries guarantee.
//!
//! # Deviations from the Python source
//!
//! * A causal dependency is a type, [`CausalDependency`], not a dictionary
//!   with agreed keys; entries in the metadata that are not dependencies are
//!   ignored rather than raising.
//! * The worker filter clears the sign bit of `hashtext`, as in the outbox:
//!   in the source a negative hash matched no worker.
//! * `run` stops cooperatively, between messages, on a future the caller
//!   passes; a subscriber returns a `Result`. There is no async iterator.
//! * `tenant_id` is a `String`; the source leaves its type to the schema.
//!   `uri` is 255 characters, as in the outbox, rather than 60.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

mod error;
mod message;
mod partition;
pub mod pg;
mod port;

pub use crate::error::{BoxError, Error};
pub use crate::message::{CausalDependency, InboxMessage};
pub use crate::partition::{ByStream, ByUri, PartitionKey};
pub use crate::pg::{PgInbox, Worker, Workers};
pub use crate::port::Inbox;
