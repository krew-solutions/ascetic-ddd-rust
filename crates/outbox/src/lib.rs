//! Transactional Outbox on PostgreSQL.
//!
//! A business operation that changes state *and* tells the outside world
//! about it cannot do both atomically over two systems: a crash between the
//! commit and the publish loses the message. The outbox writes the message
//! into a table in the same transaction as the state change, and a separate
//! dispatcher reads what was committed and sends it on. Both happen, or
//! neither.
//!
//! ```ignore
//! session.atomic(async |tx| {
//!     orders.save(tx, &order).await?;
//!     outbox.publish(tx, &OutboxMessage::new(
//!         "kafka://orders",
//!         encode(&OrderPlaced { order_id: order.id }),   // wire bytes, ADR-0002
//!         json!({ "event_id": event_id }),
//!     )).await?;
//!     Ok(())
//! }).await?;
//!
//! // elsewhere, a dispatcher process:
//! outbox.run(send_to_broker, &Selection::group("broker"), Workers::default(), ctrl_c).await?;
//! ```
//!
//! The application layer sees [`Outbox`], one method; dispatching is on the
//! adapter, [`PgOutbox`]. A port of `ascetic_ddd.outbox` (Python); see the
//! `pg` module for what the tables and queries guarantee.
//!
//! # Deviations from the Python source
//!
//! * The worker filter clears the sign bit of `hashtext(uri)`. In the source
//!   a negative hash matched no worker, so about half of all keyed URIs were
//!   never dispatched once `num_workers > 1`.
//! * `run` stops cooperatively, between batches, on a future the caller
//!   passes; there is no event to poll. A subscriber returns a `Result`
//!   instead of raising, and `run` returns the first error after stopping
//!   the other loops.
//! * There is no async iterator: an iterator that yields a message cannot
//!   know when the consumer is done with it, and the acknowledgement must
//!   follow the processing. The subscriber form is the only one.
//! * The transaction id is a `u64`, which is exactly `xid8`.
//! * `payload` is bytes, not JSONB: the message is serialized, and encrypted
//!   where required, before the outbox, so the dispatcher relays bytes and
//!   plaintext never rests in the database (ADR-0002).

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod channel;
mod error;
mod message;
pub mod pg;
mod port;

pub use crate::error::{BoxError, Error};
pub use crate::message::{OutboxMessage, Position};
pub use crate::pg::{DEFAULT_BATCH_SIZE, PgOutbox, Selection, Worker, Workers};
pub use crate::port::Outbox;

pub use crate::channel::{OUTBOX_SCHEME, OutboxChannel};
