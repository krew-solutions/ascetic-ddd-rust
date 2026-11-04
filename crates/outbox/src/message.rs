//! What the outbox stores.

use serde_json::Value;

/// A message in the outbox.
///
/// The three fields a producer sets are the routing URI, the payload and the
/// metadata; the rest the database assigns on insert and the dispatcher reads
/// back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OutboxMessage {
    /// Where the message goes: `kafka://orders`, `kafka://orders/order-123`.
    /// The part after the topic is a partition key: messages with one full
    /// URI are dispatched by one worker, in order.
    pub uri: String,
    /// The message as it goes on the wire: serialized, and encrypted where
    /// the deployment requires it, before it reaches the outbox (ADR-0002).
    pub payload: Vec<u8>,
    /// About the message: must carry an `message_id` (a UUID) for idempotency,
    /// and may carry `correlation_id`, `causation_id` and the like.
    pub metadata: Value,
    /// When the row was inserted, as the database prints it.
    pub created_at: Option<String>,
    /// Order within the transaction that inserted it.
    pub position: Option<i64>,
    /// The inserting transaction, `pg_current_xact_id()`.
    pub transaction_id: Option<u64>,
}

impl OutboxMessage {
    /// A message to publish.
    pub fn new(uri: impl Into<String>, payload: impl Into<Vec<u8>>, metadata: Value) -> Self {
        OutboxMessage {
            uri: uri.into(),
            payload: payload.into(),
            metadata,
            created_at: None,
            position: None,
            transaction_id: None,
        }
    }
}

/// A snapshot of PostgreSQL's transactions, as `pg_current_snapshot()`
/// prints it, `xmin:xmax:xip`: every id below `xmin` had ended, none from
/// `xmax` on had, and `xip` lists those between that were still running.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// The least id still running, or `xmax`.
    pub xmin: u64,
    /// One past the latest id that had ended.
    pub xmax: u64,
    /// The ids between `xmin` and `xmax` still running, ascending.
    pub xip: Vec<u64>,
}

impl Snapshot {
    /// `'1:1:'`, in which nothing is visible: `'0:0:'` is not a valid
    /// snapshot.
    pub fn nothing() -> Self {
        Snapshot {
            xmin: 1,
            xmax: 1,
            xip: Vec::new(),
        }
    }

    /// Whether the transaction `xid` is visible in this snapshot: the
    /// arithmetic of `pg_visible_in_snapshot`.
    pub fn sees(&self, xid: u64) -> bool {
        xid < self.xmin || (xid < self.xmax && !self.xip.contains(&xid))
    }

    /// What this snapshot sees below `xid` and nothing from it on: a rewind
    /// to `xid` keeps of what was delivered only that (ADR-0035 decision 11).
    pub fn below(&self, xid: u64) -> Self {
        Snapshot {
            xmin: self.xmin.min(xid),
            xmax: self.xmax.min(xid),
            xip: self.xip.iter().copied().filter(|x| *x < xid).collect(),
        }
    }
}

impl std::fmt::Display for Snapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}:", self.xmin, self.xmax)?;
        for (i, xid) in self.xip.iter().enumerate() {
            if i > 0 {
                f.write_str(",")?;
            }
            write!(f, "{xid}")?;
        }
        Ok(())
    }
}

impl std::str::FromStr for Snapshot {
    type Err = String;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let malformed = || format!("snapshot `{text}`");
        let mut parts = text.splitn(3, ':');
        let (Some(xmin), Some(xmax), Some(xip)) = (parts.next(), parts.next(), parts.next()) else {
            return Err(malformed());
        };
        let number = |part: &str| part.parse::<u64>().map_err(|_| malformed());
        Ok(Snapshot {
            xmin: number(xmin)?,
            xmax: number(xmax)?,
            xip: if xip.is_empty() {
                Vec::new()
            } else {
                xip.split(',').map(number).collect::<Result<_, _>>()?
            },
        })
    }
}

/// An epoch a consumer group is handing on in one slot: the rows visible in
/// its snapshot and not in the group's `delivered`, in the order their
/// positions were drawn (ADR-0035).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Epoch {
    /// The snapshot the epoch was started with.
    pub snapshot: Snapshot,
    /// The last position handed on within it.
    pub offset: i64,
    /// Its highest position; `None` until a relay computes it, after a
    /// conversion from the positions of ADR-0007.
    pub end: Option<i64>,
    /// The generations of the ring it found rows in (ADR-0033), in order:
    /// a batch reads those alone. Empty while `end` is `None`.
    pub generations: Vec<Generation>,
}

/// A generation of the ring an epoch found rows in, and the lowest and
/// highest positions of its rows there: a batch reads it from the later of
/// the epoch's offset and `first`, up to `last`, and passes it by after.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Generation {
    /// The generation.
    pub generation: u32,
    /// The lowest position of the epoch's rows in it.
    pub first: i64,
    /// The highest.
    pub last: i64,
}

/// Where a consumer group is in one slot: every row visible in `delivered`
/// was handed on, and the epoch in flight, if any.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Position {
    /// Every row visible in it was handed on.
    pub delivered: Snapshot,
    /// The epoch being handed on.
    pub epoch: Option<Epoch>,
}

/// Where [`set_position`](crate::pg::PgOutbox::set_position) moves a
/// selection (ADR-0035 decision 11). Every form clears the epoch in flight:
/// left, it would drain and undo the move.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Move {
    /// Back to the start: nothing delivered, every row the table keeps
    /// handed on again — on the ring, from its `intact_from` (ADR-0033).
    Start,
    /// Back to transaction `xid`: of what was delivered, only what lies below
    /// it is kept, so every row of `xid` and of later transactions is handed
    /// on again, and no transaction still running below it is taken for
    /// delivered.
    From(u64),
    /// Ahead to now: every row committed so far counts as delivered, and the
    /// transactions still running stay owed.
    Now,
}
