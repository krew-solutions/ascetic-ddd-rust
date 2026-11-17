# ascetic-ddd-inbox

Transactional Inbox on PostgreSQL: incoming messages stored under their
identity, processed once each in the transaction that marks them processed,
and held back until what they causally depend on has been processed. A port
of `ascetic_ddd.inbox` (Python), adapted to Rust rather than transliterated.

```rust
// at the edge: a bus subscriber hands the message over
inbox.publish(&message).await?;

// a processing loop; `tx` is the transaction the mark commits in
inbox.run(|tx, message| handle(tx, message), Loops::default(), ctrl_c).await?;
```

## What it guarantees

* **Idempotency.** A message's identity is
  `(tenant_id, stream_type, stream_id, stream_position)` and is the primary
  key; receiving it again is `ON CONFLICT DO NOTHING`. A `message_id` in the
  metadata is unique in the table as well. A channel with a window remembers
  a processed message for that long; a repeat that arrives after is
  processed again (below).
* **Once, with the work.** The subscriber runs inside the transaction that
  marks the message processed, and is given that transaction. Its writes and
  the mark commit together or not at all.
* **Causal order.** A message may name, in `metadata.causal_dependencies`,
  the messages that must be processed first. It is stepped over until they
  are, and looked at again next time.
* **One dispatcher at a time per slot.** A row carries its slot, the hash of
  a partition key, the URI or the stream, and a dispatcher is whoever holds
  the lock on that slot's row for the length of a transaction. With causal
  dependencies, cut by stream.
* **One subscriber, every channel.** The inbox takes no filter by address:
  a consumer that saw one address would wait for ever on a causal
  dependency published under another, and the wait is resumed by the
  transaction that marks the dependency, which needs one loop over every
  row. The subscriber routes by the row's `destination` — the inbox channel
  the intake published it to, key included, the receiver's own name for it
  — and by `stream_type` (ADR-0018; the Python source dropped its
  `subscribe(uri)` for the same reason).

## The port and the adapter

The edge sees `Inbox`, one method, `publish`. Processing — `dispatch`,
`run`, `migrate` — is on `PgInbox`, because it is the business of a separate
loop.

## As a channel of the bus

The inbox is also an adapter of `ascetic-ddd-bus` (ADR-0003). A bridge from
a broker channel to the inbox channel is the intake: every wire message is
stored under the identity its headers name, and the same identity again is
ignored. The consumer is transactional and comes from the inbox itself: its
handler runs inside the transaction that marks the message processed, and is
given that transaction.

```rust
let mut bus = Bus::new();
bus.register("kafka", kafka)?;
bus.register(INBOX_SCHEME, inbox.channel())?;
let bus = Arc::new(bus);

// the intake
let intake = Bridge::new(Arc::clone(&bus))
    .run("kafka://orders", "orders-intake", Target::Fixed("inbox://orders".into()))?;

// the processing; `tx` is the transaction the mark commits in
let orders = inbox.consumer(decode_order_placed);
let processing = orders.subscribe(|tx: PgSession, order: OrderPlaced| async move {
    place(&tx, order).await
})?;
```

Headers become columns: `tenant_id`, `stream_type`, `stream_id`,
`stream_position`; `destination`, the channel the message was sent to as
the outbox stamps it, becomes `uri`, and without one the inbox channel the
message was published to, key included, does. Every other header is a
field of `metadata`, structured again when its text is a JSON array or
object. Published from an outbox to `inbox://orders/order-7`, a message
crosses the outbox dispatcher's bridge into the inbox without a broker:
after-commit delivery and processing in the marking transaction, in one
database.

The processing loop is a task on the tokio runtime; cancel the subscription
to stop it. `with_loops` on the inbox says how it runs — how many loops in
this process, how long one waits — the same `Loops` that `run` takes.

## Dependencies

A message may name causal dependencies, messages that must be processed
before it, and may arrive before them. The walk looks at the head of the
queue only: a head whose dependencies are not all processed is set aside to
wait for the first missing one, `waiting_for`, out of the queue, and the
statement that marks that dependency processed puts every row waiting for it
back, in the same transaction. Nothing polls for a dependency, and a waiting
row costs the walk nothing. The row stays in the table, so a later arrival of
the same message is still a duplicate.

A row that has waited longer than `with_max_wait`, an hour by default
(ADR-0037 decision 6), is parked by the expiry with the dependency and the
reason named in `last_error`: its dependency never arrived or is parked,
it waits through waiting rows on one that never arrived or is parked, or no
queued row is within 8 hops of waiting rows. A row whose dependency
is there and on its way — untried, held for its stage's outage, behind a
held head or a long backlog — waits on, and the expiry neither writes nor
locks it: `max_wait` means that the dependency never arrived (ADR-0042
decision 7). A parked row keeps the identity it waited for, so the
dependency's mark, when it comes after all, puts it back in the queue.

The expiry is off the dispatch path: `run` drives it on a timer of its own,
a pass of statements one after another while each examines a whole batch of
waiters, then a rest of a tenth of `max_wait`, so a row whose dependency
never arrived is parked within `max_wait`, a tenth of it and one pass. A host
that drives `dispatch` itself calls `expire()` the same way: again at once
while `Expiry::more`, after its rest otherwise. A pass goes from the key its
last statement stopped at, `(waiting_since, received_position)`, so a waiter
behind any number of spared ones is reached: 220,001 waiters take 2,201
statements, 2.3 s, on a 12-core NVMe machine under PostgreSQL 14. A defect
of the expiry stops `run`'s loops, as one of a dispatch does.

Setting a head aside is the whole of that `dispatch`, `Outcome::SetAside`:
the wait is committed at once, and the next call takes the slot's next
head. The wait and the mark of the dependency take a transaction-level
advisory lock on the dependency's identity, the wait before its last look
at the dependency, the mark before the mark; under READ COMMITTED a mark
cannot see a wait that is not committed, and without the lock a mark could
slip between the look and the wait, and the row would wait for a processed
dependency for ever (ADR-0009). The mark takes the lock only on tables with
more than one slot: with one, every dispatcher serializes on it.

The mark and the waking are one statement, so the happy path keeps its round
trips. Draining 200 messages with a subscriber that does nothing, over
localhost, three runs: 1.43–1.76 ms per message, against 1.54–1.83 ms before
the waking was added; no difference the measurement can tell.

## Failures, backoff and parking

A subscriber that fails does not roll the whole transaction back. It runs in a
savepoint; its writes roll back to it, and the transaction goes on to record
the attempt: `attempts`, `last_error`, and `next_attempt_at`. Until that time
the message is not taken, and it holds its (slot, Tenant) — that Tenant's rows
behind it wait — so that the order of arrival survives the failure; a retry
topic would let them pass and lose it. The slot's other Tenants go on. After
`max_attempts` failures the message is parked: `parked_at` is set, the
selection passes it by, the Tenant flows, and the
row stays where it is, so a later arrival of the same message is still a
duplicate. Off by default: unlimited attempts, no backoff, the message is
taken again at the next call (ADR-0005).

```rust,ignore
let inbox = PgInbox::new(pool)
    .with_retries(Retries::up_to(5).with_backoff(Retries::exponential(
        Duration::from_secs(1),
        Duration::from_secs(300),
    )));

match inbox.dispatch(&subscriber).await? {
    Outcome::Processed => {}
    Outcome::Failed { attempts, parked, held } => {}
    Outcome::SetAside => {}
    Outcome::Held => {}     // a head held without asking (below)
    Outcome::Passed => {}   // the walk found no head due; go on at once
    Outcome::Paused => {}   // every head met is of a paused channel; poll
    Outcome::Nothing => {}
}

for message in inbox.parked(&session).await? {
    inbox.unpark(&session, &message).await?;  // another go, attempts reset
    inbox.resolve(&session, &message).await?; // or: processed by hand, no effects
}
```

Only errors the subscriber returns are counted. A message that kills the
process is retried on restart with the count unchanged; counting at delivery
would need a lease with a timeout, which the inbox does without. A message
depending on a parked one waits, as it waits for a dependency that never
arrived, until the parked one is unparked and processed or resolved.

A subscriber's error is a `Failure`. Any error converts into
`Failure::Transient`, so `?` works and the message is tried again as above;
`Failure::Permanent` is the subscriber's verdict that no retry will ever
succeed — a payload it cannot read, an invariant the message breaks — and
the message is parked at once, attempts left or not. Errors of the database
are the loop's business, not the subscriber's: a loop of `run` that meets an
error of the moment — a lock cycle the server broke, a connection lost, a
server going down, told by SQLSTATE in `ascetic_ddd_session::pg::transient`
— waits, longer with each one in a row up to `Loops::max_pause`, and goes on;
a defect stops every loop and `run` returns it (ADR-0010).
Through the bus, a handler or a stage says the same with the one verdict the
bus carries, `ascetic_ddd_bus::Permanent`: the inbox's consumer reads it
and parks the message; any other error of a handler is of the moment.

```rust,ignore
let subscriber = |tx: &PgSession, message: &InboxMessage| async move {
    let order: OrderPlaced = decode(&message.payload)
        .map_err(Failure::permanent)?;   // no retry will read it
    place(tx, order).await?;             // any other error: tried again
    Ok(())
};
```

`Failure::Unavailable` says that what the subscriber needs cannot answer now —
a key service sealed, a quota spent — at the scope of a `Tenant` or of the
whole `Stage` (ADR-0040, ADR-0042). It holds the row and spends no attempt,
however often it is told, so that an outage never parks a message. The
process remembers the Tenant's trouble: its other heads are held without the
subscriber called (`Outcome::Held`) while its back-off runs, one ask of it in
flight at a time. A `Stage` verdict broad across Tenants pauses the channel
in this process — a circuit breaker per process and channel, which no other
process shares, since a key served from one's cache is no evidence of
another's. A paused channel's heads are not asked: the walk goes on as a
guest, serving the other channels' heads and moving a cursor of its own, and
once a tick a trial asks one of the channel's Tenants, its witness, whose
answer resumes the channel. `PgInbox::with_breaking(Breaking)` sets the
window, the thresholds, the ticks, the back-off and the trial's wait; the
observer is told of each head held unasked and of each pause, stall and
resume. A row's hold runs from the database's clock as its verdict is
written. A dispatch, a trial and a `resolve` run under READ COMMITTED only,
checked for each transaction; another level is refused.

The savepoint costs two statements per message on the happy path. Draining
200 messages with a subscriber that does nothing, on one PostgreSQL over
localhost, best of three runs: 1.39 ms per message before, 1.54 ms after;
the other runs 1.69–1.74 ms before and 1.80–1.83 ms after. About 150 µs, a
tenth with a subscriber that does nothing, less with one that does anything.

## Slots

A row carries its slot, `hashtext(<partition key>) % slots` with the sign bit
cleared, stored at insert; the key, `ByUri` or `ByStream`, and the number of
slots are fixed for the life of the table, `partitioned_by` and
`with_slots(S)` before `migrate` creates it, the URI and one slot by default.
The table `<table>_slots` has one row per slot. A dispatcher has no identity:
`dispatch(subscriber)` takes whichever slot has a due head and is held by
nobody, least recently served first, locks that slot's row for the length of
the transaction, `FOR UPDATE SKIP LOCKED`, and serves that slot's Tenants in
turn (below).
Any number of loops in any number of processes share the slots through the
locks alone, `run(subscriber, Loops { concurrency, poll_interval },
shutdown)`; a process that dies releases its slot with its transaction, and
the next poll of a survivor takes it. **The order kept is per (slot,
Tenant):** within a slot, the messages of one Tenant are processed one at a
time, in arrival order; the messages of different Tenants have no order
between them. Under `ByStream` that is per stream, which holds its Tenant;
under `ByUri` per (URI, Tenant). A host that relied on order across Tenants
in one slot no longer has it (ADR-0041 decision 1). A head waiting for its
backoff holds its (slot, Tenant), and that Tenant's other rows in the slot
wait behind it; the slot's other Tenants and the other slots flow. Changing
the number of slots or the key
moves rows between slots and is a migration, not a restart: `migrate` refuses
a table cut otherwise (ADR-0008).

The tables are a component of the database's migration ledger, `inbox:<table>`
(ADR-0029): `migrate` creates them once and is a read of the ledger after,
so that a process starts beside live traffic without locking them;
`teardown` drops them and forgets them, for an inbox that lives as long as a
test.

Taking a slot is one statement, and walking it for its head another, under
a snapshot taken after the lock: a statement's snapshot predates the lock it
takes, and what the slot's previous holder committed in between — its head
processed, set aside, put in backoff — is out of the first statement's
sight.

Both statements are functions the baseline makes, `<table>__take()` and
`<table>__walk(slot, walk, max_dormancy)`, and the dispatch calls each
(ADR-0041 decision 6). Sent as text, a statement is parsed and planned at
every call: that cost the walk 3.3 ms against 0.4 of execution. A
function's statement is parsed once a connection, and its plan kept until
its tables' definition or statistics change — an `ANALYZE` plans it again:
- the walk's plan is forced generic, so that a session asking for custom
  plans does not plan it at every call; the take's has no parameter and is
  generic anyway;
- the first call on a connection pays the parse and the plan: 12 ms and
  6.5 ms on the record's laptop;
- `migrate` refuses either function when it is not as the library makes
  it: its arguments, result, body, language, volatility, setting, rights or
  schema changed, EXECUTE gone from the pool's role, or another function of
  its name in its schema or the search path. The refusal names what it
  found;
- EXECUTE on both is PUBLIC's, as on any function: they run with the
  caller's rights, so they grant nothing the caller lacks. A database whose
  default privileges take EXECUTE from PUBLIC grants it to the role that
  dispatches; `migrate`, run by that role at its start, refuses otherwise;
- a pool that runs `DISCARD ALL` or `DISCARD PLANS` at each checkout plans
  them again at each: correct, at the old price. deadpool's recycling
  methods do not;
- `pg_stat_statements` shows the calls, with their time and buffers, and
  the statements inside them with `pg_stat_statements.track = all`.

To see the plan the walk runs: `auto_explain.log_nested_statements = on`,
or the statement prepared under the generic plan, inside a transaction that
is rolled back:

```sql
BEGIN;
SET LOCAL plan_cache_mode = force_generic_plan;
PREPARE w (smallint, integer, double precision, text[]) AS <the walk's statement>;
EXPLAIN (ANALYZE, BUFFERS) EXECUTE w (0, 32, 1.0, '{}');
ROLLBACK;
```

The statement is `prosrc` of `<table>__walk`, less its `INTO`. A call by
hand, `SELECT * FROM <table>__walk(…)`, writes the slot's row like any
dispatch: its cursor and its round. Outside a rolled-back transaction it
moves them.

The head index, `(slot, received_position)` over the rows
neither processed, parked nor waiting, is the only index in
`received_position` order on purpose: beside a second one over the whole
column the planner walked that one from the first row, past the whole
processed history, 89 ms at sixteen slots.

## Tenants in turn

A slot's holder serves its Tenants in turn, so that one Tenant's backlog
does not hold the others back (ADR-0041 decision 2). The slot row keeps a
cursor, the Tenant served last. A dispatch walks the slot's Tenants after the
cursor, cyclically, wrapping once, one descent of `__tenant_head_idx` each,
and takes the first whose head — its oldest queue row in the slot — is due.
It examines at most `Rotation::walk` Tenants, 32 by default; finding none
due, it moves the cursor past them and returns `Outcome::Passed`, and the
loop goes on at once. A round over the slot's Tenants that finds no head due
puts the slot to sleep until the earliest backoff it saw, a row arriving
after the round began, or `Rotation::max_dormancy`, a second by default —
the bound on a row made due behind its back: an insert in flight when the
round began, a row a mark in another slot woke, a row unparked. So a slot
whose every head waits writes ⌈T/K⌉ `Passed` and one `Nothing`, then nothing
until it wakes. `PgInbox::with_rotation` sets both.

What holds a slot's Tenants is counted from the table, for a host's gauges:
`held_counts(within)` gives, by channel, kind and cause, how many Tenants
have a head held and how many heads, and the oldest hold, never which: a
head waiting its own back-off, or held for an `Unavailable` (ADR-0042
decision 6). `unpark_all(principal, unparking)` re-arms the rows one cause
parked, each with a canary of `tries` failures before it parks again.

## Forgetting

Rows of a channel — the destination without its key — that a host gives a
window are forgotten once processed longer ago than it, by a sweep that runs
beside the loops of `run`, on a session of its own (ADR-0033 decision 4):

```rust
let inbox = inbox.with_retention(
    InboxRetention::keep_everything().window("kafka://orders", Duration::from_secs(9 * 86_400)),
);
```

Every other channel keeps its rows, as it must when its stream types are
named as causal dependencies, or when a host cannot bound its repeats. A
window covers the longest a repeat can take to arrive: the broker's whole
retention, tiers included, the time a segment outlives it, and the
producer's replay horizon, which the outbox enforces on rewinds. The sweep
forgets in statements of `batch` rows under a try-lock, never an
unprocessed, parked or waiting row, nor one a waiting row names; it starts
each statement where the last one of its channel stopped, forgets nothing for
its `grace` after the start or after a pass that could not reach the
database, and its errors never touch the loops. The mark keeps its time,
`processed_at`; rows processed before it did are kept until
`stamp_processed()`, a one-time pass, stamps them. `sweep()` runs one pass on
demand.

## Observing the inbox

`PgInbox::observed_by(observer)` attaches an `InboxObserver`, in the shape of
the session and outbox observers: a synchronous, infallible value, composed as
a tuple, fixed when the inbox is built. It is told of a message
received, with its order of arrival, its slot and the id of the storing
transaction, or that the identity was already there; a row set aside to wait
for a dependency, which is the whole of that dispatch; a slot taken and its
head, or no slot, or a slot taken with nothing due in it; the subscriber's
outcome;
the mark, with its order of processing and the rows it woke; the attempt
recorded after a failure, with whether it parked the message; rows whose
wait ran out, parked; the dispatcher's transaction closed, committed or
rolled back; a message unparked or resolved by an operator, with the rows
the resolve woke; rows a sweep forgot, per channel; and, beside a message
ignored, whether the row holding its identity went before it could be read. A dispatcher's events name the slot it holds, which is
what tells one dispatcher's events from another's when several run at once:
a slot has one holder at a time. A stored message comes with the id of the
transaction that stored it, and every step of a walk over the table with the
snapshot its statement ran under, so that a recorded run says which rows
each walk could see whatever order two tasks' events were logged in. These
are the actions of the protocol model in `verify/tla/Inbox.tla`, with the
steps the model folds into one made visible, so a recording observer yields a
trace the model can be checked against. The tests with one dispatcher at a
time do exactly that, through the recorder of `ascetic-ddd-trace`: with
`ASCETIC_DDD_TRACE_DIR` set they write one JSON line per event, and
`verify/tla/check.sh` replays those files through the model. A test with
several dispatchers at once records nothing — the order its events are logged
in is not the order of their commits — and asserts the table's state instead.

## Deviations from the Python source

* A failed message is retried with a recorded attempt, may wait for a
  backoff and may be parked (ADR-0005); the source rolls the transaction back
  and retries at once, for ever.
* A message ahead of its dependency is set aside and woken by the
  dependency's mark (ADR-0006); the source steps over it on every walk.

* A causal dependency is a type, `CausalDependency`, with `serde`; entries
  in the metadata that are not dependencies are ignored.
* The slot of a row is `hashtext(<partition key>) % slots` with the sign bit
  cleared, stored with the row: in the source a negative hash matched no
  worker, so about half of all keys were never processed once
  `num_workers > 1`. A test spreads forty streams over three slots and checks
  that each is processed exactly once.
* Dispatchers have no identity (ADR-0008). The source told each worker its
  share, `hash % num_workers`, at start-up, so a dead worker's share stood
  still until it was restarted; here a dispatch takes whichever slot has
  work under a lock on that slot's row, and a survivor takes over.
* `run` stops cooperatively, between messages, on a future the caller
  passes. A subscriber returns a `Result`. There is no async iterator.
* `payload` is bytes, not JSONB, mirroring the outbox (ADR-0002).
* `tenant_id` is a `String`; `uri` is 255 characters rather than 60.
* The identifier is `message_id`, not `event_id`, as in the outbox.

## Testing

```bash
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-inbox -- --ignored
```
