# ADR-0036: The inbox accounts for every message it is handed, and only its environment holds the source

## Status

Accepted (2026-11-07, by the owner), after its revision by the roles'
review and with the owner (§ Revision after the roles' review, § Change
Log). Part A, the intake and the surface of what it sets aside, is being
built. Part B, the parked rows, waits on what § Revision names.

It is the output of two rounds of a blind solver and a discriminator, run
for the host project that uses this library (its ADR-0086, S3b-4):

- **The first round** was asked about an inbox row that can never be
  processed on a channel whose rows must never be resolved by hand. It found
  that a message the intake cannot store stops the broker's consumer.
- **The second round** was asked about that stall alone.

Both solvers worked from the problem without the lead's position. The
discriminators checked both positions against the code, PostgreSQL 14.24,
and a measurement on Redpanda (§ Context).

It revises ADR-0033 I3 ("a collision is an error that its source retries
without end") and the Kafka adapter's contract. It leaves ADR-0030's
decision on identities in place.

## Context

**What the intake does with a message today.**

- The bridge hands the inbox's intake one message at a time and
  acknowledges it only once the intake has taken it (`bus/src/bridge.rs`).
- The intake derives the row's identity from the headers: tenant, stream
  type, stream id, stream position and `message_id` (`inbox/src/channel.rs`).
  The stream id is parsed as JSON, or kept as a string when it does not
  parse.
- It writes the row with `INSERT … ON CONFLICT (tenant_id, stream_type,
  stream_id, stream_position) DO NOTHING` (`inbox/src/pg/store.rs`). A unique
  index casts `metadata->>'message_id'` to `uuid`.
- Every failure is returned as a transport error (`channel.rs`). That
  includes:
  - a `message_id` held by a row at another identity (23505 on the unique
    index, which is not an arbiter of the targeted conflict);
  - a `message_id` that is not a UUID (22P02);
  - a header missing, or a position that is not an integer;
  - a NUL in a text column (22021, or 22P05 in jsonb);
  - a value too long for its column (22001), or a key past the index's row
    size (54000).
- With the primary key as the only arbiter, a concurrent duplicate raises
  23505 as well, when both inserts pass the key's pre-check before either has
  its entry.

**What the adapters do with an error.**

- The Kafka loop retries a failing handler every second, for ever, without
  polling and without storing the offset (`bus/src/adapters/kafka.rs`). One
  consumer and one loop serve every partition the member holds. Its own
  documentation says that "a poison message must not stop the partition".
- The in-memory adapter logs the failure and drops the message
  (`in_memory.rs`), so no test on it can see the stall.
- The own-Command lane is a bridge from the outbox into the same intake. The
  outbox retries from the failing message.

**What was measured on the current code** (the host's `tests/poison_drill.rs`,
Redpanda, two members of one group, a topic of two partitions, ten keys
publishing every half second, one message failing for a while):

- **With `max.poll.interval.ms` = 10 s:**
  - the member holding the message stopped every partition at once;
  - after 10.4 s it left the group, and its partition, the message with it,
    moved to the other member, which stopped as well;
  - from 15 s to 60 s the group consumed nothing.
- **With the driver's default of 300 s:**
  - the member held both partitions and stopped for 358 s, while the other
    member had none, since the driver's heartbeat keeps the held member in
    the group until the interval passes;
  - at 300 s the message moved and stopped the second member.
- **Once the message passed**, every member rejoined unaided. Nothing was
  lost (2,310 and 8,650 messages) and a bounded number were handled twice
  (1 and 51).

So one message that can never be stored stops its channel in every member
of the group, in turn. A hold caused by the environment is safe: the members
recover once its cause passes.

**What the library's ADRs said.**

- ADR-0033 I3 made a collision an error that its source retries without end.
- ADR-0030 had rejected refusing a message because "a refusal stalls the
  outbox's slot, which other channels share".

The two disagree, and the measurement sides with ADR-0030's reason.

## Decision

### 1. The intake takes every message it is handed

The intake's outcome is one of:

- **stored:** the insert returned a row;
- **a duplicate:** no row, and the message's own identity is held. This is
  a redelivery or a shadow, as ADR-0030 decision 4 has it;
- **set aside**, for one of these reasons:
  - *malformed*: decided before any statement, for an identity header
    missing, a header or key that is not UTF-8, or a position that is not an
    `i32`;
  - *unstorable*: the insert's SQLSTATE is in the closed list 22P02, 22001,
    22021, 22P05, 54000;
  - *identity taken*: no row, the message's own identity is free, and its
    `message_id` is held at another identity;
- **an error** for everything else: the pool, the session, no SQLSTATE,
  08\*, 40001, 40P01, 53\*, 57\*, 55P03, 25006, 42P01, 42501, 2200H, a 23505
  on an index the library does not own, a code raised by a host's
  `PartitionKey` expression. The source holds and retries.

**The list is closed and the default is to hold.**
- A failure not foreseen holds the source, which is the measured safe
  behaviour.
- Diverting a message that could have been stored would break its stream's
  order.

**The insert's conflict has no target.** `ON CONFLICT DO NOTHING` makes every
unique index an arbiter.
- A `message_id` held elsewhere no longer raises.
- A concurrent duplicate waits on the other insert and does nothing.
- On the path where no row is stored, the holder of the identity is read as
  today. If there is none, one more statement reads the identity holding the
  `message_id`: found means *identity taken*, not found means today's
  `holder_unknown`.
- A cast that fails inside the index expression still raises 22P02, which
  is in the list.

### 2. What is set aside is kept beside the inbox, written in a transaction of its own

- **The table.** A failed insert is rolled back, and the message is written
  to `{table}_rejects` in a second transaction. The source's offset is
  stored only once that commits. No savepoint is needed: the failed insert
  has nothing to be atomic with.
- **A row of `{table}_rejects`:**
  - `digest`: a UUID from the first 16 bytes of the SHA-256 of the channel and
    the framed message, so that a repeat of the same bytes is the same row;
  - the channel, without its key;
  - the identity's columns, each null where the message did not give it, so
    that a read by identity reports the message as set aside, not as absent;
  - the reason, and a detail. The detail is never PostgreSQL's message text,
    which echoes values;
  - the identity holding the `message_id`, for *identity taken*;
  - the framed message, key, headers and payload, as `bytea`;
  - `seen`, the first and last times, and a disposition, its time and its
    principal.
- **A repeat** of the same bytes adds to `seen`.
- **The payload** is the producer's ciphertext where the channel is sealed:
  unreadable after the Tenant's key is shredded.
- **The headers** are clear text, as the inbox's own identity columns are.
- **The observer** is told `on_rejected` after the commit: channel, Tenant
  when its header is a valid one, stream type, reason, digest, repeat.
- **The library's logs** name the channel, the reason, the missing header's
  name and the digest. Never a header's value, a stream id, a `message_id`,
  or SQL text.

### 3. The adapters' contract: an error means "not taken"

- A handler's error means the message was not taken. The adapter
  redelivers that message, in order, after its pause, until it is taken or
  the subscription is cancelled.
- No adapter skips a message on an error. Only the store that can set a
  message aside may take one it cannot use, which is the inbox's intake. A
  handler that will never take a message must take it into its own store and
  return success.
- A panic is still skipped and reported, as today.
- **Kafka** keeps its loop, and its documentation says this.
- **In-memory** stops dropping. It retries in order, with a delivery task
  per group, so that one group's failure does not hold another's. The cancel
  releases the subscription between two tries.
- **Owed beside it:**
  - pausing the one partition that holds instead of every partition of the
    member, while still polling (`split_partition_queue`). The member would
    then stay in the group through an environment hold;
  - the in-memory suites of the host, which lose a message today on a
    transient intake failure.

### 4. What the operator reads and does, beside the parked rows

The rows set aside and the parked rows share one surface. Their kinds
differ:
- a parked row was stored, and its subscriber failed;
- a row set aside was never stored.

Both are listed, counted and disposed of through `PgInbox`, on the caller's
session.

- **Counts.** `set_aside_counts`: the open rows and the oldest one's age,
  by channel and reason. The host's gauge pages on any open row of a channel
  it declares critical, since every reason is a producer's defect.
- **A page.** A keyset page of the rows set aside, by channel and reason.
  Each row gives its digest, reason, detail, holder, `seen` and times, and
  the metadata keys the caller names. It never gives the payload: the
  metadata carries a wrapped data key.
- **The bytes.** `set_aside_message(digest)` returns the raw bytes, for
  forensics.
- **Readmit.** `readmit(digest)` runs the same intake over the kept bytes.
  - Stored or a duplicate closes the row as readmitted. Still set aside
    leaves it open.
  - It is a disposition the channel declares. A channel folded by its
    producer's positions readmits over a higher position: a set-aside
    `managed` is repaired exactly by readmitting it. A channel that depends on
    arrival order may refuse it.
  - `readmit_all(channel, reason, window)` readmits in first-seen order, for
    a flood.
- **Discard.** `discard(digest, reason, principal)` closes the row as
  discarded and keeps it as a tombstone: a later repeat of the same bytes
  stays discarded, and is counted.
- **Counting and logging.** Every disposition is counted once committed,
  and logged with the principal, the channel and the digest.

The parked rows' reads, states and dispositions are this ADR's second part
(§ Decision 6).

### 5. Retention and forgetting

- A closed row set aside is forgotten with its channel's window
  (`InboxRetention`).
- An open row is kept until it is disposed of.
- A row without a Tenant has a retention of its own, since no forget by
  Tenant reaches it.
- Forgetting a Tenant (§ 6) deletes its rows set aside with its inbox rows.

### 6. The parked rows: read by state, disposed of by channel (the host's S3b-4)

- **Parked rows by channel.**
  - A partial index over the channel's expression and the received
    position, `INCLUDE (parked_at)`, `WHERE parked_at IS NOT NULL`.
  - Their count and oldest age by channel.
  - A payload-free page of them, replacing `parked()`, which scans the table
    and returns payloads.
  - Measured: the index builds in 9.5 s over 4.3 million rows (4.3 GB of
    heap). The migration runner takes ACCESS EXCLUSIVE first (ADR-0029
    decision 3), not the build's ShareLock, so the whole inbox stops for
    that long (§ Revision).
- **A message's state by identity**, on the caller's session, sharing its
  snapshot under REPEATABLE READ:
  - absent, with the stream's head position;
  - queued, with how long its slot has been idle;
  - retrying;
  - waiting;
  - parked;
  - processed;
  - set aside.

  Each comes with the metadata keys the caller names. A slot's idleness reads
  as "will be reached" only under bounded retries, which the API documents.
- **Unpark and resolve by identity.**
  - A channel may refuse a resolve. The refusal is a predicate of the UPDATE
    on the stored `uri`.
  - The observer is told the stored row. Today it is told the caller's
    message, so a forged `uri` passes a refusal and is counted on another
    channel.
- **Discard of a parked row.** It is a disposition the channel declares:
  any, only when overtaken by a processed row of the stream at a higher
  position, or never. It wakes nothing: a waiter expires, naming the
  discarded row.
- **Supersede.** An arrival at a parked row's identity with the same
  `message_id` and different bytes replaces it, in one statement:
  `ON CONFLICT DO UPDATE … WHERE parked_at IS NOT NULL`. It never touches a
  processed row.
- **One encoding of an identity from a header's text**, the intake's own. A
  key that parses as a number is stored as a JSON number.
- **Forgetting a Tenant.**
  - The Tenant is recorded in the inbox's own table, so that the intake
    counts and acknowledges its messages and never stores them.
  - Its rows on every channel, parked and set aside included, are deleted in
    batches.
  - It is idempotent. A host runs it at the start of the off-boarding and
    again at its end, since a publish whose snapshot predates the first run
    can still insert after it.
- **Owed:**
  - two header texts that name one stream: `1e5` and `100000`; and `1` and
    `1.0`, which land in two slots;
  - a stall gauge per slot, which needs the time the slot became due.

## Revision after the roles' review (2026-11-07)

Four roles attacked the decisions above: distributed systems, database,
security, and evolution with verifiability. Each finding below came as a
failure scenario with its reference, and each is taken in with its
executor. Where this section and the decisions above disagree, this
section holds.

### The decisions that change

**R1. A supersede is never the intake's statement.**
- **The fact.** PostgreSQL requires a target for `ON CONFLICT DO UPDATE`
  (42601 without one, measured). With the primary key as the target, the
  `message_id` index stops being an arbiter, and *identity taken* and the
  concurrent duplicate raise 23505 again. Every role found this on its own.
- **The intake's one statement stays an untargeted `DO NOTHING`.**
- **The supersede leaves this ADR** (Part B). Written as a separate
  `UPDATE` on the no-row path, it would still be last-writer-wins. A
  member that has left the group keeps retrying the older bytes after the
  correction is stored, and its late retry would replace the correction:
  the measured 51 late handlings are exactly such calls.
- **A supersede needs two things first:** an order the producer stamps in
  clear text, and an answer from the host's BC-DEVICE on what a correction
  is.
- **Until then, Part A's rule:** an arrival with a held identity, the same
  `message_id` and different bytes, at a row in any state, is set aside as
  *differs*. It is never read as a redelivery. The correction is kept and
  visible, never lost unreported.

**R2. "No row" is acknowledged only on positive evidence.**
- The untargeted conflict makes every unique index an arbiter, so "no
  row" no longer names its cause.
- The message is acknowledged only when the cause is positively known:
  - a holder at its own identity with its own `message_id`: a duplicate;
  - the same with other bytes: set aside as *differs* (R1);
  - a holder of its `message_id` at another identity of its Tenant and
    stream type (R5), read through the index's own expression
    `(tenant_id, stream_type, (metadata->>'message_id')::uuid)` (measured on
    the table-wide form: an Index Scan, 0.03 ms warm; a text comparison scans
    the table): set aside as *identity taken*;
  - a forgotten Tenant (Part B).
- **Anything else is an error, and the source retries.** That covers a
  holder swept between the insert and the reads, and a conflict on an index
  the library does not own.
- **At start, the inbox refuses a table it does not recognise:** a unique
  or exclusion index it does not own, a deferrable unique constraint (which
  raises 55000 on every insert), or a server encoding other than UTF8.

**R3. What can be decided is decided in Rust, before any statement.**
- **The fact.** A host whose `serde_json` has `arbitrary_precision` keeps
  `1e200000` as written, and PostgreSQL raises 22003 on it, a code the
  closed list lacks: the stall, back. Its library tests cannot see it,
  since without the feature the number falls back to a string.
- **The checks in Rust:**
  - a number in the stream id is representable;
  - no NUL in a header's text, and no `\u0000` in a JSON value;
  - each column's byte and character length;
  - a total key under the index row's size;
  - the `message_id` parses as a UUID.

  A failure is *malformed*.
- **The SQLSTATE list stays as a backstop.**
  - 54000 is taken only when the error names the library's own index.
    Otherwise it is the XID stop limit, an environment.
  - The channel is validated when the inbox is built, so that its name
    cannot raise 22001 on every message.
- **Executor:** a differential property test under the host's feature set:
  arbitrary headers, the Rust verdict held against a real insert into
  scratch tables.

**R4. `{table}_rejects` takes exactly what the inbox refused.**
- Every column derived from the message is `bytea`, or text with NUL
  removed and its length bounded. No `jsonb`, `uuid` or `varchar(n)` holds
  the message's text.
- Any index over the identity is over a digest of it.
- The holder of an *identity taken* is recorded as its channel and its
  received position, never as another Tenant's identity.

**R5. A message is admitted only from the service that owns its stream, and
a `message_id` is unique within its stream type.** Revised on 2026-11-07
with the owner (§ Change Log). The first wording, "a channel declares the
stream types it carries", was withdrawn.
- **A stream type has nothing to do with channels.** It is the type of the
  aggregate, the source of the events. Together with the Tenant, the stream
  id and the position it names an event, and causal dependencies are built
  on that identity ([Lloyd et al.], [Bailis et al.], [Schwarz, Mattern]).
  One aggregate publishes to several channels. The identity is therefore
  global and independent of the channel, and must stay so. A row that waits
  on a dependency waits for "event p of aggregate X", whichever channel
  brings it, including across a loop of routes (A→B→C beside A→C). The
  stream type is prefixed with the owning service's name (`device.Device`),
  so no two services' names can collide by accident.
- **The fact: nothing checks a claim.** The intake deduplicates by the
  identity its producer wrote in clear-text headers, and nothing checks
  that the producer may claim it.
  - A producer of one channel bridged into an inbox (defective or
    compromised) can write another service's stream type. Its message is
    stored under that service's identity, and the real message, arriving
    later, is a shadow and dropped. The host's concrete case: a service
    that reads `tenancy.access` sees its identities in clear text, and
    Access Versions are predictable. Once it also produces a Device channel
    bridged into the same inbox, it could hold back revocations. The
    host's Device channels have no producer yet.
  - The same producer can also write another service's `message_id` under
    its own identity, so that the real message conflicts on the
    `message_id`.
- **The fix enforces the convention the names already follow.**
  - The prefix of a stream type names the service that owns the aggregate.
  - The host's topology names the service that produces each channel.
  - The intake admits a message only when the two agree. Anything else is
    set aside as *foreign*.
  - The library takes the admission as a predicate the host gives, derived
    from its topology, never written by hand. It knows nothing of services.
- **A `message_id` is unique within its Tenant and stream type**: the index
  is over `(tenant_id, stream_type, (metadata->>'message_id')::uuid)`.
  - **Why the uniqueness stays.** It is the only guard on the consuming side
    against applying one message twice when it comes back under another
    identity. The producer's outbox keeps a `message_id` unique only within
    a generation of its ring (`outbox/src/pg/store.rs`). A host's check that
    the headers' stream agrees with the sealed payload's (ADR-0086 decision
    7) catches only a disagreement, not a consistent re-stamp: an old event
    published again at a new position with its `event_id`, which a fold by
    positions would apply as new. Kept, such a duplicate is set aside as
    *identity taken* and pages, and is never processed twice. That is fail
    fast on the consuming side, without stopping the channel; only the
    reaction to 23505 that this ADR removes made it a stop.
  - **Why the stream type, not the channel or the URI.** The uniqueness
    serves idempotency, which is a property of the message, not of its
    route. Scoped by the URI, one event arriving on two channels would fall
    into two scopes, and a re-stamped copy on one of them would pass. A
    channel renamed or re-keyed, or an aggregate's events moved to another
    channel, would change what is deduplicated.
  - Under R5's admission a producer can take only its own aggregates'
    `message_id`s, which harms no one else.
  - Own Commands keep the rule: their stream type is the command's, prefixed
    by the service that sends it to itself.
- **Executors:**
  - a PostgreSQL test for each vector: a foreign stream type set aside; a
    `message_id` of another stream type stored; a re-stamped duplicate set
    aside as *identity taken*. Removing the admission, or widening the
    index's scope back to the table, turns them red;
  - a test that a misstamp is repaired: the correction is set aside, the
    misstamped parked row is discarded, which releases its `message_id`
    (Part B), and the correction is readmitted.

**R6. Every row set aside has a bounded life, open rows included.**
- A row lives for its channel's window, or for a host's bound when it has
  no Tenant. After that its bytes go, and a tombstone remains: the digest,
  the reason, the counts, the times and the principal.
- A closed row keeps no bytes.
- **The fact.** Before this ADR a poison stopped the channel, and nothing
  grew. After it, a producer that drops a header sets aside every report it
  sends. On `device.facts` those carry location, which the host's ADR-0086
  decision 7 refuses to keep without bound.
- The forgotten-Tenant check comes before the classification.
- **Executor:** the sweep forgets an open row past its window. The mutation
  that keeps it turns the test red.

**R7. The operator's surface.**
- Every call takes the principal, reads included, and tells the observer,
  so that a host can audit it (the host's ADR-0008 decision 6).
- The metadata projection refuses the data key's fields (`dek`,
  `dek_algorithm`, `dek_bound_to`).
- The read of raw bytes is a capability of its own.
- **Each disposition runs in a transaction of its own**, and its count is
  taken after that commits. On the caller's session the library could not
  see the commit.
- **`readmit_all`** gives each message its own transaction.
- **Readmit repairs only two things:** a fix on the consuming side, or an
  *identity taken* whose holder has gone. Every other reason is a function
  of the bytes, so a flood from a producer is repaired by the producer's
  republication, not by a bulk readmit.
- `seen` counts redeliveries as well, since an outbox batch that fails
  redelivers what it had already handed on. A host pages on an open row,
  never on a repeat.

**R8. A hole, once filled, wakes what waited for it.**
- **The fact.** Expiry clears `waiting_for` and parks the waiting row
  (`store.rs`). A mark wakes only rows that still name the identity. So a
  readmit after a dependent's `max_wait` leaves the dependent parked for
  good.
- **The change.** A row parked by expiry keeps the identity it waited for,
  and a later mark of that identity queues it again.
- **Executor:** a hole, its dependent expires, the hole is readmitted, and
  the dependent is processed without an unpark.

**R9. Logs and the observer.**
- **The rule covers every log site of the inbox and the bus**, not only
  the setting aside.
  - The dispatcher's attempt and park lines print the stream id today, and
    so does the host's shadow log.
  - Errors are formatted by their Display alone. A 23505's DETAIL echoes
    the key's values, so it must never appear.
  - Text handed to an observer as an identifier has CR and LF escaped.
- **The observer's composition.** `forward_to_each!` lists the observer's
  methods one by one, so a new method left out of it is dropped silently
  by a composed observer. Executor: a test that feeds every event through a
  tuple, an `Arc` and a `Vec`.

**R10. The adapters, consumer by consumer.**
- **The inbox's intake** is where a message that can never be stored is
  taken: it sets the message aside and acknowledges it.
- **A plain `Consumer`** has no store. Its decode failure is still logged
  and acknowledged, which is a loss. Its documentation says so, and the
  host's durable channels go through a bridge into the inbox.
- **Kafka.** A subscription replaced while a message is being retried
  loses that message: the driver's fetch position has passed it. A
  re-subscription therefore waits until the retry ends, or is refused.
- **In-memory** gets a delivery task per group. Executors:
  - one group failing for good while more than the queue's capacity is
    published;
  - the library's `a_failing_handler_does_not_stop_delivery` rewritten to
    the new contract;
  - every in-memory suite of the host run with a retry count per topic and
    group, logged at teardown.

**R11. The indexes on the live inbox are built now, while it is small.**
- The migration runner takes ACCESS EXCLUSIVE before the migration's SQL.
  An index build therefore stops every statement on the inbox: every
  replica's dispatch, every channel's intake, the sampler.
- Its time grows with the heap: 9.5 s over 4.3 GB, and about 80–100 s at a
  launch's size (an estimate).
- Two indexes come with this ADR: R5's `message_id` index scoped to the
  Tenant and the stream type, which replaces the table-wide one, and Part
  B's parked index.
  - Both land before the host's Device channels have a producer. The host's
    `tests/release_order.rs` holds that.
  - Each is measured under the bus drill's load on the host's drill
    database.
- ADR-0029 decision 5's concurrent build is owed before any index of a
  later release.

**R12. Releases run side by side.**
- An older replica beside a migrated one keeps the old intake. It still
  stops on a poison until the rollout ends, which bounds that stop.
- What must hold against an old binary is enforced in the database, not in
  a new statement's text. That applies to Part B's block of a forgotten
  Tenant, which needs a trigger on the inbox's table.

### Part A, to be built

It consists of decisions 1 to 5, as R1 to R4, R6, R7 and R9 to R11 revise
them, together with R5 and R8.

**Rejected versus parked.**
- A row set aside was never stored as an inbox row. Its identity could not
  be formed, or PostgreSQL refused its values, or its `message_id` was held.
- A parked row was stored, and its subscriber failed. The intake never
  parses a payload: it is opaque, often sealed bytes. A payload that does
  not open or decode is a failure of processing, and parks.

**The order of the rollout.** The gauge of the rows set aside and its page
work before setting aside is switched on. A quarantine that nobody watches
would hide what the stop used to show.

### Part B, waiting

The parked rows' state by identity, their dispositions by channel, the
supersede and forgetting a Tenant (decision 6) wait for three things:
- R1's order stamped by the producer, and the answer from the host's
  BC-DEVICE;
- the host's BC-TENANCY's off-boarding events on the wire;
- R11's parked index.

Also, the discard of a parked row keeps its identity held, so that a replay
of it is a duplicate, and releases its `message_id` for *identity taken*.
That is what repairs a misstamp: discard the misstamped row, then readmit
its correction.

### The models, strengthened

`Bridge.tla`:
- **New steps:**
  - a set of unstorable messages, with the setting aside guarded by it;
  - an environment hold that disables forwarding, with fairness to
    recovery;
  - the reject's write and the acknowledgement as two steps, with a death
    between them.
- **Properties:**
  - `SetAsideOnlyUnstorable`;
  - every message that is not unstorable, once committed, is processed;
  - every unstorable one, once committed, is set aside;
  - `EndToEndOrder` over the messages stored.
- **Mutations TLC must refute:**
  - an acknowledgement without setting aside;
  - setting aside on an environment failure;
  - an acknowledgement before the reject commits.
- **The trace check** joins a recorded `on_rejected` with its handling.

`Inbox.tla` gains R8's wake from expiry.

### Executors corrected

- **Removed: "a 1 MB payload is set aside".** The payload is `bytea`, and
  it stores.
- **The lock on `_rejects`** needs a `lock_timeout` on the intake's session,
  and asserts that the observer is not told on the failed attempt.
- **The concurrent-duplicate race test** is kept only if its hit rate on the
  mutant (the primary key as the only arbiter) is measured first. The
  sequential *identity taken* test is the executor that can fail.
- **The Redpanda regression asserts, and runs in a tier that runs:**
  - through the fixed intake, no member leaves the group, one row is set
    aside, and the committed offset passes the poison;
  - an environment hold longer than the poll interval loses nothing and
    sets nothing aside.
- **The host's scenario teardowns** assert no open row set aside on the own
  channel, beside `defects() == 0`.

## Consequences

- **A rejected message is a hole in its stream.**
  - A consumer that folds by position sees the next position without it,
    and a snapshot channel heals at the next version.
  - A message that depends on the hole waits, then parks after its
    `max_wait`.
  - The hole is visible, counted and repairable by a readmit.
- **At-least-once from the broker into the inbox or beside it.** A crash
  between the commit and the offset's store gives a repeat, which the primary
  key or the digest absorbs. A rebalance's concurrent redelivery is a
  duplicate, not an error.
- **An environment hold still stops the member's partitions**, until the
  per-partition pause is built. The measurement shows the members recover
  once its cause passes.
- **A flood** (a producer that drops a header for every message) is diverted
  in full. It pages on the first message set aside, and is repaired by the
  producer's republication or a bulk readmit.
- **The formal model changes.** `Bridge.tla`'s `EventuallyProcessed`
  becomes "processed or set aside". `Inbox.tla` separates a message's
  identity from the message to admit a supersede.

## Alternatives Considered

- **Let the adapter skip a permanent error.** Rejected on a fact: a plain
  consumer with a sealing stage would lose a message the stage refuses, and
  nothing would have kept it.
- **A dead-letter topic on the broker.** Rejected on a fact: the own-Command
  lane has no broker by design, and a dead-letter topic would put one back.
  It would also need topology and access lists for every channel, and has no
  operator surface.
- **Skip, and log the broker's coordinates, letting the source keep the
  bytes.** Not general:
  - it fails for channels of bounded retention and for own Commands, whose
    outbox forgets by range;
  - `Message` carries no coordinates.
- **Set the message aside as an inbox row under a synthetic identity.** This
  was the strongest rival. It would reuse the parked listing, the
  dispositions and the forget by Tenant. It was rejected on one fact: an
  unpark re-runs the subscriber, while a readmit must re-run the intake. So
  the two are separate states, and decision 4 gives them one surface.
- **A savepoint around the insert.** It was unnecessary, and two round trips
  dearer for every message.
- **Hold the channel once more than N messages are set aside in a window.**
  Considered and rejected on 2026-11-07, as a fail-fast for a flood.
  - Under a producer's flood (a release that drops a header from every
    message) the bad bytes stay in the topic for good. A hold would end only
    with an offset moved by hand, the silent loss this ADR removes, and it
    would stop the channel for every Tenant again.
  - Under a consumer's regression (a check that wrongly refuses valid
    messages) a hold would avoid holes. But a bulk readmit of the fixed
    intake (R7) and the wake of expired dependents (R8) already repair that.
  - The gain is in a case already covered, and the cost is a second mode of
    the intake with thresholds nothing measures.
- **Drop the inbox's unique `message_id` index.** Considered and rejected on
  2026-11-07. It would remove the 23505 class and the squatting of a
  `message_id` at a stroke, but it is the only guard on the consuming side
  against a consistent re-stamp (R5). R5 keeps it, scoped, and R2 turns its
  conflict from a stop into a row set aside.
- **A channel declares the stream types it carries.** Withdrawn (R5): it
  conflates the aggregate's identity with the transport, and one aggregate
  publishes to several channels.
- **Default to setting aside, and list the codes that hold.** Rejected:
  several codes that are not transient still pass with the environment
  (57014, 55P03, 25006, 42P01, 42501), and diverting a storable message
  breaks its stream.

## Executors

Owed, with the implementation.

- **The review.** The roles attack this synthesis before it is built:
  - distributed systems: order, at-least-once, rebalance;
  - database: the untargeted conflict, the second transaction, the indexes,
    locks;
  - operability: the surface, a flood, paging;
  - security: raw bytes, logs;
  - evolution: the inbox's migration, the adapters' contract, the host's
    tests;
  - verifiability: the models and their mutations.

  Each finding comes with a failure scenario and an executor.
- **Unit tests:**
  - the table of SQLSTATEs and verdicts. Mutation: classifying "not
    transient" as unstorable must go red on 57014, 55P03, 25006 and 42P01;
  - property tests over arbitrary messages: the header parse never panics,
    the framing round-trips, and sanitized text has no NUL and fits its
    column;
  - the in-memory adapter retries in order, holds what follows, and a cancel
    during the pause makes no further call. Mutation: restoring the drop must
    go red.
- **PostgreSQL tests:**
  - for each reason, a poison followed by a good message: the good one is
    processed, one row is set aside, and the observer is told once.
    Mutations: the intake returning an error; the targeted conflict
    restored, on the sequential *identity taken* case;
  - a lock on the inbox's table with a `lock_timeout`: the message is held,
    then stored, and nothing is set aside;
  - a lock on `_rejects`: the message is held, then set aside once;
  - a repeat gives `seen = 2`, and a repeat after a discard stays discarded;
  - adversarial bytes (NUL, a 300-character Tenant, invalid UTF-8, a 1 MB
    payload) are all set aside;
  - readmit by the channel's disposition, and a bulk readmit in first-seen
    order;
  - an outbox row to `inbox://` without a position: the lane moves past it;
  - captured logs carry no stream id, `message_id` or header value;
  - for § 6: the parked count and page held to the index's plan; a resolve
    refused with a forged `uri`; every state reached through the real inbox,
    including a key `12345`; the shared snapshot under REPEATABLE READ; the
    supersede statement never over a processed row; forgetting a Tenant,
    including a publish that raced it.
- **Redpanda** (the host's `tests/poison_drill.rs`, now a regression): through
  the fixed intake, no member leaves, one row is set aside, and the
  partition's committed offset passes the poison. An environment hold longer
  than the poll interval loses nothing and sets nothing aside, and each
  stream's order of arrival equals the order produced.
- **Models:**
  - `Bridge.tla` gains the set-aside step, and a configuration whose mutation
    (acknowledge without setting aside) TLC refutes;
  - the trace check in `inbox/tests/bridge.rs` accepts the new step.

## References

The library's key sources, from which its design grows. Agents working on
the library read them before changing the bus, the outbox or the inbox:

- [Lloyd et al.] "[Don't Settle for Eventual Consistency. Stronger
  properties for low-latency geo-replicated storage](https://queue.acm.org/detail.cfm?id=2610533)"
  ([pdf](https://dl.acm.org/ft_gateway.cfm?id=2610533&ftid=1449165&dwn=1)) by
  Wyatt Lloyd (Facebook), Michael J. Freedman (Princeton University), Michael
  Kaminsky (Intel Labs) and David G. Andersen (Carnegie Mellon University).
- [Bailis et al.] "[Bolt-on Causal Consistency](http://www.bailis.org/papers/bolton-sigmod2013.pdf)"
  by Peter Bailis, Ali Ghodsi, Joseph M. Hellerstein and Ion Stoica (UC
  Berkeley, KTH/Royal Institute of Technology).
- [Schwarz, Mattern] "[Detecting Causal Relationships in Distributed
  Computations: In Search of the Holy Grail](https://disco.ethz.ch/courses/hs08/seminar/papers/mattern4.pdf)"
  by Reinhard Schwarz and Friedemann Mattern.
- [Burckhardt] "[Principles of Eventual Consistency](https://www.microsoft.com/en-us/research/publication/principles-of-eventual-consistency/)"
  ([pdf](https://www.microsoft.com/en-us/research/wp-content/uploads/2016/02/final-printversion-10-5-14.pdf))
  by Sebastian Burckhardt (Microsoft Research).
- [Vernon] "[Reactive Messaging Patterns with the Actor Model: Applications
  and Integration in Scala and Akka](https://kalele.io/books/)" by Vaughn
  Vernon. Chapter 9, Message Endpoints: Transactional Client/Actor.
  Chapter 10, System Management and Infrastructure: Message Metadata/History,
  Message Journal Store.
- [Zakrevsky, Domain Events] "[Domain Events in DDD](https://dckms.github.io/system-architecture/emacsway/it/ddd/tactical-design/domain-model/domain-events/domain-events-in-ddd.html)"
  by Ivan Zakrevsky.
- [Zakrevsky, Message race] "[About the message race in terms of competing
  subscribers](https://dckms.github.io/system-architecture/emacsway/it/integration/asynchronous/message-ordering-in-competing-consumers.html)"
  by Ivan Zakrevsky.

This ADR's own:

- The host's ADR-0086 (§ Owed, S3b-4; § Change Log, 2026-11-07) and its
  `tests/poison_drill.rs`.
- ADR-0028, ADR-0029, ADR-0030, ADR-0033, ADR-0035 of this library.
- PostgreSQL 14: `INSERT … ON CONFLICT` and its arbiter indexes
  (`execIndexing.c`, `ExecCheckIndexConstraints`).
- librdkafka: `max.poll.interval.ms`, and leaving a group.

## Change Log

- 2026-11-07 — Proposed, from two rounds of a blind solver and a
  discriminator and a measurement on Redpanda.
- 2026-11-07 — Revised after the roles' review (distributed systems,
  database, security, evolution with verifiability), and split:
  - **Part A**, the intake and the surface of what it sets aside, to be
    built;
  - **Part B**, the parked rows, waiting.

  The core held. The supersede left the intake's statement, and became Part
  B's, behind an order the producer stamps. "No row" is acknowledged only on
  positive evidence. The checks that can be made are made in Rust. Channels
  declare their stream types, and `message_id` is unique within a channel.
  Rows set aside have a bounded life. The ShareLock stated in decision 6
  was corrected to ACCESS EXCLUSIVE.
- 2026-11-07 — Revised with the owner, in a discussion of the revision
  above.
  - **R5 rewritten.** A stream type is the aggregate's type, part of the
    causal identity and independent of channels. So "a channel declares its
    stream types" and "a `message_id` unique within a channel" were
    withdrawn. The intake now admits a message only from the service its
    stream type's prefix names, as the host's topology has it.
  - **The `message_id` uniqueness is kept,** scoped to the Tenant and the
    stream type. Dropping it was weighed and rejected: it is the only guard
    on the consuming side against a consistent re-stamp.
  - **Considered and rejected:** holding the channel past N messages set
    aside in a window.
  - **Added:**
    - the rollout's order: the gauge and its page before setting aside;
    - the line between a row set aside and a parked row.
  - **The library's key sources added** to § References.
- 2026-11-07 — Accepted by the owner; Part A's implementation begun.
- 2026-11-07 — A1 built, and what building it found.
  - **A NUL in the destination's key** passed the reading in Rust, and only
    the server's backstop (22021) caught it. The destination can come off
    the wire through a bridge's header target, so it is refused in Rust, as
    every other field is. A property test over arbitrary messages found it.
  - **The frame's round trip** moves to A3, where readmit needs to read a
    frame back.
- 2026-11-07 — A4, the inbox's part, built, and what building it found.
  - **The driver logs every value.** `tokio_postgres` logs each statement
    with its parameters at `Debug`: the Tenant, the stream id, the
    `message_id`, every header and the payload's bytes. The library cannot
    stop it, so the rule is the host's: keep the target `tokio_postgres` at
    `Info` or above wherever real data runs. `inbox/tests/logs.rs` holds
    that nothing the driver logs above `Debug` carries a value, the bound
    the rule rests on. Owed in A6: the host's logger holds it.
  - **The library's error shows no outside text.** A database refusal shows
    its SQLSTATE and constraint, without the server's message. A
    subscriber's failure shows that it failed. The text stays reachable
    through `source()`.
  - **The observer is declared from one list,** its trait and its
    compositions together. A method can no longer be left out of a composed
    observer.
  - **Beyond this ADR's reach, owed to their own decisions:**
    - the session crate's `AbortCause` shows the server's primary message,
      which may quote a refused value;
    - the outbox's and the session's observers compose by hand, the gap the
      inbox's had.
- 2026-11-07 — A2 built, from a blind solver and a discriminator, and A4's
  part in the bus.
  - **R10's re-subscription is superseded.** "Waits until the retry ends, or
    is refused" waits for ever on a handler that fails for good, the very
    one a re-subscription replaces, and leaves a cancel followed by a
    subscription losing the message. Two more defects stood behind it: the
    new task started before the old one ended, two calls in flight on one
    partition; and the stop's stored permit made a first call that failed
    after a replacement abandon its message. Now one loop per Kafka
    consumer and per in-memory group delivers, and it owns the message in
    hand; a subscription is the handler the loop calls at each try. A
    replaced or cancelled subscription leaves the message to the next
    handler, tried at once. The loop ends once no handler is attached and
    the consumer is dropped: `Bridge::run` drops its consumer with its
    handler attached.
  - **The in-memory back-pressure is kept.** Producers wait while a group
    taking its messages has `capacity` of them; a group whose head keeps
    failing stops counting, and its backlog grows, reported at each
    doubling. A group dropped with no handler ends, and its name is free.
  - **Measured on Redpanda: a member held past `max.poll.interval.ms`
    while another joins is removed without its assignment marked lost.**
    The driver learns of the revoke only when polled, and the member does
    not poll while it holds the message. So a check of `assignment_lost()`
    had no executor that could fail on it and was taken out. The partition
    moves with the message, the committed offset passes it, and the member
    removed takes it once more, a duplicate. The per-partition pause, so
    that the member stays in the group, remains owed.
  - **The bridge holds a message it can never forward.** Setting it aside
    needs a store, which the bridge lacks; every bridge of a header target
    reads an outbox, which writes every row's destination. A bridge of a
    header target over a broker would owe a channel to set aside in.
  - **A plain consumer's decode failure stays a documented loss** (R10); a
    stage's failure on one holds. Owed in A6: the host's test collector
    puts a sealing stage on a plain consumer.
  - **A panic stays skipped and reported** (decision 3): holding it would
    turn a deterministic panic into the measured stop. Its message is lost,
    seen only in the log.
  - **Logs.** The adapters' and the typed consumers' lines name the channel,
    the group and the count of tries, never the handler's or the decoder's
    text, which may carry the message's values (R9).
  - **Executors**, each with the mutation it catches:
    - in-memory, over paused time: a retry in order (the drop restored), a
      group failing for good past the capacity (holding groups counted),
      producers held by a slow handler (no back-pressure), a cancel in the
      pause (the head lost), a replacement at once (the pause waited out),
      a group's end (it ends with its consumer; it never ends);
    - Kafka, on Redpanda: a retry in order, a replacement while retrying
      and a fresh member past it (the message dropped on replacement), a
      subscription after a cancel, a message never taken going to the next
      member (its offset stored on failure), a member held past the poll
      interval; and the loop ending with the consumer while attached.
- 2026-11-07 — A5's bridge model built.
  - `Bridge.tla` has the set aside, the acknowledgement after it as a step
    of its own, and an environment hold with fairness to the recovery.
    `SetAsideOnlyUnstorable`, `SetAsideNeverStored`, `EventuallySetAside`
    and `EventuallyProcessed` for what the intake can store hold, with
    `EndToEndOrder`, over 1.6 million states.
  - TLC refutes the three mutations: acknowledging without setting aside,
    setting aside on a hold, and acknowledging before the row set aside
    commits.
  - The trace check joins a recorded rejection with the outbox's handling
    that follows it. A recorded run fits; a forged one, handled before its
    rejection, does not.
  - R8's wake from expiry in `Inbox.tla` comes with R8, in A3.
- 2026-11-07 — A3's surface and retention built; R8 comes next, with its
  models.
  - **The surface:** `set_aside_counts`; `set_aside_page`, a keyset page of
    the open rows by channel and reason that gives the metadata keys asked
    for, never the payload, and refuses the data key's fields;
    `set_aside_message`, the kept bytes, a call of its own; `readmit`,
    `readmit_all` and `discard`. Every call names its principal, reads
    included, and is told to the observer (`on_operated`) once it has
    committed. Each disposition runs in transactions of the inbox's own:
    the intake's, then the row's close.
  - **Readmit is a disposition the channel declares**
    (`PgInbox::readmitting`); no channel readmits unless told. A readmit
    that the intake takes closes the row, its bytes gone; one set aside
    again leaves it open, seen once more.
  - **A discard is a tombstone** with its principal and reason; a repeat of
    the same bytes stays discarded, and is counted.
  - **Retention (R6):** the sweep closes an open row past its channel's
    window as expired, its bytes gone, and forgets a closed row a window
    after it closed; a row without a Tenant lives the host's bound
    (`InboxRetention::untenanted`), whatever its channel.
  - **The frame reads back** (`unframe`), held by a round-trip property.
  - The rows set aside gained `disposed_because` and an index of closed
    rows by channel, in their own migration, not yet released.
  - Executors: a PostgreSQL test each for the counts and pages, the bytes,
    readmit after a fix on the consuming side, a bulk readmit, discard and
    retention; seven mutations caught, among them every channel
    readmitting by default.
