# ADR-0030: A transaction writes one message per identity, and the inbox names who holds one it ignores

## Status

Proposed (2026-10-27), implemented with it. Decided by a blind solver and a
discriminator working from the problem alone, and the author's synthesis of
the two against the code and PostgreSQL 14.24. Every claim about the server
below was measured on scratch schemas by the solver, measured again by the
discriminator, and held by the tests listed under § Executors.

## Context

**The inbox's contract.** The inbox stores a message under its identity —
`(tenant_id, stream_type, stream_id, stream_position)`, the headers ADR-0003
names the `Message`'s — with `INSERT … ON CONFLICT DO NOTHING`. A second
message under an identity already stored is taken for a redelivery of the
first and ignored. Its only trace is a `Received` with no receipt, the same
event a redelivery reports. The identity is also the inbox's order: a
stream's messages run in position order (ADR-0005, ADR-0006).

So the inbox is sound only if a producer never writes two *different*
messages under one identity on one channel. Nothing held producers to that.

**The incident** (in the host project that uses this library). A reaction
to one domain event wrote a Command to its own inbox for each lineage of
each Device the event touched. Each Command's identity was the event plus a
"part" the author chose: the Device alone. Two lineages of one Device would
have made two Commands under one identity. The inbox would have kept one and
dropped the other, and no test, gate or signal would have noticed. The data
shape that triggers it — a second lineage — did not exist yet, which is why
no test could supply it. The author's choice was fixed; the class stayed
open. The host had met the class twice before, in two events of one
publication on one channel, and in a Command keyed by a version that recurs
across transactions.

**What can and cannot collide.** These cannot:

- Two Tenants: the Tenant is in the key.
- Two events of one aggregate: their versions differ.
- A relay's redelivery: it carries the same `message_id`, and dropping it is
  what the identity is for.

What remains is two cases:

- **K, within one transaction:** two different messages under one identity
  on one channel. This is the incident's class.
- **K′, across transactions:** an identity a producer synthesised from
  something other than the event it handles.

Two cases are legitimate reuse, and nothing may forbid them:

- **One identity on two channels.** One fact can travel in two forms: an
  integration event, and a Command derived from it, to two consumers.
- **One identity in two transactions.** A republication re-sends a snapshot
  under its version, with a fresh `message_id`, so that a consumer that
  holds it ignores it.

**What was measured** (PostgreSQL 14.24):

- **Savepoints.** A write in a savepoint that is rolled back frees its
  identity; a released one keeps it. `pg_current_xact_id()` inside a
  savepoint is the top-level id, even when the transaction's first write
  happens there.
- **FIPS.** On a server whose OpenSSL is in FIPS mode, `md5()` raises "could
  not compute MD5 hash". This is reported on several distributions and on a
  managed service. `sha256()` works there.
- **Immutability.** `convert_to` and `textsend` are `STABLE`, so an index
  cannot use them. `decode(replace(t, '\', '\\'), 'escape')` is `IMMUTABLE`
  and yields the same bytes as `convert_to(t, 'UTF8')`.
- **Index entry size.** A text key fails on a stream id of about 2.7 KB
  ("index row size … exceeds btree version 4 maximum 2704"). A 16-byte digest
  kept as a `uuid` costs 41 B per row.
- **Write cost.** The unique index adds +10–13 µs per row server-side. Seen
  from a client, against a round trip of ~200 µs per row, it was
  indistinguishable from noise.
- **No lock waits.** The key leads with the writer's own transaction id, so
  another transaction holding the same identity uncommitted never makes a
  writer wait.
- **Building the index.** 10.7 s per million rows under `ACCESS EXCLUSIVE`
  (ADR-0029). It fails on a history that already broke the rule, and names
  only `(xid, digest)`.
- **The inbox's conflict.** `INSERT … ON CONFLICT DO NOTHING RETURNING`
  joined in one statement with a read of the holder returns no row at all
  when the holder committed while the insert waited on it. The statement's
  snapshot predates that commit. A second statement under `READ COMMITTED`
  sees it.

## Decision

1. **The identity's names belong to the bus.** `ascetic_ddd_bus::identity`
   exports `TENANT_ID`, `STREAM_TYPE`, `STREAM_ID`, `STREAM_POSITION` and
   `HEADERS`. ADR-0003 already calls them the `Message`'s. The inbox reads its
   columns by them, and the outbox's guard is written over them.

2. **A transaction writes one message per identity on a channel.** The
   outbox component's second migration adds a unique index on
   `(transaction_id, digest)`:
   - **Channel.** The destination without its key (`uri::without_key`), so
     two keys of a topic count as one channel.
   - **Identity.** The four identity fields of `metadata` as one JSON array,
     so that no two spellings run together.
   - **Digest.** SHA-256 over the text's UTF-8 bytes, its first sixteen bytes
     kept as a `uuid`.
   - **Exemption.** A row missing any of the four fields is exempt (`NULL`).

   Before the index, the migration names every identity a transaction already
   wrote twice — the identity, the channel, the transaction and both message
   ids — and raises with them. A unique index would refuse to build over them
   too, but would name only a digest.

3. **The refusal is typed and final.** `publish` maps the index's 23505 to
   `Error::IdentityTaken(Identity)`, named from the message being written. The
   holder's row cannot be read: the transaction is already aborted. The error
   is not transient.

   Aborting is the point. A refusal by `ON CONFLICT` would leave the
   transaction able to commit what it wrote before, if a caller swallowed the
   error. The raised error keeps the transaction failing closed under
   ADR-0028.

4. **The inbox names who holds an identity it ignores.** `Received` gains
   `held_by`: the stored row's `message_id` when the message was ignored. It
   is read by a second statement, on the conflict path only, in the same
   transaction (§ Context, the inbox's conflict).
   - Equal to the message's own id: a redelivery.
   - Another id: a second message under one identity. On a channel whose
     producers never reuse an identity, this is a defect, a K′ at the moment
     of its drop. On one that republishes, it is a repeat.

   The inbox decides nothing about which it is: the host's observer knows its
   channels.

## Consequences

- An incident of class K cannot commit. The writing reaction is refused, and
  the workflow rolls back whole:
  - a cause carried by the inbox is parked at once, its error naming the
    identity;
  - a request is refused.
- Recovery is a fix of the identity, then the operation again; nothing
  partial was written.
- The rule authors write to: **the messages a cause writes of one kind on
  one channel are a set keyed by their identity.** A repeat inside one
  transaction is an error, not a deduplication. At the identity level, a
  repeat and the incident cannot be told apart: the payload is sealed and
  outside the identity.
- K′ — a producer naming an identity it does not own — is not prevented. It
  is reported at the moment of its drop, with both ids, to an observer that
  knows the channel.
- Each outbox row costs ~41 B more of index, and its write ~10 µs more on
  the server.
- The index is built once per table under `ACCESS EXCLUSIVE`: about 11 s per
  million rows. An outbox whose history already broke the rule refuses the
  migration and names what it lost. The partial index
  `WHERE transaction_id >= <the migrating xid>` exempts that history if it
  must be kept (measured); building without the lock is ADR-0029's deferred
  decision 5.
- A text spelling the inbox's JSON equality treats as one id — reordered
  keys, `12` and `12.0` — passes the guard. That needs two writers of one
  stream type on one channel within one transaction. On a channel where it
  matters, the receipt observer compares in the inbox's own JSON.

## Alternatives Considered

- **Identities unique by construction** (the part a command id, a payload
  digest, an ordinal). Rejected: they dissolve the streams authors group
  messages into on purpose, for order.
- **The inbox refuses or parks a second message.**
  - Refusing: the relay acks only after the target accepts, so a refusal
    stalls the outbox's slot, which other channels share.
  - Parking: it changes the receive action of `verify/tla/Inbox.tla`, whose
    trace checker would reject the new step.
  - Either way, it would fire on legitimate republication.

  Deferred instead, with its trigger: a defect reported in production by
  decision 4.
- **The inbox keyed by `message_id`.** Rejected: it breaks republication's
  deduplication and `CausalDependency`, which names a row by its identity.
- **A registry of the transaction's writes in the session, or a query before
  each insert.**
  - The registry: hand-written savepoint semantics, which the index gets
    from the server.
  - The query: O(n²) over a fan-out.
  - Both cover only the writers that go through them.
- **A check deferred to commit.** Rejected: the inbox's dispatcher releases
  the subscriber's savepoint before it commits. A failure at commit fails
  the dispatcher's transaction rather than recording an attempt, so the
  message would be retried while it holds its partition.
- **The guard without the channel, or across transactions.** Rejected: it
  forbids one fact in two forms, and republication.
- **`md5`.** Rejected: it raises on a FIPS-mode server, where every write
  would be refused.
- **A reconciliation of outbox against inbox, swept.**
  - It needs a watermark trailing the relay's acknowledged position, or a
    drop is missed for good.
  - It reads two layouts from the host's code, and joins the two hottest
    tables periodically.

  Kept only as an audit query.

## Executors

`crates/outbox/tests/pg.rs` and `crates/inbox/tests/pg.rs`, over
`ASCETIC_DDD_TEST_PG_URL`:

- Two messages under one identity on one channel, two keys of one topic, in
  one transaction: refused `IdentityTaken`, naming the channel and the
  identity. Nothing commits. Red without the migration.
- Stored, not refused:
  - one identity on two channels;
  - one identity in two transactions;
  - messages without an identity;
  - a write after the same identity's savepoint was rolled back.

  After a released savepoint, the write is refused.
- A history that broke the rule: the migration names the channel, the stream
  type and both ids. Red without the pre-check, as a bare digest.
- The released migrations keep their checksums (ADR-0029).
- The guard's digest is `sha256`, never `md5` (a unit test of the migration's
  text: the FIPS decision's executor).
- `held_by`:
  - is `None` for a stored message, the message's own id for a redelivery,
    and the first's for a second message under the identity;
  - names the holder that committed while the insert waited on it.

  Red with the holder unread.

## References

ADR-0003 (the outbox and the inbox as channels; the identity in the
`Message`), ADR-0005 and ADR-0006 (the inbox's order and dependencies),
ADR-0028 (an aborted scope is never committed), ADR-0029 (migrations, each
taking its one table first). PostgreSQL 14 documentation: § 9.4 `decode`,
§ 9.5 `sha256`, § 11.6 unique indexes, § 13.2.1 `READ COMMITTED`.

## Change Log

- 2026-10-27 — proposed, with the outbox's guard, the inbox's `held_by` and
  the identity's names in the bus.
