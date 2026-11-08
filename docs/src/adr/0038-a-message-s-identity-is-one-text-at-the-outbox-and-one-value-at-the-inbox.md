# ADR-0038: A message's identity is one text at the outbox and one value at the inbox

## Status

Accepted (2026-11-08) and built, under the library's gate:
- one round of a blind solver and a discriminator;
- the synthesis attacked by two roles, distributed systems and
  verifiability.

The owner was away and had authorised the library's changes, merges and
pushes for the day. It discharges the owed items of ADR-0037 on the
outbox's guard, the position's spelling and `identity_digest`.

## Context

A message's causal identity is the tenant, the stream's type, the stream's
id and the position. Both ends of the bus key by it, but not in the same
way.

**The inbox keys by value.**
- The stream id is read by `identity::read_stream_id` into a value in the
  domain D (ADR-0037 decision 1).
- The position is read by `str::parse::<i32>`.
- The row's primary key compares `jsonb` values.
- So ` 1` and `1`, `o-1` and `"o-1"`, and `{"b":1,"a":2}` and
  `{"a":2,"b":1}` are one identity each. So are `5`, `05`, `+5`, and `0`
  and `-0`.

**The outbox's guard keys by text.**
- The guard is ADR-0030's unique index over one transaction's messages on a
  channel, which turns a second message under an identity into
  `IdentityTaken`.
- It hashes the headers' text, as `metadata_of` keeps them, and the rebase
  keys rows by the same text.
- So each pair above is two identities to the guard. Two messages of one
  transaction spelled two ways pass it, and the inbox drops the second as a
  repeat of the first (`Shadowed`). That is the loss ADR-0030 exists to
  refuse.

**What the guard took and the inbox would set aside:**
- a stream id outside D;
- a position no `i32` writes;
- a tenant or a stream type past its column;
- an identity past `MAX_IDENTITY_BYTES`;
- a stream named in part (the guard exempts it);
- a header named twice (the outbox's map keeps the last);
- a header named `destination`, which the inbox's relay stamps itself;
- a header named `part`, which the guard counts and the inbox's identity
  does not hold, so two parts pass the guard and the inbox drops one;
- a name of serde_json's own.

**The writer was not canonical.** `write_stream_id` wrote an object with
`Value::to_string`. Under `preserve_order`, which the host builds with, its
members come out in the order they were read. So one value had more than
one text, by build and by history. `identity_digest` shared the root.

## Decision

The outbox refuses at the producer's commit every message whose identity
is not the bus's one spelling of it, or that the inbox would set aside. It
aborts the transaction when it does. So equal text at the guard is equal
value at the inbox.

1. **One text per value: a canonical writer in `bus::identity`.**
   - `write_stream_id` keeps ADR-0037's rule for a top-level string: raw
     unless it would read as JSON.
   - Every other value is written as `canonical_json`: compact, every
     string quoted, an object's members in the order of their keys' UTF-8
     bytes, D's integers in decimal.
   - The order is the writer's, never the map's, so the text is the same
     in every build.
   - The algorithm is DEK's canonical encoding, which is not shared: DEK's
     text is frozen by existing ciphertexts.
   - `is_canonical_stream_id(t)` is `write(read(t)) == t`.
2. **One text per position.** `read_stream_position` parses an `i32` and
   requires it to print back as the text. `+5`, `05`, `-0`, ` 5` are
   refused, at the inbox's intake and at the outbox's commit alike.
3. **One reader of an identity.** `read_identity` takes the four texts and
   returns an `Identity` or the reason it is refused.
   - It checks the widths, the stream id, the position, and the stored
     size against `MAX_IDENTITY_BYTES`. All of these move from the inbox
     into the bus.
   - The inbox's `row_of` calls it, and re-exports the bound.
4. **The outbox refuses at commit, and aborts.**
   - `PgOutbox::publish`, before its INSERT, holds a message that names a
     stream (any of its type, id or position) to all four identity headers.
     Each must be present, as text, readable by `read_identity`, and the
     stream id canonical.
   - It refuses any message carrying `destination`, `part`, or a name of
     serde_json's own.
   - `OutboxProducer`, the only place that sees the wire's headers, refuses
     a header named twice, a header or a key that is not UTF-8.
   - Either refusal makes the server abort the transaction, by a statement
     of fixed text that names nothing of the message. The session
     remembers the abort and refuses the commit (ADR-0028), so a caller that
     ignores the refusal commits nothing of the transaction. Within nested
     scopes the abort reaches the innermost, as `IdentityTaken`'s
     unique violation does.
   - The refusal is the new `Error::Refused { channel, message_id,
     reason }`. It is never transient, and it never carries the refused
     text, which may be personal data or the very value refused.
   - A message with no stream headers stays exempt, as ADR-0030 has it.
5. **One digest per identity.** `identity_digest` hashes
   `Identity::framed`, whose stream id is `canonical_json`. For a string id
   or a sorted object nothing changes; for an object read out of order, in
   a `preserve_order` build, its digest changes once.
6. **No migration.** Every row the outbox admits is text, canonical and
   without `part`, so the guard's text index and the rebase's key are
   exact as they are. The inbox's rows are keyed by value and unchanged.

**The rollout's order:** producers first, then inboxes. An inbox's strict
position would set aside a redelivery a lenient producer stamped.
- Measured read-only on the host's stand on 2026-11-08: three outboxes,
  about 180,000 rows, none with a non-canonical position, none with `part`
  or `destination`.
- The host's producers stamp through the writer and print positions from a
  capped `AggregateVersion`.

## Consequences

- A refusal widens a loss: from one message, which the inbox would set
  aside and page on, to the producing transaction, which is aborted. The
  defect is deterministic and found at its source, and no host producer
  reaches it. An inbox handler's fan-out is parked together with its
  trigger.
- A refused message has already drawn a data key: the sealing stages run
  before the outbox. That is acceptable for a producer's defect.
- A stage's own failure, before the wire, still returns with the
  transaction healthy (§ Owed).
- In a `preserve_order` build, `write_stream_id`'s text of an object read
  out of order changes. Its value does not.

## Rule of Three

1. **A host producer is refused in production.** Checked: every stamping
   site goes through the writer, and positions are capped below `i32::MAX`.
   A host test's hand-written unsorted object id is refused at the pin, as
   it should be.
2. **The writer still depends on the build.** Checked: the order is sorted
   by the writer. The executor runs in the build that can tell, which is
   `verify/inbox-features` (§ Executors).
3. **A refusal leaves the transaction healthy.** Checked: the abort is a
   server-side error the session remembers. The swallow tests ignore the
   refusal and see the commit refused with zero rows. The mutation that
   drops the abort turns them red.

## Alternatives Considered

- **Compare values in the guard's SQL.** Rejected:
  - a raw id is not JSON, so `::jsonb` raises on every raw UUID;
  - D's rules (`-0`, private keys) cannot be written in SQL;
  - rebuilding the index holds `ACCESS EXCLUSIVE`.
- **Normalise the headers instead of refusing.** Rejected:
  - `bound_to` is public, and sealing binds the bound headers' bytes, so
    the outbox would have to know which it may not touch;
  - a normaliser after the stages is a second writer of the writer's text;
  - the tenant and the type have no normal form, so a refusal path is
    needed anyway.
- **Refuse at the relay.** Rejected: the producer's transaction has
  committed by then, so the relay could only hold the row, which is a
  stall, or drop it.
- **An inbox that requires canonical text.** Rejected for the stream id:
  ADR-0037 compares as JSON, and a strict inbox would refuse a producer
  whose serializer formats differently. Kept for the position, whose
  decimal has one universal form.

## Executors

- **The writer's exact text:** golden rows run in both builds.
  - `{"b":2,"aa":1}` is written `{"aa":1,"b":2}`, which a sort by length
    would not give.
  - A part beside an `after` object, escapes, a two-byte key.
- **Canonical texts and positions:** golden rows of each, in both builds.
- **One value, one text:** a property over D's values, each read back from
  a text with every object's members reversed. In `verify/inbox-features`
  that gives a map in another order.
  - Mutation caught: `to_string` restored, red only there, as predicted.
  - `verify/inbox-features` asserts that `preserve_order` is on.
- **One identity, one framed text:** reordered keys frame alike in both
  builds.
- **The intake's property** holds an admitted position to its text.
  Mutation caught: a plain `parse`.
- **The widths** are held from the constants: a two-byte tenant at its
  column is taken, one past it refused.
- **The outbox's refusals:** a PostgreSQL test per ground, each readable
  but not the bus's spelling where the ground is the spelling (`"o-1"`,
  `{"b":1,"a":2}`, ` 1`, `05`, `+5`, a stream in part, a position not
  text, a two-byte tenant past its column, `part`, `destination`, a
  private name).
  - Each is refused, not transient. Ignored, the transaction's earlier
    message does not commit.
  - The bus's spellings and a tenant at its width are taken.
  - The wire's grounds through the producer: a name twice, a header or a
    key not UTF-8.
  - Mutations caught: the abort dropped (both tests), the canonical check
    dropped.

## Owed

- **`part` out of the guard** at the next rebuild of the ring's index,
  with the IDENTITY step's pre-check, then the refusal lifted. Or
  ADR-0034's five-column key in the inbox first.
- **A stage's failure** returns before the wire with the transaction
  healthy: an abort for it needs a hook on `TransactionalWireProducer`.
- **`causal_dependencies` checked at commit**, by moving `dependencies_of`
  into the bus.
- **The relay's `message_of`** reads metadata with `row.get`, which panics
  on a failed read.
- **The host:** a gate over its topology, so that no inbox subscribes to
  two channels carrying one stream type. One identity arriving on two
  channels into one inbox is shadowed by design: the guard is per channel,
  and identity is global (ADR-0036 R5).
- **Size bounds at the outbox:** it stores what the inbox's `Bounds` sets
  aside.

## References

- ADR-0028 (the session's absorbed failures), ADR-0030 (the guard),
  ADR-0034 (`part`), ADR-0036 R5, ADR-0037 decision 1.
- `crates/dek/src/domain/canonical.rs` (the canonical encoding's algorithm).

## Change Log

- 2026-11-08 — Accepted and built: the decisions above, their executors,
  and the revisions of ADR-0030's consequence and ADR-0037 decision 1's
  wording.
