# Specifications

TLA+ models of the protocols the crates implement, checked with TLC. A model
describes the participants and their steps — transactions, dispatchers,
workers, crashes, retries — and TLC explores every interleaving on a small
instance, looking for a state that breaks a property. What is checked is the
*protocol*, not the Rust code: the gap between the two is closed by tests, and
later by trace validation, where test runs are checked against the model.

```bash
./verify/tla/check.sh          # needs Java and tla2tools.jar, see the script
```

## Outbox — `outbox/Outbox.tla`

Producers publish inside transactions; a dispatcher per (group, worker)
fetches committed messages of its partition in `(transaction_id, position)`
order, hands each to a subscriber, acknowledges the last of the batch; any of
them crashes at any point, and a subscriber may fail.

PostgreSQL is reduced to five facts: a transaction id is assigned at the first
write and increases; commits happen in any order; a row is visible once its
transaction committed; `pg_snapshot_xmin` is the smallest open id, or the next
one; the position row is locked per dispatcher. The header of the module says
why "visible iff committed" is a sound stand-in for a snapshot under the
visibility rule.

Properties, on three messages, two URIs, one group, two workers, batches of
two and one crash:

| property | kind | meaning |
| --- | --- | --- |
| `DeliveredCommitted` | invariant | a subscriber only ever receives committed messages |
| `NoPassedOver` | invariant | a position never passes a live message of its partition that was not received |
| `OrderPerUri` | invariant | messages of one URI reach a group in `(xid, pos)` order, counting first receipts |
| `EventuallyDelivered` | liveness | every committed message is received by every group |

`OutboxNoRule.cfg` runs the same model with the visibility rule switched off,
fetching by transaction id alone. TLC finds `NoPassedOver` violated in a few
steps: a transaction takes id 1 and stays open, a later one takes id 2 and
commits, the dispatcher delivers and acknowledges 2, then 1 commits behind the
position and is never fetched. That trace is the reason the rule exists
(`crates/outbox/src/pg.rs`), and `check.sh` requires it to appear.

The fairness assumption spells out a known property: `EventuallyDelivered`
needs every transaction to end. One transaction left open stalls every later
message, of every group.

Not modelled: the URI prefix of a selection, several messages in one
transaction, and the arithmetic of `hashtext` — the worker assignment is an
arbitrary function, so the sign bug fixed in the Rust port is out of scope
here and covered by a test instead.

## Inbox — `inbox/Inbox.tla`

Messages arrive under an identity, in some order; a dispatcher takes the
oldest unprocessed row of its partition whose causal dependencies are
processed, runs the subscriber inside its own transaction and marks the row
in that transaction; several dispatchers may serve one partition; any of them
crashes, and a subscriber may fail.

PostgreSQL is reduced to four facts: `ON CONFLICT DO NOTHING` makes an
identity one row however often it arrives; `FOR UPDATE SKIP LOCKED` makes a
row held by an open transaction invisible to the others; the dependency check
reads committed marks; the subscriber's writes and the mark commit together
or not at all. `MCInbox.tla` fixes the instance: three messages, the second
depending on the first, two workers, one of them served by two dispatchers at
once, one crash. Which message lands on which worker is left open, so every
partitioning is checked, including dependencies across workers.

| property | kind | meaning |
| --- | --- | --- |
| `EffectsOnce` | invariant | the subscriber's writes for a message are committed at most once |
| `EffectsIffProcessed` | invariant | ... and exactly when the message is marked: writes and mark are one transaction |
| `CausalOrder` | invariant | a message is processed only after everything it depends on |
| `HeldOnce` | invariant | two dispatchers never hold the same row |
| `EventuallyProcessed` | liveness | a received message whose dependencies are processed is eventually processed |

`EffectsIffProcessed` is the guarantee the inbox exists for, and the one the
Go channel API of the earlier port broke by marking a message on delivery
rather than on completion.

Two more configurations pin the two decisions around dependencies:

- `InboxMissingDep.cfg`: the first message never arrives. The second is
  stepped over for ever, by design for now, and everything else still holds,
  the third message included. A separate waiting channel for such messages is
  planned; until then this is the contract.
- `InboxNoSkip.cfg`: the dispatcher takes only the oldest row, without the
  `OFFSET n` loop that steps over rows whose dependencies are not processed.
  TLC finds the partition blocked: the second message arrives before the
  first, sits at the head, and nothing behind it, its own dependency
  included, is ever taken. `check.sh` requires this violation.

Surfaced, not fixed: a subscriber that never succeeds blocks its partition,
because the oldest eligible row is taken first. The model bounds failures to
state liveness; a dead-letter policy is an open item of the target project.
