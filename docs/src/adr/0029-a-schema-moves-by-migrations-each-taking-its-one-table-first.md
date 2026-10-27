# ADR-0029: A schema moves by migrations, each taking its one table first

## Status

Proposed (2026-10-27), implemented with it. Decided by a blind solver and a
discriminator working from the problem alone, and a synthesis of the two
against the code and PostgreSQL 14.24; every claim about the server below was
measured on scratch databases, and the claim the design rests on (§ Context,
"What holds") was measured a third time by the author before building.

Replaces the `setup` of every PostgreSQL adapter of this library — the outbox,
the inbox, the event store, the DEK store and the KMS; idempotent DDL run at
every process start — with migration sets run by a runner of this library, and
gives the host the same runner for its own schema.

## Context

**The incident.** A host service put its schema in place at every start, as
this library's outbox and inbox do: `CREATE TABLE IF NOT EXISTS`, `CREATE INDEX
IF NOT EXISTS`, `ALTER TABLE … ADD COLUMN IF NOT EXISTS`, in one transaction
under an advisory lock. Replicas start during rolling deploys, crash restarts
and scale-out while others serve. Several processes running that setup beside
ordinary work reported `deadlock detected` — ordinary transactions among the
victims — and stalls of seconds on unrelated inserts.

**What the server does on a schema that is already current.**

| Statement | Lock taken before the `IF [NOT] EXISTS` check | Held |
|---|---|---|
| `CREATE [UNIQUE] INDEX IF NOT EXISTS` | `ShareLock` on the table: blocks every write | to `COMMIT` |
| `ALTER TABLE … ADD COLUMN IF NOT EXISTS` | `AccessExclusiveLock`: blocks every read | to `COMMIT` |
| `CREATE TABLE / SEQUENCE / EXTENSION IF NOT EXISTS` | none on an application table | — |
| a `DO` block guarded by a catalogue check that is false | none | — |

So an idempotent setup is not lock-free when it has nothing to do: it locks
every table it names, in the order it names them, until it commits — the
outbox's setup locked the outbox, the inbox's the inbox, the event store's its
log, a host's up to nine tables. One live writer on one of them stalls the setup, and every reader
queued behind the setup stalls with it; a live transaction that holds a lock
the setup later needs closes a cycle.

**What does not protect live transactions** (measured):

- *`lock_timeout` below `deadlock_timeout`.* Each waiter runs the deadlock check
  once, `deadlock_timeout` after it began waiting. When the live transaction
  began waiting first, it is the victim whenever the setup closes the cycle
  within (`deadlock_timeout − lock_timeout`, `deadlock_timeout`) after it: a
  window `lock_timeout` wide, never empty.
- *One pre-existing table per transaction.* `CREATE INDEX` then `ADD COLUMN` on
  one table, against a live transaction that reads it then writes it: the live
  transaction was aborted at once.

**What holds** (measured in every interleaving tried, and by the author): the
migration's first statement is `LOCK TABLE <target> IN ACCESS EXCLUSIVE MODE`,
and it locks no other pre-existing table. It waits only at that first request,
when it holds nothing live traffic asks for; after it, it asks for nothing that
can wait on live traffic. A live transaction already holding a weaker lock on
the target jumps the queue and commits; a queued request makes only soft edges,
which the server re-orders rather than aborts.

**What a version row cannot do.** A single "schema version" or digest row turns
the current-schema start into a read, but two binaries of different versions —
a rolling deploy, a rollback, a development stand switched between branches —
flip it back and forth, each re-running the whole script with all its locks;
and the change path stays one transaction over many tables. Only a set of
migrations, each in a transaction of its own, makes both safe.

**What the pool allows.** Seedwork ADR-0009 keeps nothing session-scoped
(PgBouncer in transaction pooling; deadpool's `Fast` recycling runs no SQL):
a session advisory lock would leak to the next borrower. Everything here is
transaction-scoped.

## Decision

1. **A ledger per database**, `ascetic_ddd_migrations(component, migration,
   checksum, applied_at)`, primary key `(component, migration)`, created by
   the runner under a fixed advisory transaction lock. A **component** is a
   named, ordered set of migrations owned by one piece of code — a host's
   service, a host's kernel module, one outbox table, one inbox table. A
   migration's identity is its id within its component; its checksum is
   computed over the text the code owns (a template, for a component whose
   table names vary).

2. **The current-schema start is a read.** `to_regclass` of the ledger and one
   `SELECT` of the component's rows, outside any transaction: no lock on an
   application table. The component's known migrations must all be there with
   their checksums; one applied under another text is refused
   (`MigrationError::Changed`) — the database is recreated, never patched by
   hand. A row the code does not know (a newer binary, another branch) is
   tolerated and reported.

3. **A pending migration is one transaction**:
   1. `SET LOCAL lock_timeout`;
   2. `pg_advisory_xact_lock` of the component;
   3. the ledger read again (another runner may have applied it);
   4. the oids of the database's tables, as they stand before it;
   5. `LOCK TABLE <target> IN ACCESS EXCLUSIVE MODE`, when the migration
      names its pre-existing target — written by the runner, not by the
      author, so "strongest first" is structural;
   6. the migration's SQL;
   7. the runner's own locks (`pg_locks` of its backend, an index counted
      as its table): none on a pre-existing table but the target, or the
      migration is refused (`MigrationError::TwoTables`) and rolled back.
      A component's first migration — its baseline, which creates its
      tables — is exempt;
   8. the ledger row; `COMMIT`.

   Pre-existing is the oid snapshot of step 4, never `pg_class.xmin`: `ALTER
   TABLE` rewrites the table's `pg_class` row (measured).

4. **Blocked is retried.** `55P03` (lock not available) and `40P01` (deadlock)
   roll the transaction back; the runner sleeps a jittered backoff outside the
   session and tries again, until a budget is spent
   (`MigrationError::Blocked`, naming who held the target). A migration is
   applied once whatever the number of runners.

5. **No non-transactional migration.** `CREATE INDEX CONCURRENTLY` and the like
   have no variant: they would need a session lock and a connection of their
   own, against ADR-0009. Their trigger is a table large enough that an
   `ACCESS EXCLUSIVE` hold for a plain build is felt; then they get a round of
   their own.

6. **Every table of this library is in a component**: `outbox:<table>`,
   `inbox:<table>`, `event_store:<table>`, `dek:<table>`, and the KMS's three
   tables one component each — `kms_keks:`, `kms_signing:`, `kms_mac:` —
   since two services may share one of them and not the others. A baseline
   is the DDL `setup` ran, its checksum over its template; `migrate`
   replaces `setup`, and `teardown` drops the tables and forgets their rows,
   so that a test's tables and the ledger go together. The DEK store's and
   the KMS's `setup` only created tables, which takes no lock when they
   exist; they move with the rest so that the library has one way a table
   comes into being, and so that their first change of shape is a migration
   rather than an `ALTER` at every start.

## Consequences

- A start against a current schema takes no lock on any application table: a
  replica can start beside live traffic, as often as it likes.
- A schema change takes its table's strongest lock for the length of its
  migration: reads of that table wait that long. At the sizes this library's
  users have today that is milliseconds; a large table is the trigger of
  decision 5.
- A migration is written as a delta, appended; one already applied is never
  edited — its checksum refuses the start. A development database that applied
  an unmerged migration since edited is recreated.
- A migration that must touch two pre-existing tables is two migrations, or is
  refused before it can deadlock anyone.
- A table dropped by hand stays in the ledger, and the next `migrate` reads
  it as applied: a test drops its tables with `teardown`, never by SQL of its
  own.
- A host keeps its own concerns outside the ledger — row-level security
  folded at every start under its own catalogue guards, roles, grants.

## Alternatives Considered

- **A version or digest row and `lock_timeout`.** Rejected: flip-flops between
  two binaries, and leaves the change path one many-table transaction (§
  Context); `lock_timeout` alone does not save a live transaction.
- **A session advisory lock on a connection of its own, and `CREATE INDEX
  CONCURRENTLY`.** Deferred, not rejected (decision 5): needed only for large
  tables, at the price of ADR-0009's guarantee.
- **"One table per transaction" checked statically, by a parser of the SQL.**
  Rejected: it guesses the lock footprint the runner can read in `pg_locks`, and
  it does not stop the single-table cycle of § Context.
- **sqlx migrations, refinery.** Rejected: a second driver stack; no
  lock-timeout retry; no locking between runners documented.

## Executors

`crates/migration/tests/pg.rs`, over `ASCETIC_DDD_TEST_PG_URL`:

- a fresh database applies a component's migrations once; the next start reads
  the ledger and applies nothing;
- the current-schema start completes while another session holds `ACCESS
  EXCLUSIVE` on every one of the component's tables;
- two runners at once apply each migration once;
- a target held by another session is waited for within `lock_timeout`,
  retried, and applied once it is released; held past the budget, the start
  fails `Blocked`, naming the holder;
- a migration applied under another text is refused; a ledger row the code
  does not know is tolerated and reported;
- a migration that locks a second pre-existing table is refused and leaves
  nothing; `ALTER TABLE` and `CREATE INDEX` on its one target are accepted;
- a live transaction that reads the target and then writes it commits while a
  migration of that target runs.

The suites of the outbox, the inbox, the event store, the DEK store and the
KMS run over `migrate` and `teardown`; two `migrate` at once agree; an inbox
asked for another cut is refused; and each crate holds the released checksums
of its migrations, so that an edit of a released one fails its build's tests
(the outbox's, mutated, was caught).

## References

ADR-0009 (the session, nothing session-scoped), ADR-0028 (a failed statement
aborts the scope). PostgreSQL 14 documentation: § 13.3 Explicit Locking, § 20.11
`lock_timeout`, `deadlock_timeout`.

## Change Log

- 2026-10-27 — proposed, with the runner and every table of the library as
  components.
