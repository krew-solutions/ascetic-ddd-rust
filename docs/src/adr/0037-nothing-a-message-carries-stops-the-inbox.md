# ADR-0037: Nothing a message carries stops the inbox

## Status

Accepted (2026-11-08, by the owner); being built in the order of
§ Slices.

It is the output of one round of a blind solver and a discriminator, whose
synthesis was then attacked by five roles: distributed systems, database,
evolution, security and verifiability. The round was run for the host
project, after its executor of ADR-0036 R3 found the defect of § Context
before it was written.

It revises ADR-0036:
- R3, its rule on numbers and its executor;
- the claim of R4 and of `channel.rs` that structured headers come back as
  they left;
- decision 3, whose premise "only the environment holds the source" this
  ADR extends from the intake to the dispatcher, the reader of the rows and
  the broker's adapter.

ADR-0036's Part A stays; its Part B waits as it did.

## Context

**The defect measured.** The host builds this library with `serde_json`'s
`arbitrary_precision` and `preserve_order`, by Cargo's unification of
features; the library's own tests build without them. On 2026-11-08, in the
host's build:
- `PgInbox::intake` of a message whose `stream_id` header is `1e-16384`
  returned `Err(22003, "value overflows numeric format")`;
- so did a message with an ordinary stream id and a header `[1e-16384]`;
- `1e131072` was set aside as malformed, and `0.5` stored.

22003 is not on `unstorable`'s list, so the intake returned an error and the
bridge held the channel: the stall ADR-0036 exists to end.

**Why R3's rule lets it through.**
- The intake reads a `stream_id` header as JSON when it parses, and as the
  header's text otherwise. `1e-16384` without quotes is a JSON number.
- R3 holds a number to a finite `f64`, "a bound tighter than numeric's".
  It is not tighter. `1e-16384` is `0.0` in `f64`, which is finite, and
  PostgreSQL's `numeric` takes at most 16383 digits after the point.
- It is not looser in the right way either: `1e309` is refused, though
  PostgreSQL stores up to `1e131071`.
- Measured on PostgreSQL 14.24, the server the host runs: `jsonb` takes
  `1e-16383`, `1e131071`, `0e200000`; it refuses `1e-16384`, `1e131072`, and
  `0.` followed by 16400 zeros and a `1`, each with 22003. The full rule: the
  exponent within ±1073741822; the scale, `max(0, f − e)` counted on the
  text, at most 16383; a nonzero value's leading digit at most 131071 places
  before the point.

**What the round found beside it.** Each is a message's own value that stops
the intake, every dispatch loop, or the process. Each was confirmed by the
discriminator or a role against the code at aa48a12, and measured where a
SELECT could:
1. **A 127-deep JSON header.**
   - `structured` keeps it, and the metadata object makes it 128 deep.
   - `postgres-types` reads `jsonb` back through `serde_json::from_slice`,
     whose limit is 128 nested values; `Row::get` panics in `message_of`.
   - The panic is in `head_of`, outside any handler, and nothing restarts
     the task: the consumer dies in every process that reaches the row.
     The trigger is a header of 254 bytes.
2. **A serde_json token.**
   - Under `arbitrary_precision`, an object whose first key is
     `$serde_json::private::Number` parses as a number.
   - `{"$serde_json::private::Number":"1e-16384"}` is the same stall in
     another spelling.
   - PostgreSQL sorts a `jsonb` object's keys by length, so a token placed
     second comes back first and fails the read (`TrailingComma`): a
     70-byte header that panics the dispatcher.
3. **A causal dependency too large for `__waiting_idx`.**
   - Dependencies are not bounded at the intake. At dispatch,
     `set_waiting_unless_processed` writes `waiting_for` into the btree and
     gets 54000. That is not transient, so every loop stops.
   - The slot's rotation rolls back with the transaction, so the same slot
     comes first again at every poll.
   - At the largest key the intake admits, the waiting datum is 2692 bytes,
     a tuple of exactly the btree's 2704.
4. **A NUL in a subscriber's failure.**
   - The host wraps a decode error's text in `Permanent`. serde quotes the
     value it refused (`unknown variant`), raw.
   - `record_failure` binds it, and PostgreSQL refuses it with 22021, so
     every loop stops on every poll.
   - The same text carries plaintext fragments of a sealed payload into
     `last_error`, where crypto-shredding the Tenant's key does not reach.
5. **Amplification on read.**
   - `jsonb` keeps `1e-16383` in 22 bytes and renders it in 16,387.
   - Measured: a header of 18 KB, holding 2,000 such numbers, renders in
     32,774,007 bytes and takes 42 s of the server's CPU per read. Past
     1 GB the read fails inside the statement that locates the head.
   - The host's build admits it today.
6. **Results without a bound.**
   - `expire_waiting` has no limit and returns every column, payload
     included. So do the mark's `woken`, `resolve_row` and `parked_rows`.
   - K rows waiting on a dependency that never arrives come back in one
     statement once their wait runs out, and an OOM kills the process. The
     next process takes the same rows.
7. **A header name that is not UTF-8.**
   - rdkafka 0.39 reads a header's name with `to_str().unwrap()`, outside
     the adapter's `catch_unwind`.
   - The delivery task panics, is never restarted, and does not store the
     offset. The next member of the group panics on the same record.
8. **A message without a size bound.**
   - The broker bounds the compressed batch. librdkafka decompresses up to
     `receive.message.max.bytes`, 100 MB by default, which the adapter
     leaves as it is.
   - The intake parses the header into a `Value` before any length check.
     Under the host's features that is 40 to 50 times the text in memory
     (derived from the types, not measured).
9. **Counters on `integer`.**
   - `seen + 1` raises 22003 at the 2³¹st repeat of a row set aside.
   - `attempts + 1` does the same under the library's default of unlimited
     retries.

**And beside them, not stalls:**
- **One identity, two slots.**
  - `jsonb` compares numbers by value and prints them with their scale:
    `'1.0' = '1'` is true, their `::text` differs, and so does `hashtext`.
  - So `1` and `1.0` are one primary key but two slots under `ByStream`,
    and two advisory locks. The wait and the mark take different locks.
  - `1e5` and `100000` are one key: the second stream's message is
    acknowledged as a shadow.
- **One message, two identities by build.**
  - Without `arbitrary_precision`, `1e-400` becomes `0.0`, which is stream
    `0`, and `1e400` becomes a string.
  - With the feature, the text is kept.
  - So the same message has one identity in one host's build and another
    in a build with other features.
- **The dialect is guessed.** The inbox does not know whether a header is
  JSON text or raw text; it tries the parse.
  - `bus/identity.rs` says "the stream's id, as JSON text"; `channel.rs`
    says "or a string when it is not JSON".
  - So a raw id that looks like JSON changes type. The host stamps
    `DeviceKey`, any non-empty text, raw: a Device keyed `12345` is the
    number 12345 today, and one keyed `1.5` would be refused by a rule that
    refuses non-integers.

## Decision

The rule this ADR adds to ADR-0036's: **nothing a message carries can stop
the intake, a dispatch loop or the process.** A message's own value is
decided in Rust before it reaches PostgreSQL. What Rust did not foresee and
PostgreSQL refuses for the value's sake is set aside at the intake, and
parked at dispatch, never held. A statement whose size the message decides
is bounded.

### 1. A stream's id is JSON text, read by one function

- **The contract.** The `stream_id` header carries the stream's id as JSON
  text. A producer that means a string writes it quoted.
- **The function.** `crates/bus/src/identity.rs` owns it, beside the
  contract it documents. It is used for the row's `stream_id` and for every
  causal dependency's.
- **One pass over the text, before any `Value` is built.** The library's own
  scanner of RFC 8259 reads the text and builds the value. It does not go
  through serde_json, whose readings of numbers, of `-0` and of its private
  tokens differ by feature, so the verdict and the value are the same in
  every build. The text is the identity's domain D when:
  - every number is an integer written without fraction or exponent, not
    `-0`, within `[i64::MIN, u64::MAX]`. It is built as an `i64` or `u64`,
    so `jsonb`'s equality, `::text` and hash agree on it;
  - no string or key holds U+0000 or a lone surrogate;
  - no key, once unescaped, begins with `$serde_json::private::`;
  - nesting is at most 32 deep;
  - the value's stored size is within decision 4's bound.
- **Text that is not JSON** by the grammar, a raw UUID for instance, is the
  string of that text. The host stamps raw ids today, and a text that is not
  JSON can mean nothing else.
- **JSON outside D is refused**: the intake sets the message aside as
  malformed. Any failure after the scanner accepted the text is a refusal,
  never the fallback to a string.
- **The writer is the inverse, for the stream id alone.** A string whose
  text the scanner would read as JSON is written quoted, so that
  `read(write(v)) == v` for every v in D. Metadata headers are written as
  they are (decision 2).
- **The host stamps JSON text before it takes the library that refuses.**
  For a UUID or an object of strings that changes no identity: a raw UUID
  and a quoted one read as the same string. For a raw id that reads as JSON
  (digits, `true`, `null`), quoting is a change of identity, which decision
  8's census finds before the change.

### 2. Metadata is the headers' text

- Every header that is not a column is kept as its text, a `jsonb` string.
  It is never structured.
- Nothing reads the structure:
  - `causal_dependencies()` reads a string list;
  - `message_id` is a string;
  - SQL reads only `->>'message_id'`;
  - the operator's surface reads the frame.
- So a handler gets the producer's bytes. Numbers, depth, tokens and the
  amplification leave metadata at once, and `channel.rs`'s claim becomes
  true.
- Rows stored structured stay readable. `causal_dependencies()` and
  `wire_of` read both forms; the dispatcher reads them through decision 6's
  tolerant read.
- A message that carries one header name twice is malformed. `header`
  returns the first occurrence and the metadata map kept the last, so a
  dependency list checked at the intake could differ from the one dispatch
  reads.

### 3. A message's size is bounded before it is read

- **The intake** bounds the payload, each header's text, the number of
  headers and their total, before any parse or clone. A message past a
  bound is set aside with a bounded frame: its headers' names and sizes and
  a prefix of its bytes.
- **The Kafka adapter** sets `receive.message.max.bytes` and
  `fetch.max.bytes` to the same bounds.
  - Its executor measures what librdkafka does with a record past the cap.
    A record fetched again for ever would itself be a stall; it is then
    taken as a message the intake sets aside.
- **The adapter reads a header's name as bytes**, through
  `rd_kafka_header_get_all` and `CStr::to_bytes`.
  - A name that is not UTF-8, or that the C string cut at a NUL, reaches
    the intake, which sets the message aside.
  - A `catch_unwind` around building the message keeps the task alive if a
    driver panics in another place.

### 4. A causal dependency is read by one validating parser, at both ends

- **The intake and the dispatcher read the list the same way.** It must
  parse, and every entry must be a dependency:
  - the tenant and the stream type within their widths, without NUL;
  - the stream id read by decision 1;
  - the position an integer.
- **The list has at most 64 entries.**
  - `first_unprocessed_dependency` looks each one up in turn, and starts
    again after every wake.
  - N dependencies arriving one by one cost N²/2 statements on one slot.
- **One size bound, for the row's key and for a dependency's datum.** It is
  computed in Rust on the stored form of the `waiting_for` datum:
  - the varlena, container and entry headers;
  - strings as raw bytes;
  - each number at its largest size, padding included;
  - the position at the largest `i32`, never the row's own position.

  It replaces `KEY_BYTES`, which counts text. The primary key's tuple is
  always smaller than the waiting tuple, so every stream the intake admits
  can be depended on. `migrate` refuses a block size other than 8192, on
  which the bound rests.
- **What fails.** A failure at the intake is malformed. A failure at
  dispatch, on a row stored before, parks the row with its reason
  (decision 6).

### 5. The backstop, and the table's catalogue

- **22003 joins `unstorable`'s list** for the intake's statements: the
  INSERT, and the holder's and the identity's SELECTs.
  - Their only number parses are the message's `jsonb` values.
  - Their other conversions are already on the list or checked in Rust:
    the sequence raises 2200H, the slot is masked and clamped, the `uuid`
    casts raise 22P02.
- **`migrate` compares the table's catalogue with the library's own.**
  - What it compares: indexes, constraints, defaults, generated
    expressions, triggers, policies, rules and the row-security flag.
  - The library's objects carry the `{table}__` prefix. A newer release's
    objects under that prefix are accepted, so an older replica still
    starts beside a newer one (R12).
  - Any other object is refused at `migrate`: a host's index or trigger
    would change what the intake's statements can raise.

### 6. The dispatcher is total over the rows it reads

- **Locate, then read.** The head is located by a statement that returns
  only its `received_position` and `ctid`, under `FOR UPDATE`. Its values
  are read in a savepoint.
  - A read that fails, in the client or on the server, is then a failure of
    one known row.
  - A plain SELECT assigns no transaction id, so a 54000 there cannot be
    the server's transaction-id limit.
- **Every statement that binds or renders a row's values runs in a
  savepoint**: the read, the wait, the failure's record and the mark.
  - **Its failure on the dispatch's closed list** parks the row in the same
    transaction, while the slot's lease and the row's lock are held, by
    `ctid` and the queue's predicate, changing exactly one row, with a
    static reason. The list: 54000 naming the inbox's own index, 22021,
    22P05, 22003, 22001, 22P02, and a value the client cannot decode.
  - **Any other code is the environment's,** as today. A statement timeout,
    a lock timeout or a cancel never parks a healthy row.
- **The slot's rotation survives a rolled-back dispatch.** It is moved by a
  statement of its own, so one head cannot keep its slot first.
- **Every statement that returns rows returns a bounded batch, narrow and
  tolerant.**
  - Bounded: the expiry, the woken, the parked listing and the resolve.
  - Narrow: only the head's own read carries the payload.
  - Tolerant: a row that does not decode comes back as unreadable, by its
    position and slot, and the batch goes on.
- **A row parked as unreadable or unwaitable is superseded by any arrival
  at its identity**, whatever its `message_id` and bytes. The repaired
  message's mark then wakes the row's dependents.
- **The operator's unpark, resolve and parked listing work by received
  position as well**, so one unreadable row does not close the surface for
  every Tenant.
- **A dependent's wait is bounded by default.** `max_wait` defaults to one
  hour instead of none, so a dependency that is refused, or was set aside,
  is paged and never waited for silently.

### 7. What the inbox writes of its own is storable

- `seen` and `attempts` saturate instead of overflowing. Saturating needs no
  DDL; moving to `bigint` would rewrite the rejects table under ACCESS
  EXCLUSIVE.
- **A failure's text** loses its NULs and is bounded by characters, never
  cut inside one, since a panic in dispatch kills its task. The library's
  documentation says `last_error` is stored unsealed. The host's decoders
  name a field and a category, never the value they refused.
- **A repeat of a message set aside writes the current reason and
  detail**, keeping `first_seen_at`. A release that judges the same bytes
  otherwise then refreshes its rows.

### 8. The rollout

- **A census, in Rust, by the library's own functions**, over every inbox's
  rows. It counts by ground:
  - stream ids decision 1 would refuse, including every number in a stream
    id;
  - dependencies decision 4 would refuse;
  - metadata with numbers, tokens or depth;
  - waiting data past the bound.

  It reads without rendering the amplifying values: SQL path predicates
  select rows, and Rust reads only those.
- **The producer's side**, the only executor of "no stream outside D is
  minted": the host has a test that feeds every site that stamps a stream
  id through decision 1.
- **What the census cannot see.** `jsonb` has lost the producer's spelling
  (`1e5` is stored as `100000`). So the page on a row set aside (ADR-0036
  decision 4) is the detector of a producer missed.
- **Order.** The host quotes its stream ids, runs the census on its
  inboxes, and takes the library at a pin that carries decisions 1 to 7
  together. Before the census reads zero, rows the dispatcher would park
  are resolved by hand.

## Rule of Three

How this decision fails, each checked:
1. **A producer's ids leave D, and its stream is set aside from then on.**
   Checked: the host stamps UUIDs, objects of strings, and canonical JSON
   that refuses non-integers. Its `DeviceKey` is any text, stamped raw;
   quoting it (decision 1) keeps it a string. The page shows any producer
   missed, the first message it sends.
2. **The scanner and PostgreSQL disagree.** Checked: D holds only integers
   within 64 bits, which `numeric` stores and renders exactly (measured on
   u64::MAX and i64::MIN). The scanner refuses where the grammar is unsure.
   The differential executor holds it on the server.
3. **A park takes a healthy row, or a parked row cannot leave.**
   - Checked: the park's codes are a closed list per statement; timeouts
     and cancels stay the environment's.
   - The park changes exactly one row, under the locks the dispatch holds.
   - An unreadable parked row is superseded by any arrival at its identity
     and is listed by its position.
4. **An older replica beside a newer one.**
   - An older intake still structures metadata and still stalls on a
     poison; R12 accepts that for the rollout.
   - The catalogue check accepts a newer release's objects by their prefix.
   - Rows stored structured stay readable by both.

## Alternatives Considered

- **A stream id outside D stored as the header's text** (the blind solver).
  - It is available and the same in both builds.
  - It moves the boundary of the dialect's guess without ending it: a raw
    `12345` still becomes a number.
  - It writes a third reading rule into live identities, which dependents
    and the outbox guard would replicate, for no producer that needs it.
  - Rejected for refusal, which pages at the stream's first message, and
    for the host stamping JSON text.
- **Canonicalising numbers** (`1e5` to `100000`). The build without the
  feature has already lost the text, and it would merge an id `3e8` with
  `300000000`. Rejected.
- **Keeping every number PostgreSQL stores.** The verdict would differ by
  build, and amplification would stay. Rejected.
- **The library enabling `arbitrary_precision` itself.** It would change
  serde_json for every binary that uses the library. Rejected; the scanner
  is independent of the feature.
- **Metadata structured, with the checks of decision 1 applied to it.**
  Nothing reads the structure, and every check would be a way to refuse a
  header for no reader. Rejected for text.
- **Parking on any error at dispatch.** A statement timeout or an
  operator's cancel would park healthy rows en masse. Rejected for a closed
  list per statement.
- **A shadow phase:** the new refusals counted, not applied, until a
  retention window shows none. Weighed. The library has one consumer, which
  is not deployed to a Tenant; the census, the producer's test and the page
  cover it. Rejected for this release; a library with deployed consumers
  would need it.
- **Migrating `seen` and `attempts` to `bigint`.** It rewrites the rejects
  table under ACCESS EXCLUSIVE; saturating needs no DDL. Rejected.

## Executors

**A package outside the workspace**, `verify/inbox-features/`, with its own
`[workspace]` and lock.
- It depends on `ascetic-ddd-bus` and `ascetic-ddd-inbox` by path, with
  `serde_json`'s `arbitrary_precision` and `preserve_order`, as the host
  builds them. A second job adds `raw_value`.
- The workspace's tiers run `--all-features`, so a feature inside the
  workspace would turn every crate's tests into that build and lose the
  default one. Its target joins `make all`.

**Decision 1:**
- **A golden table of header text against its identity, or a refusal,** run
  in the default build and in the package's. Its rows:
  - `12345`, `0`, `-0`, `[-0]`, `{"a":-0}`;
  - u64::MAX and one past it; i64::MIN and one past it;
  - `1e5`, `1E5`, `1.0`, `0.5`, `1e-16384`, `1e131072`, `1e400`, `[1e400]`;
  - `{"$serde_json::private::Number":"5"}`, the token placed second, and
    the token escaped;
  - depths 32, 33, 129 and 10,000;
  - `"\ud800"`;
  - `o-1`, `"o-1"`, `"12345"`, `" 1"`.

  Mutations it catches:
  - the `-0` rule dropped: red in the package's build only;
  - a token checked on the parsed `Value`: red in the package's build,
    whose row carries an integer inside the token;
  - the f64 rule restored;
  - the depth bound off by one;
  - a parse failure that falls back to a string.
- **A round trip, `read(write(v)) == v`,** over values in D, with
  generators of strings whose text is JSON. Mutation caught: the writer
  quoting by a heuristic.
- **A pairwise oracle on the server.** Two identities that `jsonb` finds
  equal are equal in Rust, and their `::text` is equal. Mutation caught: a
  rule admitting `1.0` beside `1`.

**Decisions 2 and 3:**
- **A header's bytes survive the inbox.** A property through PostgreSQL that
  `wire_of` of a read row returns every header that is not a column, byte
  for byte. It runs over texts `3`, `true`, `[1]`, `"x"`, `{"b":2,"aa":1}`
  and duplicate keys. Mutation caught: `structured` restored, or the stream
  id's quoting applied to metadata.
- **A row inserted by SQL with a structured dependency list** waits and is
  woken. Mutation caught: the structured arm of `causal_dependencies()`
  dropped.
- **The bounds:**
  - a message past each bound is set aside with a bounded frame;
  - a duplicate header name is malformed;
  - a Kafka record whose header name is `\xff` is set aside on the stand's
    Redpanda, and the next record is delivered;
  - a record past the fetch cap does not hold the partition.

**Decision 4:**
- **Each ground is malformed at the intake:** a list that does not parse,
  an entry that is not a dependency, a NUL in a tenant, 65 entries, and an
  identity past the bound.
- **The largest identity the intake admits is set waiting and woken**, once
  for each shape at its own maximum:
  - a random alphanumeric string;
  - an integer-dense composite;
  - multi-byte strings;
  - a tenant and a stream type of 128 characters.

  The fillers are incompressible, since the index compresses values over
  about 510 bytes. Mutation run once and recorded: the bound loosened by
  8 bytes turns it red.
- **Every entry of an admitted list is a dependency at dispatch**: the
  counts are equal.

**Decision 5:**
- `only_the_listed_codes_are_the_message_s_own` inverted for 22003.
- A test of the catalogue:
  - `migrate` passes on a table carrying a newer `{table}__x` trigger, a
    `{table}__x` unique index and an unknown ledger row;
  - it refuses a host's trigger, index, rule or policy.

**Decision 6:**
- **One test per park ground,** each a row inserted by SQL with
  `concurrency = 1`:
  - a 128-deep metadata;
  - a token placed second;
  - an oversized incompressible dependency;
  - in the default build, a stream id that reads back lossy
    (`'12345678901234567890123'::jsonb`), so that a statement binding the
    row's identity changes no row;
  - amplifying numbers.

  Each asserts that the row is parked with its reason and that the next row
  of its slot is processed. Mutation caught: a park that binds the row's
  values.
- **A NUL failure** is recorded, sanitised, `attempts` = 1, not parked.
  Mutation caught: binding the raw text.
- **`parked()` over an unreadable row** lists it by position, and a
  republication at its identity supersedes it and wakes its expired
  dependent.
- **A test with batch + 1 rows waiting** asserts how many rows each
  statement returns.
- **Inbox.tla gains `ParkAtOnce`:** a row parked on its first take for its
  own value, under the row's lock.
  - `ParkedIsAside` admits it.
  - A mutant configuration that drops the queue's predicate from the park
    must fail.

**Decision 7:**
- `seen` and `attempts` set to 2147483647 by SQL; a repeat and a failure
  then succeed.
- A repeat after the verdict changed shows the current reason.

**Decision 8:**
- The census is a library function with a test over seeded rows of every
  ground.
- The host's test of its stamping sites, and its run of the census on the
  stand before the pin.

**Tests and documents this decision contradicts, rewritten with it:**
- `intake.rs`'s `1e999999` read as a string, and its `[1e999]` hostile
  case;
- the 22003 row of the codes' test;
- the property's `jsonb_takes` oracle, which ignores numbers;
- `KEY_BYTES` and its doc;
- the f64 rule's doc;
- `channel.rs`'s "come back as they left";
- `message.rs`'s "entries that are not dependencies are ignored";
- ADR-0036 R3's executor line.

## Owed, outside this decision

- **`stream_position` read by `str::parse::<i32>`**, which takes `+5`,
  `05` and `-0`: two texts, one identity in the inbox and two in the
  outbox's guard. The header's text must print back as the value.
- **`identity_digest` hashes Rust's text,** which depends on key order and
  on `preserve_order`. It has no reader yet, so it is latent.
- **The outbox producer refusing a stream id outside D at its commit**,
  and its guard comparing identities by decision 1's value rather than by
  the header's text.
- **The measurements the round could not run here:**
  - the SQLSTATE of a render past 1 GB, run on a machine with headroom,
    not a laptop;
  - a zstd bomb on Redpanda, watching the consumer's memory;
  - pglz on realistic dependency identities.

## Slices

1. Decision 1: the scanner, the reader and the writer in `bus`, the golden
   table and `verify/inbox-features/`.
2. Decisions 2 and 3: metadata as text, the intake's bounds, duplicate
   names.
3. Decision 4: the dependencies' parser and the size bound.
4. Decision 5: the backstop and the catalogue.
5. Decision 6: the dispatcher, with Inbox.tla.
6. Decision 7: the counters, the failure's text and the refreshed verdict.
7. Decision 3's adapter part: header names as bytes, the fetch caps.
8. Decision 8: the census, and the documents above.

The host adopts them in one pin, after quoting its stream ids.

## References

- ADR-0036 and its Change Log; ADR-0030 (identities); ADR-0033 (I3).
- PostgreSQL 14 `numeric` input (`set_var_from_str`), `jsonb` comparison
  and output, btree's `BTMaxItemSize`, `index_form_tuple`'s compression.
- serde_json 1.0.151: the recursion limit (`de.rs:63`), `-0` and
  `arbitrary_precision` (`de.rs`, `number.rs`), the private tokens
  (`number.rs:18`, `raw.rs:342`, `value/de.rs:121-134`).
- rdkafka 0.39.0 `message.rs:284`; librdkafka's
  `receive.message.max.bytes`.
- RFC 8259 § 6 and § 9: an implementation may limit the range and precision
  of numbers, and the nesting depth.

## Change Log

- 2026-11-08 — Proposed. One round of a blind solver and a discriminator,
  then five roles. Every finding of § Context is held by an executor of
  § Executors.
- 2026-11-08 — Accepted by the owner; slice 1 begun.
- 2026-11-08 — Slice 1 built.
  - `bus::identity`: the scanner (`read_json`), `read_stream_id` and
    `write_stream_id`. A raw id holding U+0000 is refused as well, so that
    D's rule on strings holds for raw ids too.
  - The inbox's intake reads the stream id through it, and `wire_of`
    writes it through its inverse. So `Inbox::publish` of the string
    `"12345"` no longer stores the number 12345.
  - The golden table and the round trip run in the workspace's build and
    in `verify/inbox-features` (`make host-features`, in `make all`), whose
    jobs build serde_json as the host does, then with `raw_value`.
  - Mutations caught, each by the table or the round trip:
    - `-0`, serde_json's keys, the depth bound and duplicate keys;
    - the fallback to a string, U+0000, the writer without quotes, a lone
      low surrogate and the range past u64.
  - Not a mutation: the "written plainly" flag repeats what the integer
    parse already refuses; it is kept for its reason.
- 2026-11-08 — Slice 2 built.
  - Every header that is not a column is kept as its text; `structured`
    and the f64 rule (`storable`) are gone.
  - A header named twice is malformed.
  - `Bounds` on `PgInbox::with_bounds`, checked before any header is read.
    A message past them is set aside as malformed and kept as a cut frame:
    - a mark that no whole frame has;
    - the digest of the whole frame, computed as it is written;
    - sizes and prefixes.

    The same bytes again are the same row, and a readmit is refused.
  - Executors, each with its mutation caught:
    - a header reaches the handler byte for byte through PostgreSQL:
      structuring restored;
    - a row inserted by SQL with a structured dependency list waits and is
      woken: the structured arm of `causal_dependencies()` dropped;
    - a message past the bounds is kept cut and refused at readmit: the
      check, the cut frame and the readmit's refusal each removed;
    - the unit tests of text metadata, duplicate names, each bound, and
      the cut frame's identity.
    - The intake's property now also holds that every metadata value is a
      string and every number an integer within 64 bits.
- 2026-11-08 — Slice 3 built.
  - **One reader of a dependency list**, `dependencies_of`, at the intake
    and at dispatch (`InboxMessage::checked_causal_dependencies`). The list
    is read by the bus's scanner; it has at most `MAX_DEPENDENCIES` (64)
    entries, each the identity of a row the inbox could store.
  - **At dispatch,** a row stored before whose list the intake would
    refuse fails for good without its subscriber, and parks; the slot goes
    on.
  - **`MAX_IDENTITY_BYTES` (2696) and `CausalDependency::stored_bytes`**
    replace `KEY_BYTES`. The bound is an upper bound on the identity's
    `jsonb` datum, calibrated on PostgreSQL 14:
    - a number is a `numeric` of at most 16 bytes after up to 3 of
      alignment;
    - a position is at most 12 bytes;
    - a string is its bytes;
    - a container is 4 bytes, and 4 per element or 8 per member.
  - **`migrate` refuses a server whose pages are not 8 KiB.** No executor
    on the stand, whose pages are 8 KiB: its mutation survives, which is
    recorded here.
  - **Executors:**
    - the largest identity of each shape is stored, waited for and woken,
      with incompressible fillers: a string, two-byte characters, a tenant
      and a stream type at their columns, and twenty-digit integers;
    - one byte past is set aside at the intake;
    - a row stored with a list the intake refuses is parked and the slot
      goes on;
    - the bound dominates `pg_column_size` of every shape, and is within
      4 bytes of it for a string;
    - unit tests of each ground of a list's refusal.
  - **Mutations caught:**
    - the bound loosened by 8 bytes (54000 at the wait);
    - the intake not reading the list;
    - no count;
    - dispatch ignoring a refused list;
    - a number, a position or an array's entries counted short;
    - a string counted by characters.
  - **A finding of building it.** A number counted short survived the
    wait at the bound. The btree compresses an item past about 510 bytes,
    and `numeric`'s headers repeat, even in twenty-digit integers each
    another. So the wait cannot hold a term of the bound, and the measure
    of the uncompressed datum does.
