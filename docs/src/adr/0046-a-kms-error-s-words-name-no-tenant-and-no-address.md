# ADR-0046: A KMS error's words name no Tenant and no address

## Status

Proposed and built (2026-11-20), in one slice:
- `crates/kms`:
  - no error's text names its tenant;
  - reqwest's errors go without their URL;
  - Vault's words have the key's name, the tenant's, written over where Transit answers.
- `crates/inbox`: an executor of a head's oldest hold.
- `crates/bus` and `crates/inbox`: the doc comments that said a Tenant-scoped `Unavailable` spends an attempt.

What a consumer needs of this library to hand the inbox a failure's verdict whole was decided here under the
library's gate:
- a blind solver, the main position and a discriminator;
- the synthesis refuted by three roles: Distributed Systems with Operability, Security, and Verifiability.

Two of the three library changes the consumer asked for were refuted and are recorded in § Alternatives
Considered: a failure's text kept whatever its cause, and a refusal a handler tells naming its witness. The one
built is what the roles found the consumer needs regardless: a KMS's words must not carry a Tenant or an address.

## Context

The library's one consumer reduces a failure to "transient or not" before the inbox sees it (its ADR-0091 § Owed
4, item 1). It asked three things of this library
(`ascetic-ddd-rust.task-inbox-verdict-prerequisites.md`):
- **L1:** a named cause erases the failure's text. `record_failure` keeps `last_error` only for `UNEXPLAINED`
  (`crates/inbox/src/pg/store.rs`).
- **L2:** an `Unavailable` a handler tells, met while sealing its outbox writes, reaches the rows' breaker, whose
  trial a cached seal closes.
- **L3:** a Tenant-scoped `Unavailable` holds a row with no end.

The round found that the words the KMS crate gives an error reach places they must not:
- **`Error::Transport`** printed reqwest's error, which ends with `for url (…)`. The URL is
  `{addr}/v1/{mount}/datakey/plaintext/{tenant}`: the tenant's id and the Vault's address.
- **`Error::Vault`** printed Vault's joined `errors`. They quote the request's path, the key's name, in a rate
  limit's answer (`request path "transit-bus/datakey/plaintext/<tenant>": rate limit quota exceeded`) and in a
  route's (`no handler for route "transit/keys/<tenant>"`, ADR-0039's table, measured). The comment at the
  variant ("No path: a key's name is its tenant's") held for its `path` field only.
- **`KekNotFound`, `SigningKeyNotFound`, `MacKeyNotFound`, `Conflict` and `Forbidden`** printed `tenant_id`.

Those texts reach:
- the DEK's `Last.text`, and through `dek::Unavailable`'s `Display` a handler that relays it;
- the inbox's `last_error`;
- at the consumer, a REST problem's `detail` sent to an external client, and its edge's warn log.

A rate limit's 429 is of the class `Unavailable` (`crates/kms/src/error.rs`): exactly the failure a host relays
during a KMS's trouble.

## Decision

1. **A KMS error's text names no tenant.** `Display` of `KekNotFound`, `SigningKeyNotFound`, `MacKeyNotFound`,
   `Conflict` and `Forbidden` says "the tenant". The tenant stays a field, for code that matches on it.
2. **reqwest's errors go without their URL** (`crates/kms/src/vault/reqwest.rs`, `Error::without_url`) at both
   calls that can fail: the send, and the read of the answer.
3. **Vault's words name no key.** `transit()` writes the name of the key its `path` names as `<key>` wherever it
   stands whole in Vault's joined `errors` (`without_key_name`). The forms are:
   - `/datakey/plaintext/{name}`;
   - `/keys/{name}[/…]`;
   - `/{operation}/{name}`.

   Only a whole name is written over: one that stands apart from the characters a name is made of, so that a
   short name never cuts into another word. Every refusal carrying Vault's words passes through `transit()`:
   `Error::Vault`, and the `Malformed` the explaining reads build from it.
4. **L1: the inbox's rule stands.** A failure's text is kept only when its cause is unexplained. A named cause
   is a class, and its text is a foreign library's words, which the round showed can carry another Tenant, an
   address, or a value:
   - the outbox's `IdentityTaken` names the outbound message's Tenant;
   - a PostgreSQL error may quote a refused value.

   A host names a cause for a dependency's trouble (`kms.*`, `bus.*`), whose class is what its operator acts on,
   and keeps its own workflows' refusals unexplained, with their text.
5. **L2: no library change.** A host relays a handler's sealing `Unavailable` at `Tenant` scope (§ For the
   consumer).
   - **Why it is enough:** in a full outage every sealed inbound channel is paused by its opening dependency
     (ADR-0042 decisions 1 and 9; ADR-0044 registers it), so its handlers are not asked. A handler meets the
     sealing end's trouble alone only before the opening's gate trips, while the opening's keys are warm (the
     `keep`, five minutes by default), and when Vault's datakey path alone refuses (a quota) while decrypt
     serves.
   - **What it gives:**
     - `Tenant` scope holds the head per (channel, Tenant) in the process's memo, spending no attempt and
       parking nothing;
     - no breaker is involved, so a cached seal closes nothing.
     - The head is paged by its oldest hold (ADR-0042 decision 6).
6. **L3: no library change.** `held_counts` gives the oldest `Unavailable` hold by channel and cause
   (`HeldCount`), the page ADR-0042 decision 6 states, and `held_since` keeps a head's first hold whatever holds
   it after.
   - One Tenant's endless hold and a stage's outage under one cause share a `HeldCount` row, told apart by
     `tenants`. A host pages on the oldest age either way.

## Consequences

- An operator reading a KMS's refusal in a log, a parked row or a client's answer learns the class and the
  operation, never the tenant or the Vault's address. The tenant is the row's own, in its `tenant_id`.
- A host's diagnosis of a dependency's trouble rests on its cause (`kms.unavailable`, `kms.forbidden`, …) and the
  KMS's own audit, not on the row's text.
- The relay at `Tenant` scope asks each held Tenant's head again once its memo's interval runs out: up to the
  memo's cap of 300 s per (channel, Tenant) per process. This costs asks during the residual case above, and
  delays a Tenant's resumption after recovery by up to that cap.

## Rule of Three

1. **A Vault answer quoting a path in another form escapes the writing-over.** The forms are `transit()`'s
   callers' own (`crates/kms/src/vault/mod.rs`), all named. A new caller with a new form returns `None` from
   `key_name_in` and leaves the words as they are. Answered by the test over every form, and by the quota's
   end-to-end test through `transit()`.
2. **A name that is also a common word is written over in Vault's prose.** It is written over only where it
   stands whole. A UUID or a dotted key name stands whole only where it is the name. A test pins that `t` is not
   written over inside `transit`.
3. **Vault's words still carry the mount.** The mount is the deployment's configuration, not a tenant's. The
   address is gone with reqwest's URL.

## Alternatives Considered

- **Keep a failure's text whatever its cause (L1).** Refuted by the Security role:
  - Vault's words carry paths with the tenant's key name;
  - PostgreSQL texts may quote values;
  - the outbox's `IdentityTaken` names another Tenant;
  - `parked()` and `held()` are not audited and not filtered by Tenant (ADR-0036 R7);
  - off-boarding does not reach `last_error`.

  Widening what the column keeps widens each of these.
- **Keep the text by who told it: a handler's, not a stage's.** Refuted by the discriminator. A stage's text on
  the way in names at most the row's own Tenant; only the sealing end's text, relayed by a handler, can name
  another. The rule kept the dangerous case and dropped the safe one.
- **A refusal names its witness (L2), with a gate per (channel, witness).** Refuted by the discriminator: one
  sealing stage serves many inbound channels, so breadth, checks and resumption multiply, and a gate born at
  first sight trips at no served history.
- **Evidence per witness process-wide, a pause per channel that met it.** Refuted by Distributed Systems and
  Verifiability:
  - a fast refusal never opens the gate: no head to check;
  - the tie lapses after the window while the gate is open, and the channel flaps every window;
  - one rare row pauses a whole other channel;
  - recovery is set by the stage's own back-off;
  - a time-based resume passes every executor the synthesis named.
- **A handler's refusal weighed by the channel's registered opening dependency.** Refuted: the check decrypts,
  and a decrypt served refutes every datakey suspicion (ADR-0042 C42). Datakey and decrypt are separate Vault
  paths and grants.
- **A bound on a Tenant-scoped hold in the library (L3).** ADR-0042 decided that an `Unavailable` never spends
  or parks. The host's page on the oldest hold is the end a person gives it.

## Executors

| Element | Executor | Mutation that turns it red |
| --- | --- | --- |
| 1 | `error::tests::no_error_s_text_names_its_tenant`: every variant with a tenant built with a UUID, none naming it; an exhaustive `variant` match, so a new variant fails to compile there first | a variant printing its tenant (killed) |
| 2 | `vault::reqwest::tests::a_refused_call_names_no_url`, offline: reqwest refuses `ftp://` before any I/O, naming the URL | `without_url` dropped at the send (killed) |
| 3 | `vault::tests::a_transit_path_names_its_key_in_each_of_its_shapes`; `vault::tests::vault_s_words_name_no_key` over Vault's quota and route texts and a short name; `vault_mapping::a_quota_s_answer_names_no_tenant` through `transit()` | `transit()` without the writing-over (killed); no word boundary (killed) |
| 6 | `outage::a_head_s_trouble_is_timed_from_its_first_hold_whatever_holds_it_after`: held when asked, held unasked, a spend, `oldest` the first hold | `held_since = statement_timestamp()` in each of the three statements (each killed) |

Not executed: the read of an answer's text has no offline failure, so `without_url` there has no test of its
own. It is the same call as the send's.

## For the consumer

At the pin that carries this decision:
1. **A dependency's trouble relayed by a handler.** A `dek::Unavailable` met while sealing inside a handler goes
   to the inbox as a `bus::Unavailable` at `Tenant` scope under its cause, never `Stage`. A handler's `Stage`
   refusal reaches the rows' breaker, whose trial a cached seal or a Command that seals nothing closes.
2. **Name a cause only for a dependency's trouble** (`kms.*`, `bus.*`). A workflow's own refusal stays
   unexplained: its text is kept for the person who reads the parked row.
3. **Page on the oldest `Unavailable` hold** (`held_counts`, ADR-0042 decision 6) before relaying any
   `Unavailable`. Without the page a held head has no end anyone sees.
4. **No consumer code changes for this ADR.** Its texts change: a consumer test that matched a tenant id in a
   KMS error's text goes red.

## Owed

- **Audited reads of the rows' text.** `parked()` and `held()` take no principal and filter no Tenant (ADR-0036
  R7). A failure's text is a Tenant's data.
- **A Tenant's off-boarding in other Tenants' rows.** `last_error` is unsealed and no crypto-shredding reaches it.
  The consumer's off-boarding deletes keys only.
- **A PostgreSQL error quoting a value in a failure's text** (`crates/inbox/src/error.rs`): today's convention,
  never a value, is not executed.

## References

ADR-0036 (R7, R9), ADR-0037 (decision 7), ADR-0039 (the class table, measured), ADR-0040, ADR-0041, ADR-0042
(decisions 1, 2, 6, 9; C42), ADR-0044. The consumer's ADR-0091 § Owed 4.

## Change Log

- 2026-11-20 — Proposed and built.
