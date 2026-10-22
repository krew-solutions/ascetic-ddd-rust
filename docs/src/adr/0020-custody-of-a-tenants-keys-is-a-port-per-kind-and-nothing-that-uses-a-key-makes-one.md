# ADR-0020: Custody of a tenant's keys is a port per kind, beside its use; under managed keys nothing that uses a key makes one; a rotation never makes the first version, and a retirement keeps the latest and never lowers the floor

## Status

Proposed (2026-10-18), for the owner's review. Held by one body of tests run
on every adapter that serves the signing and MAC custody,
`crates/kms/tests/common/custody.rs` — on the in-memory keys
(`tests/in_memory_keys.rs`), on PostgreSQL (`tests/pg_signing.rs`) and on a
Vault dev server (`tests/vault_signing.rs`): nothing signs or MACs before
custody makes the key and nothing is made by trying, making is idempotent, a
rotation makes no first version, a retirement retires below its bound, never
lowers the floor, refuses a bound above the latest and may retire a version
that still signs, a deletion of nothing is nothing. Beside it:
`tests/vault_mapping.rs` (the adapter's calls against a scripted transport —
managed `sign` and `mac` ask nothing but the operation, a rotation only
rotates and reads its version from the answer, a retirement reads once and
writes only a higher floor, raising a signing floor of the key's own with
it, a making reads Transit's own description and refuses another type),
`tests/vault_signing.rs` (a key of another type at the name is a conflict, a
rotation period asked is the key's, a holder's token signs and MACs and every
custody operation under it is `Forbidden`), `tests/vault.rs` and `tests/pg.rs`
(the KEK's rotation of nothing makes none, its retirement never lowers the
floor nor passes the latest — on PostgreSQL a bound above the latest deleted
every row before this), `tests/cache.rs` (a retirement and a deletion forget
what was unwrapped).

Amends ADR-0016 point 1 (the ports' methods) and point 5 (signing and MACs
make the key only without managed keys), ADR-0017 points 3 (`managed_keys()`
covers every kind) and 4 (`KeyCustody` takes `rotate_kek` and `delete_kek`),
ADR-0019 point 8 (the in-memory keys retire through the port). Discharges
ADR-0019's owed items 1 (signing custody beside the port) and 2
(`managed_keys()` for `sign` and `mac`).

The consumer's decision this serves is the platform's custody of a tenant's
keys (its ADR-0071 decision 3): a tenant-management context makes every key
a service holds for a tenant at onboarding, from one manifest, and deletes
them by name at off-boarding, so that no service makes a key of any mount
and a service still running for an off-boarded tenant cannot bring one
back. Worked as a dialectic: a blind solver given the problem without a
candidate, its design held against the author's own drawn up beforehand;
the two agreed on the shape, and the solver's measurements found the two
defects of the retirement below.

Amended 2026-10-23: what a retirement does to the material of the versions
retired differs by adapter, and the port's documentation says so — on
PostgreSQL the rows are deleted, and nothing brings them back; on Vault the
material stays until the key is trimmed, which the port does not do, and an
operator may lower the floor outside the port. Decision 4's "bounds the key
material kept at rest" holds on PostgreSQL alone. Recorded under the
consequences and the owed items, by the review of this ADR.

## Context

ADR-0017 put the KEK's custody beside its use — `KeyCustody` with
`create_kek` and `retire_kek_versions_before` — and left `rotate_kek` and
`delete_kek` on `KeyManagementService`. The signing and MAC keys of ADR-0016
had no custody of their own:

- **Their keys were made only as a side effect.** `sign` and `mac` posted
  `keys/{name}` before every operation, unconditionally; `managed_keys()`
  switched off the KEK's ensure alone.
- **A rotation made the first version.** `rotate_signing_key` made version 1
  when the key was missing (the Vault adapter's `rotate_named` created the
  key, PostgreSQL's `map_or(1, …)`, the in-memory `push`), as `rotate_kek`
  did. An onboarding step retried at least once cannot make a key through
  it, and a scheduled rotation that meets an off-boarded tenant brings its
  key back — the hazard ADR-0017 removed from `encrypt`.
- **Signing versions could not be retired through any port** (the in-memory
  keys had an inherent method, ADR-0019 point 8), and MAC keys could not be
  rotated through any port.
- **What signs saw custody in its type**: `rotate_signing_key` and
  `delete_signing_key` on `SigningService`, `delete_mac_key` on
  `MacService`, `rotate_kek` and `delete_kek` on `KeyManagementService`.

And the KEK's retirement as built had two defects:

- **On Vault it wrote the floor without reading it.** Transit lowers
  `min_decryption_version` when asked, so a stale or out-of-order
  retirement un-retired versions.
- **On PostgreSQL a bound above the latest deleted every row** — a
  crypto-shred nobody asked for — where Vault refuses the same request with
  a `400`.

Measured on Vault 1.21.4 OSS, Transit:

- `POST keys/{name}` on a new key answers `200` with the key's description
  (`data.type`, `data.latest_version`); on a key that exists with another
  `type`, `200`, the key unchanged — Transit says nothing of the conflict.
- `POST keys/{name}/rotate` on a missing key: `400 key not found`; Transit
  itself never makes a key on rotate. On a key: `200`, the new
  `data.latest_version` in the answer.
- `sign` without `key_version` and `hmac` on a missing key: `400` (`signing
  key not found`, `encryption key not found`); neither makes the key.
- `config` with `min_decryption_version` lowered (2 → 1, and to `0`): `200`,
  the retired versions served again. Above the latest: `400`. With a
  non-zero `min_encryption_version` below the new decryption floor: `400`
  (`min encryption version … must be greater than or equal to min
  decryption version`); both raised in one call: `200`.
- `hmac` after `min_decryption_version` is raised: the latest; `verify` of
  an `hmac` by a retired version: `400`.
- `auto_rotate_period` at creation: accepted on `ed25519` and `hmac`; below
  an hour, `400`.
- Under a token holding `read` on `keys/+` and `update` on `sign/+` and
  `hmac/+`: `sign` and `hmac` at the latest `200`; `POST keys/{name}` `403`;
  `rotate`, `config` and `DELETE` `403`. A token holding `update` without
  `create` on `keys/+` still makes a key: only `read` keeps a holder from
  making one.

## Decision

1. **A custody port per kind of key, beside that kind's use port.**
   `KeyCustody<S>` (the KEK's, the name kept) — `create_kek`, `rotate_kek`,
   `retire_kek_versions_before`, `delete_kek`; `SigningKeyCustody<S>` —
   `create_signing_key`, `rotate_signing_key`,
   `retire_signing_versions_before`, `delete_signing_key`;
   `MacKeyCustody<S>` — `create_mac_key`, `rotate_mac_key`,
   `retire_mac_versions_before`, `delete_mac_key`. The use ports hold use
   only: `encrypt_dek`, `decrypt_dek`, `generate_dek`, `rewrap_dek`; `sign`,
   `sign_with_version`, `verify`; `mac`; and `public_keys`. A holder bound by
   a use port cannot name a custody operation; an implementor serves exactly
   the ports it can — the in-memory keys hold no KEK (ADR-0017 point 6) and
   serve the signing and MAC custody alone.
2. **Making is idempotent and typed.** It answers the latest version; a key
   that exists is left as it is. A key at the name of another Transit type,
   or derived where a plain one is asked (or the reverse, for the KEK), is
   `Conflict`: Transit says nothing of it, so the adapter holds Transit's own
   description to what was asked. A rotation period asked and not the key's
   is set by `config`; `None` leaves the key's as it is; an adapter without
   a scheduler refuses one as `Unsupported`. `KeyOptions {
   auto_rotate_period }` for the signing and MAC keys, as `KekOptions` for
   the KEK.
3. **A rotation never makes the first version**, as Transit's never does:
   a key missing is `KekNotFound`, `SigningKeyNotFound`, `MacKeyNotFound`.
   It is not idempotent — a retried rotation makes one version more —
   which serves as well for every kind: a KEK's data is rewrapped to the
   latest, a signing key's verifiers are given the keyring and a pinned
   signer moves only when told, a MAC key makes one more refusal of each
   open etag.
4. **A retirement's bound is the oldest version kept.** The latest is never
   retired: a bound above it is `NoKeyOfVersion` and nothing is retired. A
   bound at or below the floor there is does nothing — the floor is never
   lowered, so a retried or a stale retirement is harmless; on Vault the
   adapter reads the key once and writes only a higher
   `min_decryption_version`, and on PostgreSQL the deletion below the bound
   is by its nature monotonic. A version that still signs may be retired:
   that is how a compromise is answered (the platform's ADR-0010 § 4,
   "steps 1, 2 and 4 at once"); on Vault a signing floor of the key's own
   below the bound is raised with it in the same call, which Transit
   requires and which changes nothing observable, a retired version neither
   signing nor verifying. A MAC key's retirement changes nothing a `mac`
   makes — it is made with the latest — and bounds the key material kept at
   rest.
5. **A deletion of nothing is not an error.**
6. **`managed_keys()` covers every kind.** Built so, no adapter makes a key
   on first use: a tenant without one is `KekNotFound` on a wrap,
   `SigningKeyNotFound` on `sign`, `MacKeyNotFound` on `mac` — on Vault
   named by one read of the key on a `400`, `403` or `500`, as ADR-0019
   reads a pinned signature's refusal. The default is unchanged: a consumer
   and its tests that rely on keys made on first use keep working. The
   executor of "a holder makes no key" is the holder's KMS identity — on
   Vault, `read` alone on `keys/+`; the switch is what makes the right
   configuration work, and a configuration that forgets it fails at its
   first `sign` with a `403`, loudly.
7. **A custody operation refused under the identity is `Forbidden`** —
   naming the operation, `create`, `rotate`, `retire` or `delete` — read by
   one read of the key where the key's absence would explain it otherwise.
8. **The in-memory keys** serve the signing and MAC custody ports with these
   rules and are built `managed_keys()` as the adapters are; their inherent
   retirement goes, the port's replaces it. `Cached<K>` passes every custody
   port through and forgets what was unwrapped under a tenant's KEK on its
   retirement and deletion.

## Consequences

- Breaking for every caller of the moved methods: a call site adds a `use
  … as _` of the custody port; a test that opened a key with a rotation
  makes it first. In this repository: the crate's tests and
  `ascetic-ddd-dek`'s tests; `ascetic-ddd-dek`'s source is bound by
  `KeyManagementService` alone and calls none of them.
- Calls on the success path: a managed `sign` or `mac` one, where two were
  (no ensure); a rotation one, where three were; a retirement two (a read
  first), where one was; a making one, with a read only where Vault answers
  without the key's description.
- A tenant without keys reaches a managed holder's use path as
  `*NotFound`: a consumer maps it to a refusal that names the gap, not to
  an unavailable dependency.
- The custodian holds `create`, `update` and `read` on `keys/+`, `update` on
  `keys/+/rotate` and `keys/+/config`, `delete` on `keys/+`; a holder `read`
  on `keys/+` and `update` on the operation's path alone.
- A retirement is final on PostgreSQL and final within the port on Vault.
  The port promises that a retired version never serves again, and both
  keep it; but PostgreSQL keeps it by deleting the rows — the material with
  them, as a trim would — where Vault raises a floor the adapter never
  lowers and keeps the material until the key is trimmed, which the port
  does not do, so an operator with Vault's own API may lower the floor and
  bring a version back, and a mistaken bound is mended there and not on
  PostgreSQL. For the KEK the deletion is the one retirement every reader
  of `kms_keys` honours, the Python and Go ports among them, which a floor
  they do not know would not bind. The guard against a mistaken bound is
  the custodian's, where this ADR put the serialisation of custody: a
  retirement is reviewed as a deletion is. Decision 4's "bounds the key
  material kept at rest" is true of PostgreSQL; on Vault the material is
  bounded by a trim, which no port calls.

## What can go wrong, and what holds it

- **A composition root forgets `managed_keys()`**: under a holder's policy
  its first `sign` is a `403`. Held by the holder test on Vault.
- **Two custodians retire one key at once**, each reading the floor before
  the other writes: Transit has no compare-and-set on `config`, so the
  lower bound may land last. The read narrows the hazard from every stale
  call to two writers at once; serialising custody per key is the
  custodian's — the platform's tenant-management context runs one tenant's
  custody in one ordered stream.
- **A pinned signer's version is retired** before it moves: its next
  signature is `NoKeyOfVersion`, loudly; the order of the consumer's
  procedure and ADR-0019's boot canary hold it.
- **A Vault release answers otherwise**: the live tests pin every measured
  answer and run at each upgrade.

## Owed, named

- The import of a tenant's own key material (BYOK), on the custody ports,
  with the first tenant that brings one.
- Whether `managed_keys()` becomes the default — a breaking step of its own,
  once consumers make their keys through custody.
- The two adapters name a KEK version retired otherwise: `NoKeyOfVersion`
  on Vault, `KekNotFound { key_version: Some(_) }` on PostgreSQL.
- A floor on PostgreSQL for the signing and MAC keys, whose tables are this
  port's alone — reads refusing below it, the material kept, `delete_*`
  the one deletion — would give Vault's reversibility and name a retired
  version `NoKeyOfVersion` as Vault does; the KEK keeps its deletion, for
  the readers of `kms_keys` that know no floor. A step of its own, taken
  with the first custodian's procedure that needs a retirement undone, or
  with a `trim` on the custody ports, which bounds the material on Vault.

## Alternatives rejected

**One custody port with the kind of key as a value** (`enum TenantKey {
Kek, Signing(algorithm), Mac(purpose) }`). The strongest case for it: a
custodian works from data, a manifest of keys, and a new purpose would need
no code. But the in-memory keys would answer `Unsupported` for the KEK at
run time where the types say it, the options differ by kind (`derived` is
the KEK's alone — a derived signing key would be derived per context), and
the custodian's loop over a manifest is a three-arm `match` either way; a
new purpose or algorithm is a parameter of an arm, not a new arm.

**The custody methods left on the use ports, the KMS policy the only
guard.** The smallest change, and the policy does guard. But a holder that
cannot perform them carries them in its bound, and the in-memory keys would
keep giving a consumer's tests operations its production identity is
refused — the reason ADR-0017 put the KEK's custody beside its use.

**An umbrella `trait Custody: KeyCustody + SigningKeyCustody +
MacKeyCustody`.** Importing a subtrait does not bring the supertraits'
methods into scope (`E0599`), so it spares no call site an edit; a
custodian that wants one bound declares its own.

**A conditional rotation, `rotate(after: version)`.** Cannot be atomic on
Transit, whose rotate has no compare-and-set; needs a read of the latest
the KEK and MAC ports do not otherwise have; and with a stale `after` it
would skip a compromise rotation silently.

**Refusing to retire a version that still signs.** Adds a step to the
emergency it exists for.

**The retirement written without a read.** Brings retired versions back on
any stale call.

**Managed keys as the default now, or a type-state switch.** The first
changes run-time behaviour under every consumer with no compile error; the
second adds a type parameter to every signature that names an adapter and
enforces nothing the KMS identity does not.

**Renaming `KeyCustody` to `KekCustody`.** Symmetry bought with an edit at
every consumer's import and in the ADRs; the name already pairs it with
`KeyManagementService`.
