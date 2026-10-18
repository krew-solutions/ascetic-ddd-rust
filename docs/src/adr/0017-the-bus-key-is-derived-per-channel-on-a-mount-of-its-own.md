# ADR-0017: The bus's key is one per tenant on a mount of its own, derived per channel — the adapter makes no key, custody is a port beside, and a refusal is read once

## Status

Accepted (2026-10-13): held by `tests/vault_mapping.rs` of `ascetic-ddd-kms`
(the adapter's reading of Transit's answers against a scripted transport: no
key made under managed keys, the context with every operation and `derived`
on a key made, a `400` and a `403` on a KEK operation explained by one read,
custody's creation and retirement), `tests/vault.rs` (the same on a Vault
dev server: a derived key binds the DEK to its context, managed keys are not
made on first contact, custody makes the key once and retires its versions,
a gone key is `KekNotFound` and another tenant's key `Decrypt`), `tests/pg.rs`
(the PostgreSQL adapter: managed keys, the context bound into the wrap,
custody), `tests/domain.rs` (a KEK serving a context binds what it wraps to
it), and `tests/stage.rs` of `ascetic-ddd-dek` (two stages built apart from
one channel constant seal and open; another channel's stage is refused for
good).

The consumer's decision this serves is the platform's ADR-0071 (sealed
payloads on the bus), worked there as a dialectic — a blind solver, the
session, the owner, a discriminator measuring on Vault 1.21.4 OSS — whose
findings on this crate are the facts below; the shape of the port and the
adapters was examined from two sides there (the solver's changes S1–S3, the
discriminator's D4 and D7) before being written here.

## Context

`ascetic-ddd-dek`'s `EnvelopeStage` seals a message's payload under a DEK
wrapped by the tenant's KEK through `KeyManagementService`, as ADR-0002
decided; the KEK is the tenant's, one on one mount, named by the tenant's id
(ADR-0016). A platform that runs the stage on its bus found four things the
crate did not say or did wrongly, measured on Vault 1.21.4 OSS:

- **The adapter made keys for whoever wrapped.** `encrypt_dek` and
  `generate_dek` called `POST keys/{name}` first, "idempotent, no read
  first" (ADR-0016 point 5 for signing; the KEK did the same). Transit
  creates a key on `encrypt` too, under a token that may `create` *or*
  `update` on `keys/+`. So a producer that wrapped for a tenant whose key
  had been deleted — crypto-shredded at off-boarding — made the key again at
  version 1, and the tenant's data flowed again; and a token that may not
  make keys failed every wrap with a `403`.
- **A gone key was tried again, forever.** Transit answers `400` for a key
  it has not got on `decrypt`, `datakey` and `rewrap`, and `404` only on
  `GET keys/{name}`; the adapter mapped `404` alone to `KekNotFound`, so a
  gone key reached the stage as `Error::Vault`, which its verdict leaves of
  the moment, and the inbox — whose default retries are unbounded
  (ADR-0005) — held the message's partition, shared by every tenant of the
  topic, until an operator looked. ADR-0016 point 6 had already explained a
  `400` by one read of the key, for signing and MACs only.
- **One key per tenant served every channel alike.** A consumer that may
  unwrap on a tenant's key may unwrap any channel's DEK of that tenant,
  stored data's too, given the ciphertext; the broker's ACL is the only
  thing telling channels apart. Transit's derived keys (`derived=true`)
  derive a key per caller-supplied `context`, and a Vault policy can pin
  the `context` a caller may pass (`allowed_parameters` with
  `required_parameters`), which gives a per-channel gate at the KMS without
  a key object per channel — but only on a key that is derived: on a plain
  key Transit ignores `context`, so a policy's pin on a mount holding both
  kinds opens the plain keys' DEKs. `derived` cannot be changed after
  creation; per-tenant rotation, retirement and deletion work on a derived
  key as on a plain one.
- **Nothing retired a version.** `min_decryption_version` makes what an
  earlier version wrapped unopenable for good, and a bus's ciphertext lives
  no longer than its channel's retention, so the bound of a message's life
  is one call once the retention has passed; the port had no such call, and
  the store's DEKs, which live as long as the data, must not share the key
  it is made on.

## Decision

1. **The bus's key is a derived key per tenant on a mount of its own.** The
   platform composes two `VaultTransitService`s: one on the mount of
   ADR-0016 for the store's KEK, the signing and the MAC keys, names
   untouched; one on a second mount, every key of it derived, one per
   tenant, named by the tenant's id as the first is. `with_mount` exists
   and the port does not change: the purpose is the adapter instance, as
   ADR-0016's consequence said ("one service per mount, the ports do not
   change"). A mount, not a name label, because the per-channel gate of
   point 2 is void on a path shared with plain keys, and a policy's prefix
   names a mount.
2. **The channel is the derivation context, taken from the contract.**
   `with_derivation_context(channel)` on either adapter: on Vault every KEK
   operation carries the context, base64, and a KEK the service makes is
   made `derived`; on PostgreSQL the context is bound into the wrap's
   associated data beside the tenant — length-prefixed, so neither runs
   into the other — which gives the same property with the same error: a
   DEK wrapped for one channel does not unwrap for another, `Decrypt`. The
   composition root builds one instance, one `Cached` and one
   `EnvelopeStage` per channel, at the producer's end and at the consumer's,
   from the channel's name in the contract both are generated from; the
   context never comes from a header, so a sender cannot choose it. One
   stage per channel also keeps `Reuse` sound as it is: the DEK in service
   is keyed by the tenant, and would otherwise be drawn under one channel
   and reused for another's message, which the other's consumer could not
   open. The mechanism is the crate's, beside the rule
   (`ascetic-ddd-dek::Sealing`, amended 2026-10-13): the stages of a
   service's channels built up front from the list its contracts name,
   each over the KMS instance the caller makes for the channel, behind a
   `Cached` of its own, with the parameters the deployment's; `of` answers
   `None` for a channel the sealing was not built for, so a composition
   root refuses to start rather than seal under a typo. A pool shared by
   `Arc` is a pool (`ascetic-ddd-session`), so the service's own pool
   serves the stages without a wrapper.
3. **The adapter makes no key for whoever wraps.** `managed_keys()` on
   either adapter: no `POST keys/{name}` before a wrap; a tenant without a
   KEK is `KekNotFound` on every wrap, parked at once by the stage. A
   producer's or a consumer's token may then be denied `create` and
   `update` on `keys/+`, and a shredded tenant stays shredded. The default
   stays as it was, for the store and for the tests: `ensure_key` on first
   contact.
4. **Custody is a port beside, not on, `KeyManagementService`.**
   `KeyCustody<S>`: `create_kek(tenant_id, &KekOptions { derived,
   auto_rotate_period })` — idempotent, answering the latest version; a key
   that exists with another `derived` is `Conflict`, since that cannot be
   changed, and the rotation period, which can, is set by `config` — and
   `retire_kek_versions_before(tenant_id, version)`, Transit's
   `min_decryption_version`. Beside the KMS port for the reason ADR-0016
   point 1 gave for signing: `ascetic-ddd-dek` is bound by
   `KeyManagementService` and must not grow a bound it does not use, and a
   producer or a consumer of the bus must not hold the custody of its keys.
   `Cached<K>` passes it through and forgets the tenant's unwrapped keys on
   a retirement, as on a deletion. The PostgreSQL adapter serves it as the
   simple adapter: `create_kek` under the tenant's lock, `derived` with
   nothing to enforce (there is no policy to pin a context by; the binding
   is the wrap's), `auto_rotate_period` refused as `Unsupported` rather than
   ignored (a table has no scheduler), retirement a deletion of the rows
   below the version. BYOK import stays owed, with the first tenant that
   brings a key.
5. **A refusal of a KEK operation is explained by one read of the key, and
   what the read does not explain is the request's own fault.** ADR-0016
   point 6 extended to the KEK: on a `400` from `encrypt`, `decrypt`,
   `datakey` or `rewrap` the adapter reads `GET keys/{name}` once — a `404`
   is `KekNotFound`, a version outside what the key serves (the version
   parsed from the `vault:v{n}:` text) `NoKeyOfVersion`; and, the key there
   and the version served, the `400` no longer stays Vault's: on `decrypt`
   and `rewrap` it is `Decrypt` (a wrong context, another tenant's key,
   bytes changed since they were sealed — authenticated encryption tells no
   more, by design), elsewhere `Malformed` with Vault's words. A `403` is
   read the same way: the key there, `Forbidden` — the policy does not allow
   the operation under this identity, or the context is not one it admits —
   the key not there, `KekNotFound`, the read refused too, the `403` as it
   came, for the operator whose policy lacks the read. Vault's texts are
   not matched. The stage's verdict parks `Forbidden`, `Conflict` and
   `Unsupported` with the rest it parks: no retry under the same identity
   and context mends them.
6. **The in-memory keys do not serve the KEK port**, as before: the stage's
   and the custody's tests run on the PostgreSQL adapter, the consumer's on
   Vault.

Amended 2026-10-18 (ADR-0020): `managed_keys()` (point 3) covers every
kind of key — `sign` and `mac` make none either; `KeyCustody` (point 4)
takes `rotate_kek` and `delete_kek` from `KeyManagementService`, so that
what wraps and unwraps names no custody; a rotation of a tenant without a
KEK is `KekNotFound` rather than its first version; a retirement reads the
key and never lowers the floor, and a bound above the latest is
`NoKeyOfVersion` — on PostgreSQL it deleted every row before.

Amended 2026-10-14 (ADR-0018): at an inbox that receives several channels
— one subscriber for every row, one stage per `through` — the stage is
chosen per row by its `destination`, the intake's name for the channel
received on: `Sealing::by_destination`, built up front from the received
channels, refusing a row it has no stage for before any KMS is asked.

Amended 2026-10-13, after the first consumer composed the stages by hand:
the composition — one stage per channel, built from the channel list, over
a KMS instance per channel — is the rule's own and lives beside it as
`Sealing`; the consumer keeps the KMS recipe and the list. The consumer's
first form, a lazily built map keyed by whatever text was asked, would
have sealed a mistyped channel under a context nobody admits; built up
front, a typo is a refusal at boot.

## Consequences

- A tenant has a key object more — the bus's — and one lifecycle more: its
  retirement follows the bus's retention, the store's never; two deletes at
  off-boarding where ADR-0016 counted three, and a test on the consumer's
  side that both mounts hold nothing for the tenant.
- The same `KeyManagementService` surface serves a derived key and a plain
  one; a consumer tells them apart by how it composes the adapter, not by
  what it calls.
- The failure path of a KEK operation costs one read where it cost none;
  the success path none. What the stage parks is wider, and what it
  retries narrower: a `400` or a `403` with the key there was a retry
  before and is a parking now.
- `MasterKey::generate_kek` and `load_kek` take the context: a breaking
  change of the model's surface, for the one adapter that calls them and
  the tests.
- `create_kek` reads the key back after making it, since Transit leaves a
  key that exists as it is and says nothing: one read on a path that is
  custody's, not the wrap's.

What can go wrong, and what holds it:

- *Two ends spell the channel differently* — a rename in one contract and
  not the other: `Decrypt` at the consumer, parked, forever. Held by the
  stage test that builds both ends from one constant, and by the
  consumer's generator that takes the name from the one file both are
  generated from.
- *A policy admits a channel the consumer no longer reads, or lacks one it
  does*: `Forbidden`, parked. Held by the consumer's generated policies
  under its CI gate, and by an alert on parked messages; a `403` on the
  read itself stays of the moment, and an operator sees the policy lacks
  `read`.
- *Transit's `400` for a wrong context and for a corrupted ciphertext are
  one `Decrypt`*: so are they in authenticated encryption; a consumer that
  must tell them apart has the key's description and the message's
  `dek_bound_to` to look at, not Vault's words.

## Alternatives rejected

**The context in the stage's associated data instead of the key's
derivation.** Binds the payload, not the DEK: a consumer that may unwrap
the tenant's key opens the DEK of any channel, and the KMS's policy cannot
pin what it does not see. The stage keeps `[tenant_id, message_id]` as its
associated data; the channel is bound where the policy looks.

**A key object per (tenant, channel), and the port's identity generalised
from `tenant_id` to a label.** Tenants × channels rotation schedules,
policies and deletes for a gate the pinned context gives without one;
`tenant_id` threads through the stage, the cache, both adapters and the
tests for nothing gained. The platform's owner declined it on that ground.

**Derived contexts on the existing mount.** Measured void: Transit ignores
`context` on a plain key, so a consumer's pinned context opens the store's
DEKs through the `+` of its policy path.

**Telling a `403` or a `400` apart by Vault's text.** Works on this release
and breaks silently on the next (ADR-0016).

**Creating the key on first wrap, with `derived` from the context.** Keeps
the old contract and lets a producer resurrect a shredded tenant's key;
custody is the tenant-management context's, and the producer's token
should not be able to.

**An in-memory adapter of the KEK port for the stage's tests.** The
PostgreSQL adapter is the simple one already, and the tests run on it with
nothing but the network replaced; a third implementation of the binding
would be a third place for it to drift.
