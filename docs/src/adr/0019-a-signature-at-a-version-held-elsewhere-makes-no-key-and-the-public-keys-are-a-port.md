# ADR-0019: A signature whose version something outside the KMS already holds is made at that version and makes no key; the public keys of the versions that verify are read through a port of their own, derived where the adapter holds the private halves

## Status

Proposed (2026-10-18), for the owner's review. Held by `tests/vault_mapping.rs`
of `ascetic-ddd-kms` (the adapter's reading of Transit's answers against a
scripted transport: a pinned signature asks its version and no `POST keys`,
a signature by another version is refused, every measured refusal is
explained by one read, the public keys read from PEM and base64 against
signatures Transit made and openssl verified), `tests/vault_signing.rs` (the
same on a Vault dev server, a token holding the key's `sign` and `read`
alone among them), `tests/pg_signing.rs` (the PostgreSQL adapter: the
earlier version signs when named, a pinned signature makes nothing, the
keyring is the versions there are, a public half replaced in the table is
refused), `tests/in_memory_keys.rs` (the in-memory keys sign, retire and
publish as the adapters do), `tests/cache.rs` (the keyring is read through)
and the domain's unit tests (a keyring is never empty, of one algorithm; a
P-256 key is held uncompressed).

Supersedes ADR-0016 point 7; amends its point 5 (`sign` makes the key,
`sign_with_version` never does).

The consumer's decision this serves is the platform's ADR-0010
(authentication and session architecture, revised 2026-10-17), worked as a
dialectic — a blind solver given the problem, a discriminator who brought
three designs of its own from other premises and measured on Vault 1.21.4
OSS — whose findings on this crate are the facts below.

Amended 2026-10-18 by ADR-0020: point 8 — the in-memory keys retire
through `SigningKeyCustody`, as the adapters do; the owed items 1 (signing
custody beside the port) and 2 (`managed_keys()` for `sign` and `mac`) are
discharged.

Amended 2026-10-23: owed item 3 discharged — a refusal of `verify` is read
as decision 3 reads a pinned signature's, of the versions that verify
rather than sign: the key missing `SigningKeyNotFound` on a `403` as on a
`400`, a `403` with the key there `Forbidden`, which a holder whose policy
holds `sign` and `read` and no `verify` meets, a retired version
`NoKeyOfVersion`, an unexplained `400` `Malformed`; a version below
`min_encryption_version` serves a verification, so a `400` by it is the
request's fault and not the version's. Held by `tests/vault_mapping.rs`
(each case against a scripted transport) and the holder's test in
`tests/vault_signing.rs` (`verify` under the holder's token is `Forbidden`).

Amended 2026-10-23: the alternatives to decision 5 that the review of this
ADR found unweighed — the stored public half sealed under the master key,
and sealed for reading with the pair checked at signing — are recorded
under the alternatives rejected, with the consequence on a publisher's
privileges on PostgreSQL.

## Context

`SigningService` signed with the latest version of a tenant's key of an
algorithm and made the key on first signing (ADR-0016 point 5), and the
public half of a version was left to "a verifier elsewhere" reading Transit
or the table itself (ADR-0016 point 7: "not a method of the port until one
is needed"). The first users needed both, and found three more things:

- **A message that names its key's version inside what is signed cannot
  be signed with "the latest".** The platform mints ES256 JWTs through the
  port; a JWS's header — `alg`, `kid`, `typ` — is part of the signing input
  (RFC 7515 § 7.1), and its `kid` names the version (`d.<domain>.v<n>`). The
  version must be known before the signature is made; a rotation between a
  read of "the latest" and the signature makes a token whose `kid` names one
  version and whose signature is another's.
- **A verifier that holds the public keys refuses a version until it is
  given it.** The platform's gateway verifies against a JWKS pushed with its
  configuration and never refetches on an unknown `kid`; its rotation is
  four steps — rotate in the KMS, push both public versions, raise the
  version the issuer signs with, retire the old one after the token's
  maximum lifetime. With the latest signing, step one makes every new token
  one the gateways refuse until step two lands everywhere. Devices verifying
  bundles under a key directory are the second user of the same shape.
- **The latest-signing cannot run under the identity the signer is given.**
  The platform grants the minting token `update` on the key's `sign` path and
  `read` on the key, nothing else. `sign`'s first call is `POST keys/{name}`
  — the ensure of point 5, unconditional, outside `managed_keys()` — and
  that is a `403` under this token, while the signature itself is allowed.
- **On PostgreSQL the public half beside the wrapped private half is not
  authenticated.** The private half is wrapped under the master key with
  the key's name as associated data; the `public_key` column is plain bytes.
  `verify` read the column, so a writer of the table without the master key
  could make the adapter verify under a key of their own; a published
  public key read from the column would make database write into token
  forgery at every verifier that trusts the publication.
- **A P-256 public key was held in whatever SEC1 form it came in**, while
  its documentation promised the sixty-five-byte uncompressed point a JWK's
  coordinates are cut from.

Measured on Vault 1.21.4 OSS, Transit, `ecdsa-p256` and `ed25519`:

- `POST sign/{name}` with `key_version` on a key that does not exist: `400`
  `signing key not found`; `GET keys/{name}`: `404`.
- `key_version` above the latest: `500`; below `min_decryption_version` (the
  version retired): `500` `no such key version`; below
  `min_encryption_version`: `500`; `key_version: 0`: the latest.
- Raising `min_decryption_version` leaves `min_encryption_version` at `0`,
  which is no floor of its own; a floor above the latest is refused (`400`);
  a retirement is reversible until the key is trimmed; `GET keys/{name}`
  lists the versions from `min_decryption_version` to the latest.
- `data.keys.{n}.public_key`: base64 of the raw thirty-two bytes for
  Ed25519, a PEM SubjectPublicKeyInfo for P-256.
- Under a token holding the key's `sign` (`update`) and `read` only: the
  pinned signature `200`, a signature without `key_version` `200`, `POST
  keys/{name}` `403`, `verify` and `rotate` `403`; under `read` only, the
  pinned signature `403` while the read is `200`.
- A read of the key costs the order of a signature (6.6 ms against 7.7 ms
  per call on the stand, process spawn included).

## Decision

1. **A second way to sign, at the version named.**
   `sign_with_version(session, tenant_id, algorithm, key_version, message)`
   signs with exactly that version; the signature names it whatever the
   latest is. The version is a `NonZeroU32`: Transit reads `0` as the latest,
   which is what the method exists not to do. On Vault the version Transit
   reports is checked against the one asked, `Malformed` otherwise. `sign`
   stays as it was — right where the signed bytes do not name the version
   and the verifier is the KMS — and the trait's documentation states both
   criteria.
2. **It never makes a key.** A version is named because something outside
   the KMS already holds it; a key made here would be a version nobody
   holds, or one numbered as a deleted key's was. It is the one signing that
   runs under an identity that may not make keys.
3. **On Vault a refusal of it is explained by one read of the key, on
   `400`, `403` and `500`, never by Vault's text.** The key missing:
   `SigningKeyNotFound`; a `403` with the key there: `Forbidden`; a version
   outside `[max(min_decryption_version, min_encryption_version, 1),
   latest]`: `NoKeyOfVersion`; a `400` the read does not explain: `Malformed`
   (the rule ADR-0017 point 5 set for the KEK); a `500` it does not explain
   stays Vault's, of the moment; the read refused: the refusal as it came.
   Any other status is left as it came, unread: a read would add to the load
   a rate limit protects, or meet the same sealed or standby node.
4. **The public keys are a port of their own**, `PublicKeyService` with
   `public_keys(session, tenant_id, algorithm)`, answering a
   `PublicKeyring`: the public halves of the versions that verify, never
   empty, one algorithm, a version that verifies and no longer signs among
   them. A query, beside `SigningService` rather than on it — interface
   segregation: an adapter that only publishes keys is bound by reading
   them, and a holder of public keys that cannot sign can serve it. The
   keyring names no version as the one that signs and answers no newest, so
   that pinning a signature to whatever the KMS made last is not the easy
   path; and it does not verify — a key the KMS holds is verified in the KMS
   (ADR-0016 point 7 stands on that), a verifier elsewhere verifies with its
   own library.
5. **Where an adapter holds the private halves, a public half is derived
   from its private half and the stored one is checked against it.** On
   PostgreSQL `public_keys` and `verify` unwrap each version and compare: a
   mismatch is `Malformed` — a tamper detected, never a forged key published
   or verified under.
6. **A public key is one form per algorithm**: a P-256 key is held
   uncompressed, `0x04 || x || y`, whatever SEC1 form it was made from, as a
   signature is one form per algorithm.
7. **Neither new method is cached.** A keyring held across a rotation would
   be a JWKS without the version about to sign; `Cached<K>` passes both
   through, as it passes signing.
8. **The in-memory keys retire as an operator does**, by an inherent
   method, `retire_signing_versions_before`; retirement stays off the port.
   A version once used is not numbered again.

## Consequences

- One required method more on `SigningService` and one trait more; the four
  implementors are this crate's own. `services/policy` of the platform
  needs no source change.
- The `vault` feature gains `p256/pkcs8`, one crate more in a `vault` build
  (`pkcs8`); `der` and `spki` were in it already; `p256/pem` is not enabled,
  since its crate is not in the lock.
- A pinned signature's success path costs one call fewer than `sign`'s (no
  ensure); its failure path, one read.
- On PostgreSQL the public keys and `verify` unwrap — one AES-GCM open and
  one scalar multiplication per version — and fail with `Decrypt` under a
  wrong master key, as `sign` does: integrity over availability for what a
  verifier trusts.
- On PostgreSQL a publisher of public keys holds the master key, as a
  signer does, and so could sign: the separation `PublicKeyService` is made
  for — a holder of public keys that cannot sign — is Vault's alone, where
  the publisher holds `read` on the key and nothing else. The adapter's
  posture is the simple one (ADR-0016), and nothing short of Vault behind
  the same port changes it.
- The version that signs lives in the consumer's configuration; it can name
  a version the KMS has not made or no longer signs with, which the crate
  turns into `NoKeyOfVersion` at the first signature. A consumer turns that
  into a refused readiness with a canary — a pinned signature at boot,
  checked under the keyring — which this decision recommends and does not
  hold.

## What can go wrong, and what holds it

- **A Vault release answers another status for a version refusal**: it
  stays Vault's and is retried. Held by the live tests, which pin every
  measured status and run at each Vault upgrade.
- **A consumer pins whatever the keyring lists last**: the hazard by hand.
  The keyring answers no newest and its documentation says why; nothing
  stronger is possible from the crate.
- **A database writer replaces a stored public half**: `Malformed` on the
  read and on `verify`, held by the PostgreSQL test that overwrites the
  column.

## Owed, named

- Signing custody beside the port, as ADR-0017 point 4 did for the KEK:
  `rotate_signing_key`, `delete_signing_key` and a retirement off
  `SigningService`, whose minting holder is refused them. Breaking for
  consumers' tests, so a step of its own.
- `managed_keys()` for `sign` and `mac`: the latest-signing still cannot run
  under a token that may not make keys.
- `verify` keeping ADR-0016's mapping of a `400`; not changed here.

## Alternatives rejected

**One `sign` with an optional version.** Every caller decides at each call
whether `None` means the latest; the type would not say which signature a
JWS may be made with, and `Some(0)` is the latest again on Transit.

**Every signature pinned.** Breaks the bundle signer, whose `key_id` is
outside the signed bytes and whose verifier is the KMS — the case "the
latest" is right for — for no gain.

**The floor as the pointer** (`min_encryption_version` as "the version that
signs", held in the KMS). It is a floor, not a pointer: every other client
still signs with the latest above it, its default `0` is the hazard itself,
and reading it costs what a signature costs. Kept as an optional guard of
the consumer's procedure: raised once every replica signs with the new
version, a replica left behind fails loudly.

**Key generations instead of versions** (a new key per rotation, the
generation in the consumer's configuration). Still needs a signature that
does not make the key; deleting a generation cannot be undone where
retiring a version can until a trim; the list of live generations leaves the
KMS.

**The public-key read on `SigningService`.** A publisher would implement or
stub four methods it never runs; interface segregation decides, not who
holds the trait.

**A keyring that verifies, or answers its newest.** The first reopens the
local verification ADR-0016 rejected for keys the KMS holds; the second
makes the hazard of point 1 the obvious line.

**The stored public half on PostgreSQL.** Cheaper by an unwrap per version,
and a trust anchor anyone who can write the table controls.

**The stored public half sealed under the master key.** Tamper-evident as
the derivation is, and the private half stays wrapped while a verifier or
a publisher reads: a step towards the posture Vault has in full behind the
same port. It authenticates the writer and not the pair: a public half of
another key, sealed by the master key's holder — a fault of a rotation, an
import — passes, where the derivation refuses it. The processor is no
argument either way, a scalar multiplication and an AES-GCM open both
vanishing in the round trip to the table; the difference is the private
half in memory on the paths that verify and publish, which is below the
adapter's posture — the master key is in the process, and the private half
in memory at every signing. And the separation the port is made for, a
publisher that cannot sign, neither gives on PostgreSQL: both need the
master key, which opens everything. Found unweighed by the review of
2026-10-23, and weighed then.

**Both, sealed for reading and derived at signing.** Covers both
properties: the pair checked where the private half is unwrapped anyway,
nothing unwrapped to verify or to publish. It replaces the derivation rather
than stands beside it, at the price of a column, a migration of every row,
and a fourth run of the custody tests; deferred until a publisher of public
keys runs on the PostgreSQL adapter, which none does.

**A switch between the two.** The property the adapter holds would then be
the configuration's, for a difference below its posture; two formats of the
table and a doubled test matrix to hold it — the ground on which ADR-0020
refused a type-state switch.

**An unexplained `400` left to Vault.** Contradicts ADR-0017 point 5: a
request the adapter itself composes is not mended by a retry.

**A JWK type in this crate.** JOSE is the consumer's; the crate fixes the
bytes a JWK's `x` and `y` are cut from and says where.

**Telling the refusals apart by Vault's text.** Works on this release and
breaks silently on the next (ADR-0016 point 6).
