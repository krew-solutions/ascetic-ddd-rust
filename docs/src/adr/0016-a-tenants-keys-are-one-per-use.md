# ADR-0016: A tenant's keys are one per use — the KEK, a signing key per algorithm, a MAC key per purpose

## Status

Accepted (2026-10-13): held by `tests/vault_signing.rs` and
`tests/vault_mapping.rs` of `ascetic-ddd-kms` (the ports on a Vault dev
server; the adapter's reading of Transit's answers against a scripted
transport), `tests/pg_signing.rs` (the same properties on the PostgreSQL
adapter), and `tests/in_memory_keys.rs` (the in-memory keys behave as the
adapters do where a consumer's tests would notice).

Amended the same day: the PostgreSQL adapter serves the signing and MAC
ports too, so that the two adapters of the crate serve the same three
ports. The first draft left it out — a private key under a master key in a
general-purpose table is not the posture the ports were made for — but the
KEK port already takes that posture as its simple adapter and says where
the requirements are higher Vault is the answer; an adapter that serves one
port and refuses two would have had the consumer choose its KMS per port.
What the amendment changes is in point 1 and in the consequences; the
separation of the keys, and everything that rests on it, is as it was.

## Context

`ascetic-ddd-kms` managed one kind of key: the key-encryption key of a
tenant, wrapping the data-encryption keys of `ascetic-ddd-dek`. A consumer
then needed two more operations under a tenant's keys — a digital signature
over artefacts that other parties verify, and a MAC that tags a draft so
that a stale edit is refused — with the requirement that the keys live in
the KMS and never enter the service's process: a tenant-bound HSM posture,
and a verification that a regulated tenant may demand run in a validated
module rather than in a library the service links.

Three ways were open. The service speaks Vault Transit itself: a client
beside the seedwork's, for the one gap the seedwork has. The existing port
does the work by envelope: the service draws a signing seed, wraps it under
the tenant's KEK with `encrypt_dek`, keeps the wrapped seed in its own
table, unwraps it to sign in process — nothing in the seedwork changes, and
the signing key is in the memory of every replica and in a general-purpose
table, which is what the posture forbids. Or the seedwork gains signing,
with the keys where the KEK is.

Measured on Vault 1.21 OSS Transit, and the reasoning rests on it: a key
name is `[A-Za-z0-9._-]`, a `/` is "unsupported path" even percent-encoded;
`POST keys/{name}` with a type is idempotent; a key of any type carries an
independent HMAC key per version, and `hmac` is a key type of its own; an
ECDSA signature is ASN.1 DER unless `marshaling_algorithm: jws`, which
gives `r || s`; a signature by a key the tenant has not got, by a version
newer than the key's latest, and by a version retired through
`min_decryption_version` are all HTTP 400, differing in text only; after a
rotation the earlier versions still verify; ML-DSA is an Enterprise key
type.

## Decision

1. **Two more ports beside `KeyManagementService`, not methods on it.**
   `SigningService` — `sign`, `verify`, `rotate_signing_key`,
   `delete_signing_key` — and `MacService` — `mac`, `delete_mac_key`. The
   KEK port has consumers that need no signing: `ascetic-ddd-dek` is bound
   by `K: KeyManagementService<S>` and nothing more, and must not grow a
   bound it does not use; `Cached<K>` caches unwraps, and a verification is
   not a function of its key, so it passes the other ports through
   uncached rather than caching them wrongly. A signature and a MAC are
   apart too: they share no value type and no consumer, and a MAC is
   checked by recomputation, so its port only makes them. Both adapters
   serve all three ports: Vault with the keys in Transit, PostgreSQL with
   the keys under its master key and the cryptography in process, the same
   byte forms on either.
2. **One key per use, three keys per tenant, in one mount, told apart by
   name.** The KEK stays the tenant's id, what the Python and Go ports
   name; a signing key is `{tenant}.sign.{algorithm}`, a MAC key
   `{tenant}.mac.{purpose}`. Nothing is derived from the KEK, although
   Transit would let the KEK's HMAC component make the MACs: the three
   keys have three lifecycles, and one key would couple them. Rotating the
   KEK rewraps the DEKs and the data stays readable; rotating a signing key
   makes new artefacts verify under the new version and old ones under
   theirs; rotating a MAC key makes every open draft stale at its next
   write. Deleting the KEK is crypto-shredding of the tenant's data;
   deleting a signing key is the end of verifiability of everything it
   signed. Under one key, a rotation for the data's sake would have broken
   the open drafts, and a signing key could not have been deleted at
   off-boarding without destroying the data.
3. **The dot is the separator, and the tenant's id is checked, not
   encoded.** It is the one character Transit allows that neither part may
   contain; for the dotted names the tenant's id must be letters, digits,
   `_` and `-`, else `Malformed`. The KEK's percent-encoding is not copied:
   it does not make a `/` acceptable, so it is a latent failure rather than
   a feature.
4. **The algorithm is an input of signing and a property of the
   signature.** `sign` takes a `SignatureAlgorithm`; a `Signature` carries
   it, the version of the key that made it, and the bytes in the one form
   fixed per algorithm — Ed25519's sixty-four, ECDSA's `r || s` through JWS
   marshaling, never DER — so that two encodings of one algorithm are never
   side by side and a verifier that reads the algorithm knows the bytes. A
   tenant moves to another algorithm by signing with it: a new key, not a
   rotation; what the old key signed keeps verifying under its own.
5. **Signing makes the key, verifying never does.** `sign` and `mac` ensure
   the key before the operation (idempotent, no read first). `verify` on a
   missing key is `SigningKeyNotFound`: creating on verify would make a
   forged artefact's key appear, and a key deleted and re-created starts at
   version 1, so an old `v1` signature would then fail as invalid instead
   of as unknown.
6. **A `400` is explained by one read of the key, not by Vault's text.** On
   a `400` from `sign`, `verify` or `hmac` the adapter reads
   `GET keys/{name}` once: a `404` is the key missing, a version outside
   `[min_decryption_version, latest_version]` is `NoKeyOfVersion`, anything
   else stays `Error::Vault`. Vault's texts are not matched. The
   distinction is what lets a consumer tell "retry later" from "this
   artefact names a key the tenant has not got".
7. **Verification goes through the KMS.** The seedwork links no signature
   library for the Vault adapter, and a tenant that must verify in a
   validated module gets that only there. The public key of a version is
   available from `GET keys/{name}` for a verifier elsewhere, and is not a
   method of the port until one is needed.
8. **The keys are the model's; the in-memory keys live in the seedwork.**
   A `SigningKey` and a `MacKey` stand in `domain` beside the `Kek`, one
   version of a keyring each, taking Ed25519, ECDSA P-256 and HMAC-SHA-256
   from the libraries as the cipher takes AES-GCM: what a signing key is
   and does is the crate's model, not a helper of one adapter, so the
   PostgreSQL adapter and `testing::InMemoryKeys` hold the same type and
   cannot drift apart in a byte form. A consumer writes one adapter over
   the ports and runs it in its tests over the in-memory keys and in
   production over Vault, so what the tests exercise is the mapping that
   runs — the form of a key id, how the adapter's errors become the
   consumer's, the text of a tag — with nothing but the network behind the
   port replaced; a key registered from a seed signs the same from run to
   run. The cost is the libraries as dependencies of the crate, as
   `aes-gcm` already is.

## Consequences

- A tenant has three keys and three deletes: `delete_kek` does not touch
  the signing or MAC keys. Off-boarding a tenant is three calls, one per
  port, or a mount per tenant that is unmounted whole; a consumer that
  shreds data only has shredded data only.
- Rotation is per key and per port. The MAC key is rotated outside the
  port today — by an operator, on the mount — and the consumer's etags are
  recomputed under the current version: a rotation between a read and a
  write is one refusal and a re-read.
- One `VaultTransitService` serves all three ports over one mount and one
  token; a deployment that wants a mount per tenant composes one service
  per mount, the ports do not change.
- One `PgKeyManagementService` serves all three ports over three tables
  and one master key: the signing key's private half and the MAC key are
  wrapped by the master key under the key's name as associated data, so a
  key wrapped for one tenant, algorithm or purpose does not open as
  another's, and a key wrapped under one master key does not open under
  another. A keyring's first key and its rotations are under the same
  advisory lock a tenant's KEK is. The posture is the KEK's: the simple
  adapter, for where it is enough.
- The failure path costs one extra call; the success path none.
- The signature's byte form is part of the contract: a consumer that
  stores or transmits a `Signature` stores the algorithm and the version
  beside the bytes, and reads them back into one.
- `SignatureAlgorithm` is open (`non_exhaustive`): ML-DSA joins when a
  Vault build at hand has it, and a consumer's exhaustive match needs a
  wildcard until then.

## Alternatives rejected

**Signing by the consumer's own Vault client.** A second client for one
gap; the seedwork exists so that the gap is filled once.

**Envelope: a signing seed wrapped by the KEK, signing in process.**
Needs no new port, and puts the signing key in every replica's memory and
in a general-purpose table — the posture the KMS is there for, given up.

**`sign` and `mac` on `KeyManagementService`.** `ascetic-ddd-dek`, the
PostgreSQL adapter and `Cached<K>` would each implement what they must
refuse or cannot serve.

**One port for signing and MACs.** No shared value type, no shared
consumer; a MAC is recomputed, a signature is verified.

**The MAC from the signing key's HMAC component, or from the KEK's.**
Transit allows it and it saves a key; it ties the open drafts to the
signing key's or the data's rotation.

**Local verification with the fetched public key, on the Vault adapter.**
Cheaper per verification, and not the validated module a regulated tenant
verifies in. The PostgreSQL adapter verifies locally by nature — the keys
are its — with the model's `PublicKey`.

**The primitives as a helper module beside the adapters** (the first
draft, `software.rs`, behind a feature). Answered what the crate does with
a signing key without being part of its model, so the model had the
signature and not the key that makes it; moved into `domain` as `Kek`'s
peers.

**Telling the `400`s apart by Vault's text.** Works on this release, and
breaks silently on the next.

**No PostgreSQL adapter for signing** (the first draft). The posture is
lower than Vault's; so is the KEK's in the same adapter, by the same
choice, and an adapter serving one port of three would have the consumer
choose its KMS per port. Withdrawn by the amendment.

**`verify` answering `bool`.** A `bool` is droppable; an error rides `?`,
as `Decrypt` already does.

**The consumer keeping its own in-memory key directory.** Three ports
implemented twice, and the production mapping tested by nothing.
