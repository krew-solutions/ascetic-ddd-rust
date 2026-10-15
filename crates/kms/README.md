# ascetic-ddd-kms

Key management for a tenant's keys. For envelope encryption: a tenant has a
key-encryption key, versioned and rotated, that wraps the data-encryption keys
its data is sealed with; deleting the tenant's key-encryption keys is
crypto-shredding. For what a tenant signs and authenticates: two more ports —
digital signatures under the tenant's key of an algorithm, MACs under the
tenant's key for a purpose. Three ports, two adapters serving all three: keys
in PostgreSQL tables under a master key, the signing and the MACs done in
this process; or in HashiCorp Vault Transit, where the keys stay and the
cryptography is Vault's. In-memory keys serve the signing and MAC ports for
the tests of what consumes them. A port of `ascetic_ddd.kms` (Python) and
`asceticddd/kms` (Go), which have the first port only; the encryption stage
ADR-0002 places before the outbox.

```rust
use ascetic_ddd_kms::{Algorithm, MasterKey};
# fn main() -> Result<(), ascetic_ddd_kms::Error> {
let master_key = Algorithm::Aes256Gcm.generate_key()?;      // from configuration, in practice
let master = MasterKey::new(master_key, Algorithm::Aes256Gcm)?;

let kek = master.generate_kek("tenant-1", None)?;            // version 1, wrapped by the master; no derivation context
let (dek, wrapped) = kek.generate_dek()?;                    // wrapped names the KEK's version
assert_eq!(wrapped.key_version(), 1);
assert_eq!(kek.unwrap(&wrapped)?, dek);

let rotated = master.rotate_kek(&kek)?;                      // version 2
let rewrapped = rotated.rewrap(&wrapped, &kek)?;             // what a DEK goes through after a rotation
assert_eq!(rotated.unwrap(&rewrapped)?, dek);
# Ok(()) }
```

The service, inside the caller's transaction (feature `pg`):

```rust,no_run
# #[cfg(feature = "pg")]
# async fn example(sessions: ascetic_ddd_session::PgSessionPool, master_key: ascetic_ddd_kms::Key)
# -> Result<(), ascetic_ddd_kms::Error> {
use ascetic_ddd_kms::{KeyManagementService, PgKeyManagementService};
use ascetic_ddd_session::{Session, SessionPool};

let kms = PgKeyManagementService::new(master_key);
sessions.session(async |session| {
    kms.setup(&session).await?;
    session.atomic(async |tx| {
        let (dek, wrapped) = kms.generate_dek(&tx, "tenant-1").await?;   // the KEK is made on first contact
        assert_eq!(kms.decrypt_dek(&tx, "tenant-1", &wrapped).await?, dek);
        let version = kms.rotate_kek(&tx, "tenant-1").await?;             // 2; what version 1 wrapped still unwraps
        let rewrapped = kms.rewrap_dek(&tx, "tenant-1", &wrapped).await?; // under version 2 now
        assert_eq!(version, 2);
        assert_eq!(kms.decrypt_dek(&tx, "tenant-1", &rewrapped).await?, dek);
        Ok(())
    }).await
}).await
# }
# fn main() {}
```

## The model

Three kinds of key and one operation between them, in `domain`, and beside
them a tenant's other keys. The
`MasterKey` — the one key of the system, from configuration — wraps a
tenant's key-encryption keys: it makes the first `Kek` of a tenant, loads one
back from its wrapped form, and rotates one to the next version. A KEK wraps
data-encryption keys: `wrap`, `unwrap`, `rewrap` after a rotation,
`generate_dek`. A `WrappedKey` names the version of the key that wrapped it,
four big-endian bytes in front on the wire, so the right version is reached
for when it is unwrapped, and a key asked to unwrap another version's work
says so before it tries. The tenant's id is the associated data of every
wrapping, so a key wrapped for one tenant does not unwrap under another
tenant's key, even one of the same bytes. A key made by a key is of its
maker's kind: a KEK has the master key's algorithm, a DEK the KEK's.

Under the hierarchy sits the primitive. A `Cipher` seals bytes under a key
and associated data, opens them again, and makes fresh keys for ciphers of
its own kind; `Algorithm` names the ciphers there are — AES-256-GCM — and
makes one from a key; what an algorithm knows, the sizes of its key, nonce
and tag and how to draw a key, lives with its adapter, `Aes256Gcm`, which
takes it from the library. A `Key` is bytes that are wiped when dropped and
never printed. Sealing draws a fresh nonce from the operating system; the
half that takes the nonce is not public, and is checked against what the
Python port sealed.

A tenant's other keys are one version of a keyring each, as the `Kek` is: a
`SigningKey` of a `SignatureAlgorithm` — its private half thirty-two bytes
wiped when dropped, its `PublicKey` derived from it — makes a `Signature`
of its algorithm and version, and the public half verifies one; a `MacKey`
for a purpose makes a `Mac`. They take the primitives from the libraries
as the cipher does — Ed25519, ECDSA P-256 with SHA-256, HMAC-SHA-256 — in
the forms the algorithm fixes, and each knows its name, `{tenant}.sign.
{algorithm}` or `{tenant}.mac.{purpose}`, which is Transit's key name on
one adapter and the associated data of the wrapping on the other.

## The port and the adapters

`KeyManagementService<S>` is the surface of Vault Transit: `encrypt_dek`,
`decrypt_dek`, `generate_dek`, `rotate_kek`, `rewrap_dek`, `delete_kek`,
each in the caller's session. A wrapped DEK is bytes opaque to the caller, in
the adapter's own form.

`PgKeyManagementService` keeps KEKs in `kms_keys` — the table the Python and
Go ports write, so a key made by one port is read by another; a test unwraps
what the Python port wrapped — each wrapped by the master key the service was
built with. `setup` creates the table in one transaction under an advisory
lock on its name. Making a tenant's first key and rotating take an advisory
lock on the tenant in a scope the adapter opens itself — a savepoint inside
the caller's transaction, a transaction of its own outside one, where the
lock would otherwise end with its own statement — so two transactions
meeting a new tenant at once make one key, and two rotations at once make
versions two and three; reads take nothing. READ COMMITTED is assumed.

The PostgreSQL adapter keeps KEKs in a general-purpose database, wrapped
by a master key the process holds; it is the simple adapter, not key
management hardware. The master key is thirty-two bytes and comes from a
secret manager or the environment, never from source or configuration under
version control; the session may belong to a database other than the
data's. Where the requirements are higher, Vault Transit or a cloud KMS
behind the same port is the answer.

The same adapter serves the signing and MAC ports: signing keys in
`kms_signing_keys` by tenant, algorithm and version — the private half
wrapped by the master key under the name `{tenant}.sign.{algorithm}` as its
associated data, the public half beside it — and MAC keys in `kms_mac_keys`
by tenant, purpose and version, wrapped under `{tenant}.mac.{purpose}`; the
signing, verifying and MACs are the model's `SigningKey`, `PublicKey` and
`MacKey`, in this process, in the byte forms the ports fix, the same the
Vault adapter asks Transit for. A keyring's first key and its rotations are
under an advisory lock on the key's name, as a tenant's KEK is; `setup`
makes the three tables under a lock on each name, since two services may
share one of the tables and not the others. A key wrapped under one master
key does not open under another: `Decrypt`.

Two settings of either adapter serve the bus (ADR-0017). Built
`with_derivation_context(channel)`, the service serves a derived key: on
Vault every KEK operation carries the context, base64, and a KEK the service
makes is `derived`; on PostgreSQL the context is bound into the wrap's
associated data beside the tenant. Either way a DEK wrapped for one channel
does not unwrap for another, and the composition root builds one instance,
one `Cached` and one stage per channel, from the channel's name in the
contract. Built `managed_keys()`, the service makes no KEK on first contact:
a tenant without one is `KekNotFound` on every wrap, and the key is made by
whoever has custody through `KeyCustody<S>` — `create_kek` with
`KekOptions` (`derived`, `auto_rotate_period`, which the PostgreSQL adapter
refuses as `Unsupported`, having no scheduler) and
`retire_kek_versions_before` (Transit's `min_decryption_version`; a
deletion of the rows on PostgreSQL) — a port beside `KeyManagementService`,
held by the tenant-management context and by nobody who merely wraps.
On Vault a refusal of a KEK operation is read once: a `400` is the key
missing (`KekNotFound`), a version retired (`NoKeyOfVersion`), or — the key
there and the version served — the request's own fault, `Decrypt` on
`decrypt` and `rewrap` (a wrong context, bytes changed), `Malformed`
elsewhere; a `403` with the key there is `Forbidden`, which no retry under
the same identity and context mends.

`SigningService<S>` is signatures under the tenant's key of one algorithm —
`sign`, `verify`, `rotate_signing_key`, `delete_signing_key` — one key per
tenant and algorithm (`SignatureAlgorithm`: Ed25519, ECDSA P-256 with
SHA-256), made on first signing; the latest version signs, every version
verifies until an operator retires it. The algorithm is an input of signing
and a property of the `Signature`, which carries it, the version of the key
that made it, and the bytes in the one form the crate fixes per algorithm —
Ed25519's sixty-four, ECDSA's `r || s`, never DER — so a tenant moves to
another algorithm by signing with it, a new key rather than a rotation, while
what the old key signed keeps verifying. Verifying answers `Ok(())` or
`InvalidSignature`; a key the tenant has not got is `SigningKeyNotFound`, a
version the key has not got or no longer serves `NoKeyOfVersion`; verifying
never makes a key. Deleting a signing key ends the verifiability of
everything it signed: the tenant's off-boarding, never a rotation.

`MacService<S>` is HMAC-SHA-256 under the tenant's key for one purpose, a
`KeyLabel` such as `draft-etag` — `mac`, `delete_mac_key` — one key per tenant
and purpose, made on first use; a `Mac` carries the version of the key that
made it, and is checked by recomputation, so the port only makes them. Keeping
MAC keys apart from signing keys by name keeps their lifecycles apart: a
signing rotation does not change what a MAC key makes. Why the ports are
three and the keys one per use, and what that costs at off-boarding, is
ADR-0016.

`Cached<K>` keeps what any service unwraps, a thousand keys for five minutes
unless told otherwise, so a consumer opening a thousand messages sealed
under one DEK, or a store loading every version of a stream's keys, asks the
KMS once — what makes Vault, a network away, bearable on a hot path.
Deleting a tenant's KEK forgets the tenant's keys held here; elsewhere the
time to live is the bound, so a shredded tenant's keys open for that long at
most, which is the price, with keys held in memory for that long.

`VaultTransitService` (feature `vault`) leaves keys and cryptography to
Vault; the wrapped DEK is Vault's ciphertext text as bytes. The HTTP client is
the session's, `HttpAccess`, and every call goes through the session so its
observer times it; the client speaks to Vault through `VaultTransport`,
implemented for `reqwest::Client` under the `reqwest` feature (TLS is the
application's choice of `reqwest` feature) and for any other client by the
application. The same struct serves all three ports, over one mount, telling
a tenant's keys apart by name: the KEK is the tenant's id (what the Python and
Go ports name), a signing key `{tenant}.sign.{algorithm}` (Transit types
`ed25519`, `ecdsa-p256`), a MAC key `{tenant}.mac.{purpose}` (type `hmac`).
The dot is the separator Transit allows that the parts cannot contain, so for
the dotted names the tenant's id is checked to be letters, digits, `_` and `-`
rather than encoded: a `/` is "unsupported path" to Transit even encoded.
Signatures are asked for and verified in JWS marshaling, which is what makes
ECDSA's bytes `r || s`; a test verifies a signature Vault made under the
public key Transit publishes, read into the model's `PublicKey`, so the two
adapters' byte forms are one. Transit answers `400` both for a key it has not got
and for a version a key does not serve; the adapter reads the key once, on
that path only, to tell `SigningKeyNotFound` and `NoKeyOfVersion` apart,
rather than matching Vault's text.

`testing::InMemoryKeys` serves the signing and MAC ports from memory, with
the model's keys — a key registered from a seed so that a test's signatures
are the same from run to run (Ed25519), or made on first signing as Vault
would; MAC keys made on first use — so that the adapter a consumer writes
over the ports is exercised as it is in production, with the network behind
the port and nothing else replaced.

`Cached<K>` passes the signing and MAC ports through to `K` uncached: a
signature and a verification are not functions of the cache's key. One
`Cached<K>` then stands for every port `K` serves.

## What is not here

The data-encryption keys themselves — one per resource, versioned, wrapped
by these — are the lower half of envelope encryption and live in
`ascetic-ddd-dek`: the store that keeps them and the stage that seals with
them, over any `KeyManagementService`. Caching is `Cached<K>` above, in
front of whichever service a consumer unwraps through.

A public key is not served by the port (a verifier elsewhere reads it from
`GET keys/{name}`, `data.keys.{n}.public_key`, or from the `public_key`
column), a MAC is not verified by the port, an operator retires a version
outside it, and ML-DSA waits for a Vault build and a library that have it.
Each comes with its first user.

## Deviations from the Python and Go sources

* A wrapped key is a value, `WrappedKey`, parsed once; a key asked to unwrap
  another version's work says so, `WrongKeyVersion`, where the sources tried
  the key and reported the tag failure — or, in Go, sliced short input and
  panicked. Short input is `Malformed`. Keys `wrap` and `unwrap`; only a
  `Cipher` encrypts.
* A tenant's id is text. The sources take `Any` and format it for the
  associated data anyway; the table's column is `VARCHAR`.
* Making a tenant's first key and rotating are under a per-tenant advisory
  lock. In the sources two transactions meeting a new tenant at once both
  insert version 1, and the second fails on the primary key.
* `setup` is the adapter's, as in the outbox and the inbox; `cleanup` did
  nothing in any port and is gone. A KEK carries no `created_at`: no port
  ever wrote it, the column's default does, and nothing read it.
* The master key is not bound to a tenant; the tenant is named at each
  operation, as the associated data of the wrapping. `Kek::rewrap` takes the
  earlier version it rewraps from, where the sources' `rewrap` under the same
  key only changed the nonce. Nothing takes an algorithm: a key made by a
  cipher is for a cipher of its kind, so a KEK is of the master key's
  algorithm and a DEK of the KEK's, where the sources labelled both
  `AES-256-GCM` by constant, whatever made the bytes.
* Sealing can fail — the operating system may give no randomness — so
  `Cipher::encrypt` returns a `Result`, as Go's does; keys are wiped when
  dropped and print no bytes.
* The Vault adapter fixes no HTTP library: the session carries the client,
  and `VaultTransport` is what the adapter needs of it. Vault's error text is
  kept in `Error::Vault`.
* The signing and MAC ports, their tables `kms_signing_keys` and
  `kms_mac_keys`, and the Transit key names beside the KEK's are this
  port's; the sources have the KEK port only.

## Testing

```bash
cargo test -p ascetic-ddd-kms                                    # keys and ciphers, no services
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-kms --features pg -- --ignored    # PostgreSQL: KEKs, signing, MACs
ASCETIC_DDD_TEST_VAULT_ADDR=http://localhost:8200 ASCETIC_DDD_TEST_VAULT_TOKEN=test-root-token \
    cargo test -p ascetic-ddd-kms --features reqwest -- --ignored   # Vault Transit, a dev server
cargo test -p ascetic-ddd-kms --features vault                        # in-memory keys; the Vault
                                                                      # adapter's mapping, no server
```

The Vault tests need the `transit` engine enabled on the dev server.
