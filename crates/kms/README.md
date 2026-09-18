# ascetic-ddd-kms

Key management for envelope encryption. A tenant has a key-encryption key,
versioned and rotated, that wraps the data-encryption keys its data is sealed
with; deleting the tenant's key-encryption keys is crypto-shredding. One port
and two adapters: keys in a PostgreSQL table under a master key, or in
HashiCorp Vault Transit. A port of `ascetic_ddd.kms` (Python) and
`asceticddd/kms` (Go); the encryption stage ADR-0002 places before the outbox.

```rust
use ascetic_ddd_kms::{Algorithm, MasterKey};
# fn main() -> Result<(), ascetic_ddd_kms::Error> {
let master_key = Algorithm::Aes256Gcm.generate_key()?;      // from configuration, in practice
let master = MasterKey::new(master_key, Algorithm::Aes256Gcm)?;

let kek = master.generate_kek("tenant-1")?;                  // version 1, wrapped by the master
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

Three kinds of key and one operation between them, in `domain`. The
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
lock on the tenant, held to the end of the caller's transaction, so two
transactions meeting a new tenant at once make one key, and two rotations at
once make versions two and three; reads take nothing. READ COMMITTED is
assumed.

The PostgreSQL adapter keeps KEKs in a general-purpose database, wrapped
by a master key the process holds; it is the simple adapter, not key
management hardware. The master key is thirty-two bytes and comes from a
secret manager or the environment, never from source or configuration under
version control; the session may belong to a database other than the
data's. Where the requirements are higher, Vault Transit or a cloud KMS
behind the same port is the answer.

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
application.

## What is not here

Nothing is cached: every call reads the tenant's key from the table and
unwraps it. A DEK store — a key per stream, wrapped by these — is the next
port, `seedwork/infrastructure/repository/dek_store` in the sources.

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

## Testing

```bash
cargo test -p ascetic-ddd-kms                                    # keys and ciphers, no services
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-kms --features pg -- --ignored    # PostgreSQL
ASCETIC_DDD_TEST_VAULT_ADDR=http://localhost:8200 ASCETIC_DDD_TEST_VAULT_TOKEN=test-root-token \
    cargo test -p ascetic-ddd-kms --features reqwest -- --ignored   # Vault Transit, a dev server
```
