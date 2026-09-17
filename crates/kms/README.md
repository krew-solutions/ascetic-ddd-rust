# ascetic-ddd-kms

Key management for envelope encryption. A tenant has a key-encryption key,
versioned and rotated, that wraps the data-encryption keys its data is sealed
with; deleting the tenant's key-encryption keys is crypto-shredding. One port
and two adapters: keys in a PostgreSQL table under a master key, or in
HashiCorp Vault Transit. A port of `ascetic_ddd.kms` (Python) and
`asceticddd/kms` (Go); the encryption stage ADR-0002 places before the outbox.

```rust
use ascetic_ddd_kms::{Algorithm, Key, MasterKey};
# fn main() -> Result<(), ascetic_ddd_kms::Error> {
let master_key = Algorithm::Aes256Gcm.generate_key()?;      // from configuration, in practice
let master = MasterKey::new("tenant-1", &master_key, Algorithm::Aes256Gcm)?;

let kek = master.generate_kek(Algorithm::Aes256Gcm)?;        // version 1, wrapped by the master
let dek = Algorithm::Aes256Gcm.generate_key()?;
let wrapped = kek.encrypt(dek.as_bytes())?;                  // names the KEK's version
assert_eq!(wrapped.key_version(), 1);
assert_eq!(kek.decrypt(&wrapped)?, dek.as_bytes());
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

## The keys

A `Cipher` seals bytes under a key and associated data, and opens them again;
`Algorithm` names the ciphers there are — AES-256-GCM — and chooses one, and
a `Key` is bytes that are wiped when dropped and never printed. The
associated data is the tenant's id at every level, so a ciphertext of one
tenant does not open under another tenant's key, even one of the same bytes.
Sealing draws a fresh nonce from the operating system; a `Nonce` is made
only that way and moved into the one sealing it is for, so there is no way
to seal twice under one nonce. `Aes256Gcm::seal` is the pure half, given the
nonce, and is checked against what the Python port sealed.

A `Ciphertext` is sealed bytes with the version of the key that sealed them,
four big-endian bytes in front on the wire. The `MasterKey`, version 1, wraps
a tenant's `Kek`s; a KEK is one version of the tenant's key — rotation makes
the next — and wraps DEKs. A key refuses a ciphertext of another version
before it tries to open it.

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

* A ciphertext is a value, `Ciphertext`, parsed once; a key asked to open one
  of another version says so, `WrongKeyVersion`, where the sources tried the
  key and reported the tag failure — or, in Go, sliced short input and
  panicked. Short input is `Malformed`.
* A tenant's id is text. The sources take `Any` and format it for the
  associated data anyway; the table's column is `VARCHAR`.
* Making a tenant's first key and rotating are under a per-tenant advisory
  lock. In the sources two transactions meeting a new tenant at once both
  insert version 1, and the second fails on the primary key.
* `setup` is the adapter's, as in the outbox and the inbox; `cleanup` did
  nothing in any port and is gone. A KEK carries no `created_at`: no port
  ever wrote it, the column's default does, and nothing read it.
* The key-level `rewrap` and `generate_key` are gone: a rewrap under the same
  key only changes the nonce, and generating is the service's `generate_dek`.
  `MasterKey::generate_kek` takes no tenant: the master key is bound to one.
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
