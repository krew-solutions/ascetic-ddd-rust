# ascetic-ddd-dek

Data-encryption keys, one per resource and versioned, wrapped by the
tenant's key-encryption key through `ascetic-ddd-kms`: the store that keeps
them, and the ciphers a codec seals with. The lower half of envelope
encryption — the KMS is the upper — for whatever a repository keeps under a
key of its own: an aggregate's stream in an event store, a document. A port
of the `DekStore` of `ascetic_ddd.seedwork.infrastructure.repository`
(Python), with the resource named instead of the stream.

```rust,no_run
# #[cfg(feature = "pg")]
# async fn example(sessions: ascetic_ddd_session::PgSessionPool, master_key: ascetic_ddd_kms::Key)
# -> Result<(), ascetic_ddd_dek::Error> {
use ascetic_ddd_dek::{DekStore, PgDekStore, Resource};
use ascetic_ddd_kms::{Cipher, PgKeyManagementService};
use ascetic_ddd_session::{Session, SessionPool};

let deks = PgDekStore::new(PgKeyManagementService::new(master_key));
let order = Resource::new("tenant-1", "Order", "order-7");
sessions.session(async |session| {
    deks.kms().setup(&session).await?;
    deks.setup(&session).await?;
    session.atomic(async |tx| {
        let cipher = deks.get_or_create(&tx, &order).await?;      // the DEK is made on first contact
        let sealed = cipher.encrypt(b"the order's events")?;      // names the DEK's version
        let keyring = deks.get_all(&tx, &order).await?;           // every version, for reading back
        assert_eq!(keyring.decrypt(&sealed)?, b"the order's events");
        Ok::<_, ascetic_ddd_dek::Error>(())
    }).await
}).await
# }
# fn main() {}
```

## The model

A DEK belongs to a `Resource`: one thing of one tenant, named by its kind
and its id, the id as JSON so a composite id fits. The resource's canonical
text — the three as a JSON array, object keys in order — is the associated
data of its ciphers: a ciphertext of one resource does not open under
another resource's key, even one of the same bytes, and the text must never
change for a resource that has data.

A codec sees a resource's DEKs as `Cipher`s over bytes that carry the key
version along, four big-endian bytes in front, the layout of the sources and
of a wrapped key in the KMS. A `VersionedCipher` is one version — it seals
under that version and refuses another version's work before the key is
tried — and is what the write path gets. A `Keyring` is every version the
resource has — it seals under the newest and opens whatever version a
ciphertext names — and is what the read path gets. The version is read here,
in memory, to pick a key; the KMS reads it to fetch one from its store.

## The port and the adapter

`DekStore<S>` in the caller's session: `get_or_create` for the write path,
making version 1 on first contact; `get_all` for the read path; `get` for a
version already known; `rewrap` after the tenant's KEK rotated, wrapping the
tenant's DEKs again and saying how many; `delete` to forget one resource for
good. Deleting the tenant's KEKs in the KMS forgets every resource of the
tenant at once.

`PgDekStore` keeps DEKs in `deks`, each wrapped by the tenant's current KEK
through the `KeyManagementService` it is built with, and labelled with the
DEK's algorithm, AES-256-GCM unless `with_algorithm` says otherwise. `setup`
creates the table in one transaction under an advisory lock on its name;
making a resource's first DEK takes an advisory lock on the resource, held
to the end of the caller's transaction, so two transactions meeting a new
resource at once make one key. Reads take nothing. READ COMMITTED is
assumed.

## The envelope stage of the bus

`EnvelopeStage` (feature `bus`) is the sealing stage ADR-0002 places before
the outbox and after the inbox: a fresh DEK per message, drawn for the
message's tenant through the KMS, seals the payload; the DEK travels in the
`dek` header, wrapped by the tenant's KEK, base64, with the cipher's name in
`dek_algorithm`. The receiving side needs no table of keys, only a KMS that
holds the tenant's KEK — which is what lets a message cross to another
bounded context. The stage reaches the KMS through a session pool of its
own, since the KMS's session is not the data's. A message without a
`tenant_id` header, or one whose key or payload does not open, or whose
tenant's KEK is gone, is refused for good, `Permanent`, and the inbox parks
it; a KMS out of reach is a failure of the moment, and the message is tried
again.

```rust,ignore
let sealing = Arc::new(EnvelopeStage::new(kms_sessions, kms));
let placed = outbox.producer("kafka://orders", encode).through(Arc::clone(&sealing));
let orders = inbox.consumer(decode).through(sealing);
```

## What is not here

Nothing is cached: every call reads the resource's rows and unwraps them
through the KMS. A second DEK version for a resource is never made here:
versions are for an algorithm migration, and the row that starts one is the
migration's to write. The codecs that compose a cipher with serialization —
`EncryptionCodec`, `ZlibCodec`, `JsonCodec` in the sources — belong to the
repository that chains them.

## Deviations from the Python and Go sources

* The key of the store is a `Resource` — tenant, kind, id — not the event
  store's `StreamId`: the same triple, named for what a DEK is for rather
  than for one kind of repository. The table is `deks`, its columns named
  after the resource. A repository converts its own identity with `From`.
* The associated data of a resource's ciphers is the resource's canonical
  JSON text. The sources bind with `str(stream_id)`: Python's `repr` of a
  dataclass with quotes, Go's `fmt.Sprintf` without, so a payload sealed by
  one port does not open in the other. Rust's text is the canon, to be
  carried back; until then a payload sealed by the Python port does not open
  here, while its wrapped DEKs do load.
* The write path returns a `VersionedCipher` and the read path a `Keyring`,
  where the sources return the cipher interface for both: a codec takes
  either, and a reader of the port sees which path gets what. A
  `VersionedCipher` refuses another version's work, where the sources
  stripped the prefix unread.
* Making a resource's first DEK is under a per-resource advisory lock. In
  the sources two transactions meeting a new resource at once both insert
  version 1, and the second fails on the primary key.
* `setup` is the adapter's and `cleanup` is gone, as in the KMS.

## Testing

```bash
cargo test -p ascetic-ddd-dek                                    # the model, no database
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db \
    cargo test -p ascetic-ddd-dek --all-features -- --ignored   # the store and the stage on PostgreSQL
```
