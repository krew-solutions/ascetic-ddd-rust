//! The PostgreSQL key management service: a tenant's KEKs in a table, each
//! wrapped by the master key.
//!
//! # A derivation context, and keys made elsewhere
//!
//! Built [`with_derivation_context`](PgKeyManagementService::with_derivation_context),
//! the service wraps and unwraps under associated data that binds the
//! wrap to the context beside the tenant: what was wrapped for one channel
//! of the bus does not unwrap for another, as under a derived key of
//! Vault's (ADR-0017). The rows are the same; the binding is the wrap's.
//! Built [`managed_keys`](PgKeyManagementService::managed_keys), the
//! service makes no KEK on first contact: a tenant without one is
//! [`Error::KekNotFound`] on every wrap, and the key is made by whoever
//! has custody ([`KeyCustody`]), under the same lock.
//!
//! # The table
//!
//! `kms_keys (tenant_id, key_version, encrypted_key, master_algorithm,
//! key_algorithm, created_at)`, primary key `(tenant_id, key_version)` — the
//! table the Python and Go ports write, so a key made by one port is read by
//! another; a test unwraps what the Python port wrapped. `encrypted_key` is a
//! [`WrappedKey`] under master key version 1; `master_algorithm` is the
//! master key's algorithm when the row was written, and is what the row is
//! read back with.
//!
//! # Locks
//!
//! Making a tenant's first key, and rotating, take
//! `pg_advisory_xact_lock(hashtext(table), hashtext(tenant_id))` in a scope
//! the adapter opens itself — a savepoint inside the caller's transaction,
//! where the lock lives to the end of that transaction; a transaction of its
//! own outside one, where the lock would otherwise end with its own
//! statement, before the read it guards. Two transactions meeting a new
//! tenant at once thus make one key, the second reading what the first
//! committed, instead of both making version 1 and one failing on the
//! primary key; two rotations at once make versions two and three, not one
//! and an error. Reads take nothing. READ COMMITTED is assumed: the read
//! after the lock is in a snapshot of its own, so it sees what the lock's
//! previous holder committed.

use std::num::NonZeroU32;

use ascetic_ddd_session::Session;
use ascetic_ddd_session::pg::{Identifier, PgAccess};
use tokio_postgres::Row;

use crate::domain::{
    Algorithm, Kek, Key, KeyLabel, Mac, MacKey, MasterKey, PublicKeyring, Signature,
    SignatureAlgorithm, SigningKey, WrappedKey,
};
use crate::error::Error;
use crate::port::{
    KekOptions, KeyCustody, KeyManagementService, MacService, PublicKeyService, SigningService,
};

/// The key management service over a table of KEKs wrapped by one master key.
pub struct PgKeyManagementService {
    master_key: Key,
    master_algorithm: Algorithm,
    table: Identifier,
    signing_table: Identifier,
    mac_table: Identifier,
    context: Option<String>,
    managed_keys: bool,
}

impl PgKeyManagementService {
    /// A service wrapping KEKs with `master_key` under AES-256-GCM, in table
    /// `kms_keys`.
    pub fn new(master_key: Key) -> Self {
        PgKeyManagementService {
            master_key,
            master_algorithm: Algorithm::Aes256Gcm,
            table: Identifier::new("kms_keys").expect("a literal identifier"),
            signing_table: Identifier::new("kms_signing_keys").expect("a literal identifier"),
            mac_table: Identifier::new("kms_mac_keys").expect("a literal identifier"),
            context: None,
            managed_keys: false,
        }
    }

    /// The same service wrapping and unwrapping under `context`, bound into
    /// the wrap's associated data beside the tenant: one instance per
    /// channel of the bus, built in the composition root from the channel's
    /// name (ADR-0017).
    pub fn with_derivation_context(self, context: impl Into<String>) -> Self {
        PgKeyManagementService {
            context: Some(context.into()),
            ..self
        }
    }

    /// The same service making no KEK on first contact: a tenant without
    /// one is [`Error::KekNotFound`] on every wrap. For a producer or a
    /// consumer of the bus, whose keys are made by whoever has custody.
    pub fn managed_keys(self) -> Self {
        PgKeyManagementService {
            managed_keys: true,
            ..self
        }
    }

    /// The same service wrapping with `algorithm`, which every KEK it makes
    /// from then on is of too. Rows already written keep the algorithms they
    /// name.
    pub fn with_master_algorithm(self, algorithm: Algorithm) -> Self {
        PgKeyManagementService {
            master_algorithm: algorithm,
            ..self
        }
    }

    /// The same service in another table. The name goes into SQL as text, so
    /// it is an [`Identifier`]: parsed once, safe after.
    pub fn with_table(self, table: Identifier) -> Self {
        PgKeyManagementService { table, ..self }
    }

    /// The table the KEKs are in.
    pub fn table(&self) -> &Identifier {
        &self.table
    }

    /// The same service keeping signing keys in another table.
    pub fn with_signing_table(self, table: Identifier) -> Self {
        PgKeyManagementService {
            signing_table: table,
            ..self
        }
    }

    /// The table of the signing keys.
    pub fn signing_table(&self) -> &Identifier {
        &self.signing_table
    }

    /// The same service keeping MAC keys in another table.
    pub fn with_mac_table(self, table: Identifier) -> Self {
        PgKeyManagementService {
            mac_table: table,
            ..self
        }
    }

    /// The table of the MAC keys.
    pub fn mac_table(&self) -> &Identifier {
        &self.mac_table
    }

    /// Creates the tables that are not there: the KEKs', the signing keys'
    /// and the MAC keys'. One transaction under an advisory lock on each
    /// table's name, taken in one order, so that two processes setting up
    /// at once do not race in `CREATE TABLE IF NOT EXISTS` — on any of the
    /// three, since two services may share one of the tables and not the
    /// others.
    pub async fn setup<S>(&self, session: &S) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        let table = &self.table;
        let signing = &self.signing_table;
        let mac = &self.mac_table;
        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table} (
                "tenant_id" VARCHAR(128) NOT NULL,
                "key_version" INTEGER NOT NULL,
                "encrypted_key" BYTEA NOT NULL,
                "master_algorithm" VARCHAR(32) NOT NULL,
                "key_algorithm" VARCHAR(32) NOT NULL,
                "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT {table}_pk PRIMARY KEY ("tenant_id", "key_version")
            );
            CREATE TABLE IF NOT EXISTS {signing} (
                "tenant_id" VARCHAR(128) NOT NULL,
                "algorithm" VARCHAR(32) NOT NULL,
                "key_version" INTEGER NOT NULL,
                "encrypted_key" BYTEA NOT NULL,
                "public_key" BYTEA NOT NULL,
                "master_algorithm" VARCHAR(32) NOT NULL,
                "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT {signing}_pk PRIMARY KEY ("tenant_id", "algorithm", "key_version")
            );
            CREATE TABLE IF NOT EXISTS {mac} (
                "tenant_id" VARCHAR(128) NOT NULL,
                "purpose" VARCHAR(64) NOT NULL,
                "key_version" INTEGER NOT NULL,
                "encrypted_key" BYTEA NOT NULL,
                "master_algorithm" VARCHAR(32) NOT NULL,
                "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT {mac}_pk PRIMARY KEY ("tenant_id", "purpose", "key_version")
            );
            "#
        );
        session
            .atomic(async |tx| {
                for name in [table, signing, mac] {
                    tx.connection()
                        .execute(
                            "SELECT pg_advisory_xact_lock(hashtext($1))",
                            &[&name.as_str()],
                        )
                        .await?;
                }
                tx.connection().batch_execute(&ddl).await?;
                Ok::<_, Error>(())
            })
            .await
    }

    /// The master key for `algorithm`: the configured one for what is made
    /// now, the row's for what is read back.
    fn master_key(&self, algorithm: Algorithm) -> Result<MasterKey, Error> {
        MasterKey::new(self.master_key.clone(), algorithm)
    }

    /// Serializes the making and rotating of one tenant's keys until the end
    /// of the transaction.
    async fn lock_tenant<S>(&self, session: &S, tenant_id: &str) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        Self::lock(session, &self.table, tenant_id).await
    }

    /// The advisory lock of one key's keyring in `table`, for the
    /// transaction: what makes a first key or a rotation holds it, so that
    /// two meeting a new keyring make one key, and two rotations make the
    /// next two versions.
    async fn lock<S>(session: &S, table: &Identifier, name: &str) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        session
            .connection()
            .execute(
                "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
                &[&table.as_str(), &name],
            )
            .await?;
        Ok(())
    }

    /// The latest signing key of the tenant and algorithm, `None` for none.
    async fn current_signing_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<Option<SigningKey>, Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "SELECT key_version, encrypted_key, public_key, master_algorithm FROM {} \
             WHERE tenant_id = $1 AND algorithm = $2 ORDER BY key_version DESC LIMIT 1",
            self.signing_table
        );
        let row = session
            .connection()
            .query_opt(&sql, &[&tenant_id, &algorithm.as_str()])
            .await?;
        row.map(|row| self.load_signing_key(tenant_id, algorithm, &row))
            .transpose()
    }

    /// Version `version` of the tenant's signing key of `algorithm`, its
    /// public half derived from the private half and checked against the
    /// one stored beside it (ADR-0019): what signs at a named version and
    /// what verifies. Nothing is made.
    ///
    /// # Errors
    /// [`Error::SigningKeyNotFound`] when the tenant has no key of the
    /// algorithm; [`Error::NoKeyOfVersion`] when it has the key and not that
    /// version; [`Error::Malformed`] when the stored public half is not the
    /// private half's; [`Error::Decrypt`] under another master key.
    async fn signing_key_of_version<S>(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
    ) -> Result<SigningKey, Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "SELECT key_version, encrypted_key, public_key, master_algorithm FROM {} \
             WHERE tenant_id = $1 AND algorithm = $2 AND key_version = $3",
            self.signing_table
        );
        let row = session
            .connection()
            .query_opt(
                &sql,
                &[&tenant_id, &algorithm.as_str(), &stored_version(version)?],
            )
            .await?;
        if let Some(row) = row {
            return self.checked_signing_key(tenant_id, algorithm, &row);
        }
        // No such version: the tenant may still have the key, at others.
        let any = format!(
            "SELECT 1 FROM {} WHERE tenant_id = $1 AND algorithm = $2 LIMIT 1",
            self.signing_table
        );
        match session
            .connection()
            .query_opt(&any, &[&tenant_id, &algorithm.as_str()])
            .await?
        {
            Some(_) => Err(Error::NoKeyOfVersion(version)),
            None => Err(Error::SigningKeyNotFound {
                tenant_id: tenant_id.to_owned(),
                algorithm,
            }),
        }
    }

    /// The key of a row with its public half derived, and the stored public
    /// half checked against it: a writer of the table without the master
    /// key can change the column, never the wrapped private half, so a
    /// mismatch is a tamper — refused rather than published or verified
    /// under (ADR-0019).
    fn checked_signing_key(
        &self,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        row: &Row,
    ) -> Result<SigningKey, Error> {
        let key = self.load_signing_key(tenant_id, algorithm, row)?;
        let stored: Vec<u8> = row.try_get("public_key")?;
        if stored.as_slice() == key.public().as_bytes() {
            Ok(key)
        } else {
            Err(Error::Malformed(format!(
                "the stored public half of `{}` v{} is not its private half's",
                key.name(),
                key.version()
            )))
        }
    }

    /// The latest signing key, made under the keyring's lock if the tenant
    /// has none of the algorithm.
    async fn get_or_create_signing_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<SigningKey, Error>
    where
        S: Session + PgAccess + Sync,
    {
        if let Some(found) = self
            .current_signing_key(session, tenant_id, algorithm)
            .await?
        {
            return Ok(found);
        }
        session
            .atomic(async |tx| {
                let name = SigningKey::name_of(tenant_id, algorithm);
                Self::lock(&tx, &self.signing_table, &name).await?;
                match self.current_signing_key(&tx, tenant_id, algorithm).await? {
                    Some(found) => Ok(found),
                    None => {
                        let key = SigningKey::generate(tenant_id, algorithm, 1)?;
                        self.save_signing_key(&tx, &key).await?;
                        Ok(key)
                    }
                }
            })
            .await
    }

    /// Saves the key: the private half wrapped by the master key under the
    /// key's name, the public half beside it.
    async fn save_signing_key<S>(&self, session: &S, key: &SigningKey) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        let wrapped = self
            .master_key(self.master_algorithm)?
            .wrap(&key.name(), key.private())?;
        let sql = format!(
            "INSERT INTO {} (tenant_id, algorithm, key_version, encrypted_key, public_key, master_algorithm) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            self.signing_table
        );
        session
            .connection()
            .execute(
                &sql,
                &[
                    &key.tenant_id(),
                    &key.algorithm().as_str(),
                    &stored_version(key.version())?,
                    &wrapped.to_bytes(),
                    &key.public().as_bytes(),
                    &self.master_algorithm.as_str(),
                ],
            )
            .await?;
        Ok(())
    }

    /// The key from its row: the private half unwrapped, the public half
    /// derived from it rather than read.
    fn load_signing_key(
        &self,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        row: &Row,
    ) -> Result<SigningKey, Error> {
        let version = read_version(row.try_get("key_version")?)?;
        let wrapped = WrappedKey::parse(&row.try_get::<_, Vec<u8>>("encrypted_key")?)?;
        let master_algorithm: Algorithm = row.try_get::<_, String>("master_algorithm")?.parse()?;
        let private = self
            .master_key(master_algorithm)?
            .unwrap(&SigningKey::name_of(tenant_id, algorithm), &wrapped)?;
        SigningKey::load(tenant_id, algorithm, version, private)
    }

    /// The latest MAC key of the tenant and purpose, `None` for none.
    async fn current_mac_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<Option<MacKey>, Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "SELECT key_version, encrypted_key, master_algorithm FROM {} \
             WHERE tenant_id = $1 AND purpose = $2 ORDER BY key_version DESC LIMIT 1",
            self.mac_table
        );
        let row = session
            .connection()
            .query_opt(&sql, &[&tenant_id, &purpose.as_str()])
            .await?;
        row.map(|row| {
            let version = read_version(row.try_get("key_version")?)?;
            let wrapped = WrappedKey::parse(&row.try_get::<_, Vec<u8>>("encrypted_key")?)?;
            let master_algorithm: Algorithm =
                row.try_get::<_, String>("master_algorithm")?.parse()?;
            let key = self
                .master_key(master_algorithm)?
                .unwrap(&MacKey::name_of(tenant_id, purpose), &wrapped)?;
            Ok(MacKey::load(tenant_id, purpose, version, key))
        })
        .transpose()
    }

    /// The latest MAC key, made under the keyring's lock if the tenant has
    /// none for the purpose.
    async fn get_or_create_mac_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<MacKey, Error>
    where
        S: Session + PgAccess + Sync,
    {
        if let Some(found) = self.current_mac_key(session, tenant_id, purpose).await? {
            return Ok(found);
        }
        session
            .atomic(async |tx| {
                let name = MacKey::name_of(tenant_id, purpose);
                Self::lock(&tx, &self.mac_table, &name).await?;
                match self.current_mac_key(&tx, tenant_id, purpose).await? {
                    Some(found) => Ok(found),
                    None => {
                        let key = MacKey::generate(tenant_id, purpose, 1)?;
                        let wrapped = self
                            .master_key(self.master_algorithm)?
                            .wrap(&name, key.key())?;
                        let sql = format!(
                            "INSERT INTO {} (tenant_id, purpose, key_version, encrypted_key, master_algorithm) \
                             VALUES ($1, $2, 1, $3, $4)",
                            self.mac_table
                        );
                        tx.connection()
                            .execute(
                                &sql,
                                &[
                                    &tenant_id,
                                    &purpose.as_str(),
                                    &wrapped.to_bytes(),
                                    &self.master_algorithm.as_str(),
                                ],
                            )
                            .await?;
                        Ok(key)
                    }
                }
            })
            .await
    }

    /// The tenant's KEK of the highest version.
    async fn current_kek<S>(&self, session: &S, tenant_id: &str) -> Result<Kek, Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "SELECT key_version, encrypted_key, master_algorithm, key_algorithm FROM {} \
             WHERE tenant_id = $1 ORDER BY key_version DESC LIMIT 1",
            self.table
        );
        let row = session.connection().query_opt(&sql, &[&tenant_id]).await?;
        match row {
            Some(row) => self.load_kek(tenant_id, &row),
            None => Err(Error::KekNotFound {
                tenant_id: tenant_id.to_owned(),
                key_version: None,
            }),
        }
    }

    /// The tenant's KEK of `version`.
    async fn kek<S>(&self, session: &S, tenant_id: &str, version: u32) -> Result<Kek, Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "SELECT key_version, encrypted_key, master_algorithm, key_algorithm FROM {} \
             WHERE tenant_id = $1 AND key_version = $2",
            self.table
        );
        let row = session
            .connection()
            .query_opt(&sql, &[&tenant_id, &stored_version(version)?])
            .await?;
        match row {
            Some(row) => self.load_kek(tenant_id, &row),
            None => Err(Error::KekNotFound {
                tenant_id: tenant_id.to_owned(),
                key_version: Some(version),
            }),
        }
    }

    /// The tenant's current KEK, made if there is none — unless the keys
    /// are managed elsewhere, when none is [`Error::KekNotFound`]: under
    /// the tenant's lock, and read again after taking it, in case another
    /// transaction made it meanwhile.
    async fn get_or_create_current_kek<S>(&self, session: &S, tenant_id: &str) -> Result<Kek, Error>
    where
        S: Session + PgAccess + Sync,
    {
        match self.current_kek(session, tenant_id).await {
            Err(Error::KekNotFound { .. }) if !self.managed_keys => {}
            found => return found,
        }
        // A scope of the adapter's own: the lock is a transaction's, and
        // outside one it would end with its own statement, before the read.
        session
            .atomic(async |tx| {
                self.lock_tenant(&tx, tenant_id).await?;
                match self.current_kek(&tx, tenant_id).await {
                    Err(Error::KekNotFound { .. }) => {
                        let kek = self
                            .master_key(self.master_algorithm)?
                            .generate_kek(tenant_id, self.context.as_deref())?;
                        self.save_kek(&tx, &kek).await?;
                        Ok(kek)
                    }
                    found => found,
                }
            })
            .await
    }

    async fn save_kek<S>(&self, session: &S, kek: &Kek) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "INSERT INTO {} (tenant_id, key_version, encrypted_key, master_algorithm, key_algorithm) \
             VALUES ($1, $2, $3, $4, $5)",
            self.table
        );
        session
            .connection()
            .execute(
                &sql,
                &[
                    &kek.tenant_id(),
                    &stored_version(kek.version())?,
                    &kek.wrapped().to_bytes(),
                    &self.master_algorithm.as_str(),
                    &kek.algorithm().as_str(),
                ],
            )
            .await?;
        Ok(())
    }

    /// A KEK from its row, unwrapped by the master key under the algorithm
    /// the row names.
    fn load_kek(&self, tenant_id: &str, row: &Row) -> Result<Kek, Error> {
        let version = read_version(row.try_get("key_version")?)?;
        let wrapped = WrappedKey::parse(&row.try_get::<_, Vec<u8>>("encrypted_key")?)?;
        let master_algorithm: Algorithm = row.try_get::<_, String>("master_algorithm")?.parse()?;
        let key_algorithm: Algorithm = row.try_get::<_, String>("key_algorithm")?.parse()?;
        self.master_key(master_algorithm)?.load_kek(
            tenant_id,
            self.context.as_deref(),
            wrapped,
            version,
            key_algorithm,
        )
    }
}

/// The column is `INTEGER`; a version is a count.
fn stored_version(version: u32) -> Result<i32, Error> {
    i32::try_from(version)
        .map_err(|_| Error::Malformed(format!("key version {version} does not fit the column")))
}

fn read_version(stored: i32) -> Result<u32, Error> {
    u32::try_from(stored)
        .map_err(|_| Error::Malformed(format!("key version {stored} is not a count")))
}

impl<S> KeyManagementService<S> for PgKeyManagementService
where
    S: Session + PgAccess + Sync,
{
    async fn encrypt_dek(&self, session: &S, tenant_id: &str, dek: &Key) -> Result<Vec<u8>, Error> {
        let kek = self.get_or_create_current_kek(session, tenant_id).await?;
        Ok(kek.wrap(dek)?.to_bytes())
    }

    async fn decrypt_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Key, Error> {
        let wrapped = WrappedKey::parse(encrypted_dek)?;
        let kek = self.kek(session, tenant_id, wrapped.key_version()).await?;
        kek.unwrap(&wrapped)
    }

    async fn generate_dek(&self, session: &S, tenant_id: &str) -> Result<(Key, Vec<u8>), Error> {
        let kek = self.get_or_create_current_kek(session, tenant_id).await?;
        let (dek, wrapped) = kek.generate_dek()?;
        Ok((dek, wrapped.to_bytes()))
    }

    async fn rotate_kek(&self, session: &S, tenant_id: &str) -> Result<u32, Error> {
        session
            .atomic(async |tx| {
                self.lock_tenant(&tx, tenant_id).await?;
                let master = self.master_key(self.master_algorithm)?;
                let kek = match self.current_kek(&tx, tenant_id).await {
                    Ok(current) => master.rotate_kek(&current)?,
                    Err(Error::KekNotFound { .. }) => {
                        master.generate_kek(tenant_id, self.context.as_deref())?
                    }
                    Err(error) => return Err(error),
                };
                self.save_kek(&tx, &kek).await?;
                Ok(kek.version())
            })
            .await
    }

    async fn rewrap_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let wrapped = WrappedKey::parse(encrypted_dek)?;
        let from = self.kek(session, tenant_id, wrapped.key_version()).await?;
        let current = self.get_or_create_current_kek(session, tenant_id).await?;
        Ok(current.rewrap(&wrapped, &from)?.to_bytes())
    }

    async fn delete_kek(&self, session: &S, tenant_id: &str) -> Result<(), Error> {
        let sql = format!("DELETE FROM {} WHERE tenant_id = $1", self.table);
        session.connection().execute(&sql, &[&tenant_id]).await?;
        Ok(())
    }
}

impl<S> KeyCustody<S> for PgKeyManagementService
where
    S: Session + PgAccess + Sync,
{
    /// Makes the tenant's first KEK under the tenant's lock, or answers the
    /// version of the one there is. `derived` has nothing to enforce here —
    /// this adapter has no policy to pin a context by; the binding to a
    /// context is the wrap's — and a key that exists is taken as made with
    /// the options asked. Automatic rotation is refused: there is no
    /// scheduler in a table.
    async fn create_kek(
        &self,
        session: &S,
        tenant_id: &str,
        options: &KekOptions,
    ) -> Result<u32, Error> {
        if options.auto_rotate_period.is_some() {
            return Err(Error::Unsupported(
                "automatic rotation: the PostgreSQL KMS rotates on request only",
            ));
        }
        session
            .atomic(async |tx| {
                self.lock_tenant(&tx, tenant_id).await?;
                match self.current_kek(&tx, tenant_id).await {
                    Ok(current) => Ok(current.version()),
                    Err(Error::KekNotFound { .. }) => {
                        let kek = self
                            .master_key(self.master_algorithm)?
                            .generate_kek(tenant_id, self.context.as_deref())?;
                        self.save_kek(&tx, &kek).await?;
                        Ok(kek.version())
                    }
                    Err(error) => Err(error),
                }
            })
            .await
    }

    /// Deletes the rows of the versions below `version`: what they wrapped
    /// is [`Error::KekNotFound`] of that version from then on.
    async fn retire_kek_versions_before(
        &self,
        session: &S,
        tenant_id: &str,
        version: u32,
    ) -> Result<(), Error> {
        let sql = format!(
            "DELETE FROM {} WHERE tenant_id = $1 AND key_version < $2",
            self.table
        );
        session
            .connection()
            .execute(&sql, &[&tenant_id, &stored_version(version)?])
            .await?;
        Ok(())
    }
}

impl<S> SigningService<S> for PgKeyManagementService
where
    S: Session + PgAccess + Sync,
{
    async fn sign(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        message: &[u8],
    ) -> Result<Signature, Error> {
        self.get_or_create_signing_key(session, tenant_id, algorithm)
            .await?
            .sign(message)
    }

    async fn sign_with_version(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        key_version: NonZeroU32,
        message: &[u8],
    ) -> Result<Signature, Error> {
        self.signing_key_of_version(session, tenant_id, algorithm, key_version.get())
            .await?
            .sign(message)
    }

    async fn verify(
        &self,
        session: &S,
        tenant_id: &str,
        message: &[u8],
        signature: &Signature,
    ) -> Result<(), Error> {
        self.signing_key_of_version(
            session,
            tenant_id,
            signature.algorithm(),
            signature.key_version(),
        )
        .await?
        .public()
        .verify(message, signature)
    }

    async fn rotate_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        session
            .atomic(async |tx| {
                let name = SigningKey::name_of(tenant_id, algorithm);
                Self::lock(&tx, &self.signing_table, &name).await?;
                let version = self
                    .current_signing_key(&tx, tenant_id, algorithm)
                    .await?
                    .map_or(1, |current| current.version() + 1);
                let key = SigningKey::generate(tenant_id, algorithm, version)?;
                self.save_signing_key(&tx, &key).await?;
                Ok(version)
            })
            .await
    }

    async fn delete_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<(), Error> {
        let sql = format!(
            "DELETE FROM {} WHERE tenant_id = $1 AND algorithm = $2",
            self.signing_table
        );
        session
            .connection()
            .execute(&sql, &[&tenant_id, &algorithm.as_str()])
            .await?;
        Ok(())
    }
}

/// The public halves of the keyring's versions there are — those an
/// operator has not deleted — each derived from its wrapped private half
/// and checked against the stored one, so that what is published is what
/// the master key's holder made (ADR-0019).
impl<S> PublicKeyService<S> for PgKeyManagementService
where
    S: Session + PgAccess + Sync,
{
    async fn public_keys(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<PublicKeyring, Error> {
        let sql = format!(
            "SELECT key_version, encrypted_key, public_key, master_algorithm FROM {} \
             WHERE tenant_id = $1 AND algorithm = $2 ORDER BY key_version",
            self.signing_table
        );
        let rows = session
            .connection()
            .query(&sql, &[&tenant_id, &algorithm.as_str()])
            .await?;
        if rows.is_empty() {
            return Err(Error::SigningKeyNotFound {
                tenant_id: tenant_id.to_owned(),
                algorithm,
            });
        }
        let keys = rows
            .iter()
            .map(|row| {
                let key = self.checked_signing_key(tenant_id, algorithm, row)?;
                Ok((key.version(), key.public().clone()))
            })
            .collect::<Result<Vec<_>, Error>>()?;
        PublicKeyring::new(algorithm, keys)
    }
}

impl<S> MacService<S> for PgKeyManagementService
where
    S: Session + PgAccess + Sync,
{
    async fn mac(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
        message: &[u8],
    ) -> Result<Mac, Error> {
        self.get_or_create_mac_key(session, tenant_id, purpose)
            .await?
            .mac(message)
    }

    async fn delete_mac_key(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<(), Error> {
        let sql = format!(
            "DELETE FROM {} WHERE tenant_id = $1 AND purpose = $2",
            self.mac_table
        );
        session
            .connection()
            .execute(&sql, &[&tenant_id, &purpose.as_str()])
            .await?;
        Ok(())
    }
}
