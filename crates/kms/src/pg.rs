//! The PostgreSQL key management service: a tenant's KEKs in a table, each
//! wrapped by the master key.
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

use ascetic_ddd_session::Session;
use ascetic_ddd_session::pg::{Identifier, PgAccess};
use tokio_postgres::Row;

use crate::domain::{
    Algorithm, Kek, Key, KeyLabel, Mac, MasterKey, Signature, SignatureAlgorithm, WrappedKey,
};
use crate::error::Error;
use crate::port::{KeyManagementService, MacService, SigningService};
use crate::software::{self, KeyPair};

/// The key management service over a table of KEKs wrapped by one master key.
pub struct PgKeyManagementService {
    master_key: Key,
    master_algorithm: Algorithm,
    table: Identifier,
    signing_table: Identifier,
    mac_table: Identifier,
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

    /// The name a signing key is wrapped under: the tenant and the
    /// algorithm, as the associated data, so that a key wrapped for one
    /// does not unwrap as another's.
    fn signing_key_name(tenant_id: &str, algorithm: SignatureAlgorithm) -> String {
        format!("{tenant_id}.sign.{algorithm}")
    }

    /// The name a MAC key is wrapped under, likewise.
    fn mac_key_name(tenant_id: &str, purpose: &KeyLabel) -> String {
        format!("{tenant_id}.mac.{purpose}")
    }

    /// The latest signing key of the tenant and algorithm, `None` for none.
    async fn current_signing_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<Option<(u32, KeyPair)>, Error>
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

    /// The signing key of the tenant, algorithm and version: the public
    /// half, which is all verifying needs.
    async fn public_signing_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
    ) -> Result<Vec<u8>, Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "SELECT key_version, public_key FROM {} \
             WHERE tenant_id = $1 AND algorithm = $2 AND key_version <= $3 \
             ORDER BY key_version DESC LIMIT 1",
            self.signing_table
        );
        let row = session
            .connection()
            .query_opt(
                &sql,
                &[&tenant_id, &algorithm.as_str(), &stored_version(version)?],
            )
            .await?;
        match row {
            Some(row) if read_version(row.try_get("key_version")?)? == version => {
                Ok(row.try_get::<_, Vec<u8>>("public_key")?)
            }
            // A key the tenant has: the version named is not one of its.
            Some(_) => Err(Error::NoKeyOfVersion(version)),
            // No version at or below the one named: the tenant may still
            // have the key, at newer versions only.
            None => {
                let newer = format!(
                    "SELECT 1 FROM {} WHERE tenant_id = $1 AND algorithm = $2 LIMIT 1",
                    self.signing_table
                );
                let any = session
                    .connection()
                    .query_opt(&newer, &[&tenant_id, &algorithm.as_str()])
                    .await?;
                match any {
                    Some(_) => Err(Error::NoKeyOfVersion(version)),
                    None => Err(Error::SigningKeyNotFound {
                        tenant_id: tenant_id.to_owned(),
                        algorithm,
                    }),
                }
            }
        }
    }

    /// The latest signing key, made under the keyring's lock if the tenant
    /// has none of the algorithm.
    async fn get_or_create_signing_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<(u32, KeyPair), Error>
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
                let name = Self::signing_key_name(tenant_id, algorithm);
                Self::lock(&tx, &self.signing_table, &name).await?;
                match self.current_signing_key(&tx, tenant_id, algorithm).await? {
                    Some(found) => Ok(found),
                    None => {
                        let pair = software::generate(algorithm)?;
                        self.save_signing_key(&tx, tenant_id, algorithm, 1, &pair)
                            .await?;
                        Ok((1, pair))
                    }
                }
            })
            .await
    }

    async fn save_signing_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        version: u32,
        pair: &KeyPair,
    ) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        let wrapped = self
            .master_key(self.master_algorithm)?
            .wrap(&Self::signing_key_name(tenant_id, algorithm), &pair.private)?;
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
                    &tenant_id,
                    &algorithm.as_str(),
                    &stored_version(version)?,
                    &wrapped.to_bytes(),
                    &pair.public,
                    &self.master_algorithm.as_str(),
                ],
            )
            .await?;
        Ok(())
    }

    fn load_signing_key(
        &self,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
        row: &Row,
    ) -> Result<(u32, KeyPair), Error> {
        let version = read_version(row.try_get("key_version")?)?;
        let wrapped = WrappedKey::parse(&row.try_get::<_, Vec<u8>>("encrypted_key")?)?;
        let master_algorithm: Algorithm = row.try_get::<_, String>("master_algorithm")?.parse()?;
        let private = self
            .master_key(master_algorithm)?
            .unwrap(&Self::signing_key_name(tenant_id, algorithm), &wrapped)?;
        Ok((
            version,
            KeyPair {
                private,
                public: row.try_get::<_, Vec<u8>>("public_key")?,
            },
        ))
    }

    /// The latest MAC key of the tenant and purpose, `None` for none.
    async fn current_mac_key<S>(
        &self,
        session: &S,
        tenant_id: &str,
        purpose: &KeyLabel,
    ) -> Result<Option<(u32, Key)>, Error>
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
                .unwrap(&Self::mac_key_name(tenant_id, purpose), &wrapped)?;
            Ok((version, key))
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
    ) -> Result<(u32, Key), Error>
    where
        S: Session + PgAccess + Sync,
    {
        if let Some(found) = self.current_mac_key(session, tenant_id, purpose).await? {
            return Ok(found);
        }
        session
            .atomic(async |tx| {
                let name = Self::mac_key_name(tenant_id, purpose);
                Self::lock(&tx, &self.mac_table, &name).await?;
                match self.current_mac_key(&tx, tenant_id, purpose).await? {
                    Some(found) => Ok(found),
                    None => {
                        let key = software::generate_mac_key()?;
                        let wrapped = self.master_key(self.master_algorithm)?.wrap(&name, &key)?;
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
                        Ok((1, key))
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

    /// The tenant's current KEK, made if there is none: under the tenant's
    /// lock, and read again after taking it, in case another transaction
    /// made it meanwhile.
    async fn get_or_create_current_kek<S>(&self, session: &S, tenant_id: &str) -> Result<Kek, Error>
    where
        S: Session + PgAccess + Sync,
    {
        match self.current_kek(session, tenant_id).await {
            Err(Error::KekNotFound { .. }) => {}
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
                            .generate_kek(tenant_id)?;
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
        self.master_key(master_algorithm)?
            .load_kek(tenant_id, wrapped, version, key_algorithm)
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
                    Err(Error::KekNotFound { .. }) => master.generate_kek(tenant_id)?,
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
        let (version, pair) = self
            .get_or_create_signing_key(session, tenant_id, algorithm)
            .await?;
        Ok(Signature::new(
            algorithm,
            version,
            software::sign(algorithm, &pair.private, message)?,
        ))
    }

    async fn verify(
        &self,
        session: &S,
        tenant_id: &str,
        message: &[u8],
        signature: &Signature,
    ) -> Result<(), Error> {
        let algorithm = signature.algorithm();
        let public = self
            .public_signing_key(session, tenant_id, algorithm, signature.key_version())
            .await?;
        software::verify(algorithm, &public, message, signature.as_bytes())
    }

    async fn rotate_signing_key(
        &self,
        session: &S,
        tenant_id: &str,
        algorithm: SignatureAlgorithm,
    ) -> Result<u32, Error> {
        session
            .atomic(async |tx| {
                let name = Self::signing_key_name(tenant_id, algorithm);
                Self::lock(&tx, &self.signing_table, &name).await?;
                let version = match self.current_signing_key(&tx, tenant_id, algorithm).await? {
                    Some((current, _)) => current + 1,
                    None => 1,
                };
                let pair = software::generate(algorithm)?;
                self.save_signing_key(&tx, tenant_id, algorithm, version, &pair)
                    .await?;
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
        let (version, key) = self
            .get_or_create_mac_key(session, tenant_id, purpose)
            .await?;
        Ok(Mac::new(version, software::mac(&key, message)?))
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
