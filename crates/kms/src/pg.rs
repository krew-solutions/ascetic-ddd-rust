//! The PostgreSQL key management service: a tenant's KEKs in a table, each
//! wrapped by the master key.
//!
//! # The table
//!
//! `kms_keys (tenant_id, key_version, encrypted_key, master_algorithm,
//! key_algorithm, created_at)`, primary key `(tenant_id, key_version)` — the
//! table the Python and Go ports write, so a key made by one port is read by
//! another; a test unwraps what the Python port wrapped. `encrypted_key` is a
//! [`Ciphertext`] under master key version 1; `master_algorithm` is the
//! master key's algorithm when the row was written, and is what the row is
//! read back with.
//!
//! # Locks
//!
//! Making a tenant's first key, and rotating, take
//! `pg_advisory_xact_lock(hashtext(table), hashtext(tenant_id))`, held to the
//! end of the caller's transaction. Two transactions meeting a new tenant at
//! once thus make one key, the second reading what the first committed,
//! instead of both making version 1 and one failing on the primary key; two
//! rotations at once make versions two and three, not one and an error.
//! Reads take nothing. READ COMMITTED is assumed: the read after the lock is
//! in a snapshot of its own, so it sees what the lock's previous holder
//! committed.

use ascetic_ddd_session::Session;
use ascetic_ddd_session::pg::{Identifier, PgAccess};
use tokio_postgres::Row;

use crate::cipher::{Algorithm, Key};
use crate::error::Error;
use crate::key::{Ciphertext, Kek, MasterKey};
use crate::port::KeyManagementService;

/// The algorithm of every KEK this service makes.
const KEY_ALGORITHM: Algorithm = Algorithm::Aes256Gcm;

/// The key management service over a table of KEKs wrapped by one master key.
pub struct PgKeyManagementService {
    master_key: Key,
    master_algorithm: Algorithm,
    table: Identifier,
}

impl PgKeyManagementService {
    /// A service wrapping KEKs with `master_key` under AES-256-GCM, in table
    /// `kms_keys`.
    pub fn new(master_key: Key) -> Self {
        PgKeyManagementService {
            master_key,
            master_algorithm: Algorithm::Aes256Gcm,
            table: Identifier::new("kms_keys").expect("a literal identifier"),
        }
    }

    /// The same service wrapping with `algorithm`. Rows already written keep
    /// the algorithm they name.
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

    /// Creates the table if it is not there. One transaction under an
    /// advisory lock on the table's name, so that two processes setting up
    /// at once do not race in `CREATE TABLE IF NOT EXISTS`.
    pub async fn setup<S>(&self, session: &S) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        let table = &self.table;
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
            "#
        );
        session
            .atomic(async |tx| {
                tx.connection()
                    .execute(
                        "SELECT pg_advisory_xact_lock(hashtext($1))",
                        &[&table.as_str()],
                    )
                    .await?;
                tx.connection().batch_execute(&ddl).await?;
                Ok::<_, Error>(())
            })
            .await
    }

    /// The master key as `tenant_id` sees it, for `algorithm`.
    fn master_key(&self, tenant_id: &str, algorithm: Algorithm) -> Result<MasterKey, Error> {
        MasterKey::new(tenant_id, &self.master_key, algorithm)
    }

    /// Serializes the making and rotating of one tenant's keys until the end
    /// of the transaction.
    async fn lock_tenant<S>(&self, session: &S, tenant_id: &str) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        session
            .connection()
            .execute(
                "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
                &[&self.table.as_str(), &tenant_id],
            )
            .await?;
        Ok(())
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
        self.lock_tenant(session, tenant_id).await?;
        match self.current_kek(session, tenant_id).await {
            Err(Error::KekNotFound { .. }) => {
                let kek = self
                    .master_key(tenant_id, self.master_algorithm)?
                    .generate_kek(KEY_ALGORITHM)?;
                self.save_kek(session, &kek).await?;
                Ok(kek)
            }
            found => found,
        }
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
                    &kek.encrypted_key().to_bytes(),
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
        let encrypted_key = Ciphertext::parse(&row.try_get::<_, Vec<u8>>("encrypted_key")?)?;
        let master_algorithm: Algorithm = row.try_get::<_, String>("master_algorithm")?.parse()?;
        let key_algorithm: Algorithm = row.try_get::<_, String>("key_algorithm")?.parse()?;
        self.master_key(tenant_id, master_algorithm)?.load_kek(
            encrypted_key,
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
        Ok(kek.encrypt(dek.as_bytes())?.to_bytes())
    }

    async fn decrypt_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Key, Error> {
        let ciphertext = Ciphertext::parse(encrypted_dek)?;
        let kek = self
            .kek(session, tenant_id, ciphertext.key_version())
            .await?;
        Ok(Key::new(kek.decrypt(&ciphertext)?))
    }

    async fn generate_dek(&self, session: &S, tenant_id: &str) -> Result<(Key, Vec<u8>), Error> {
        let kek = self.get_or_create_current_kek(session, tenant_id).await?;
        let dek = kek.algorithm().generate_key()?;
        let encrypted_dek = kek.encrypt(dek.as_bytes())?.to_bytes();
        Ok((dek, encrypted_dek))
    }

    async fn rotate_kek(&self, session: &S, tenant_id: &str) -> Result<u32, Error> {
        self.lock_tenant(session, tenant_id).await?;
        let master = self.master_key(tenant_id, self.master_algorithm)?;
        let kek = match self.current_kek(session, tenant_id).await {
            Ok(current) => master.rotate_kek(&current)?,
            Err(Error::KekNotFound { .. }) => master.generate_kek(KEY_ALGORITHM)?,
            Err(error) => return Err(error),
        };
        self.save_kek(session, &kek).await?;
        Ok(kek.version())
    }

    async fn rewrap_dek(
        &self,
        session: &S,
        tenant_id: &str,
        encrypted_dek: &[u8],
    ) -> Result<Vec<u8>, Error> {
        let dek = self.decrypt_dek(session, tenant_id, encrypted_dek).await?;
        self.encrypt_dek(session, tenant_id, &dek).await
    }

    async fn delete_kek(&self, session: &S, tenant_id: &str) -> Result<(), Error> {
        let sql = format!("DELETE FROM {} WHERE tenant_id = $1", self.table);
        session.connection().execute(&sql, &[&tenant_id]).await?;
        Ok(())
    }
}
