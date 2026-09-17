//! The PostgreSQL DEK store: a resource's DEKs in a table, each wrapped by
//! the tenant's KEK through the KMS port.
//!
//! # The table
//!
//! `deks (tenant_id, kind, resource_id, version, encrypted_dek, algorithm,
//! created_at)`, primary key `(tenant_id, kind, resource_id, version)` — the
//! sources' `stream_deks`, with the columns named after [`Resource`].
//! `encrypted_dek` is what the KMS wrapped; `algorithm` is the DEK's own,
//! what its cipher is built with.
//!
//! # Locks
//!
//! Making a resource's first DEK takes
//! `pg_advisory_xact_lock(hashtext(table), hashtext(resource))`, held to the
//! end of the caller's transaction, so two transactions meeting a new
//! resource at once make one key, the second reading what the first
//! committed, instead of both making version 1 and one failing on the
//! primary key. Reads take nothing. READ COMMITTED is assumed, as in the
//! KMS.

use ascetic_ddd_kms::{Algorithm, Key, KeyManagementService};
use ascetic_ddd_session::Session;
use ascetic_ddd_session::pg::{Identifier, PgAccess};
use serde_json::Value;
use tokio_postgres::Row;

use crate::domain::{Keyring, Resource, VersionedCipher};
use crate::error::Error;
use crate::port::DekStore;

/// The DEK store over a table, wrapping through the KMS `K`.
pub struct PgDekStore<K> {
    kms: K,
    table: Identifier,
    algorithm: Algorithm,
}

impl<K> PgDekStore<K> {
    /// A store in table `deks`, making DEKs for AES-256-GCM, wrapping them
    /// through `kms`.
    pub fn new(kms: K) -> Self {
        PgDekStore {
            kms,
            table: Identifier::new("deks").expect("a literal identifier"),
            algorithm: Algorithm::Aes256Gcm,
        }
    }

    /// The same store in another table. The name goes into SQL as text, so
    /// it is an [`Identifier`]: parsed once, safe after.
    pub fn with_table(self, table: Identifier) -> Self {
        PgDekStore { table, ..self }
    }

    /// The same store making DEKs for `algorithm`. Rows already written
    /// keep the algorithm they name.
    pub fn with_algorithm(self, algorithm: Algorithm) -> Self {
        PgDekStore { algorithm, ..self }
    }

    /// The table the DEKs are in.
    pub fn table(&self) -> &Identifier {
        &self.table
    }

    /// The KMS the DEKs are wrapped through.
    pub fn kms(&self) -> &K {
        &self.kms
    }

    /// Creates the table if it is not there, in one transaction under an
    /// advisory lock on the table's name.
    pub async fn setup<S>(&self, session: &S) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        let table = &self.table;
        let ddl = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {table} (
                "tenant_id" VARCHAR(128) NOT NULL,
                "kind" VARCHAR(128) NOT NULL,
                "resource_id" JSONB NOT NULL,
                "version" INTEGER NOT NULL,
                "encrypted_dek" BYTEA NOT NULL,
                "algorithm" VARCHAR(32) NOT NULL,
                "created_at" TIMESTAMPTZ NOT NULL DEFAULT CURRENT_TIMESTAMP,
                CONSTRAINT {table}_pk PRIMARY KEY ("tenant_id", "kind", "resource_id", "version")
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

    /// The resource's cipher over `dek`, as DEK version `version`, bound to
    /// the resource as associated data.
    fn cipher(
        &self,
        dek: &Key,
        resource: &Resource,
        version: u32,
        algorithm: Algorithm,
    ) -> Result<VersionedCipher, Error> {
        let cipher = algorithm.cipher(dek, resource.canonical())?;
        Ok(VersionedCipher::new(version, cipher))
    }

    /// Serializes the making of one resource's first key until the end of
    /// the transaction.
    async fn lock_resource<S>(&self, session: &S, resource: &Resource) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        session
            .connection()
            .execute(
                "SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))",
                &[&self.table.as_str(), &resource.canonical()],
            )
            .await?;
        Ok(())
    }

    /// The resource's row of the highest version, if any.
    async fn latest<S>(&self, session: &S, resource: &Resource) -> Result<Option<Stored>, Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "SELECT version, encrypted_dek, algorithm FROM {} \
             WHERE tenant_id = $1 AND kind = $2 AND resource_id = $3 \
             ORDER BY version DESC LIMIT 1",
            self.table
        );
        let row = session
            .connection()
            .query_opt(
                &sql,
                &[&resource.tenant_id(), &resource.kind(), resource.id()],
            )
            .await?;
        row.as_ref().map(Stored::read).transpose()
    }

    async fn insert<S>(
        &self,
        session: &S,
        resource: &Resource,
        version: u32,
        encrypted_dek: &[u8],
    ) -> Result<(), Error>
    where
        S: Session + PgAccess + Sync,
    {
        let sql = format!(
            "INSERT INTO {} (tenant_id, kind, resource_id, version, encrypted_dek, algorithm) \
             VALUES ($1, $2, $3, $4, $5, $6)",
            self.table
        );
        session
            .connection()
            .execute(
                &sql,
                &[
                    &resource.tenant_id(),
                    &resource.kind(),
                    resource.id(),
                    &stored_version(version)?,
                    &encrypted_dek,
                    &self.algorithm.as_str(),
                ],
            )
            .await?;
        Ok(())
    }
}

/// A row of the table, as read.
struct Stored {
    version: u32,
    encrypted_dek: Vec<u8>,
    algorithm: Algorithm,
}

impl Stored {
    fn read(row: &Row) -> Result<Self, Error> {
        Ok(Stored {
            version: read_version(row.try_get("version")?)?,
            encrypted_dek: row.try_get("encrypted_dek")?,
            algorithm: row.try_get::<_, String>("algorithm")?.parse()?,
        })
    }
}

/// The column is `INTEGER`; a version is a count.
fn stored_version(version: u32) -> Result<i32, Error> {
    i32::try_from(version)
        .map_err(|_| Error::Malformed(format!("DEK version {version} does not fit the column")))
}

fn read_version(stored: i32) -> Result<u32, Error> {
    u32::try_from(stored)
        .map_err(|_| Error::Malformed(format!("DEK version {stored} is not a count")))
}

impl<S, K> DekStore<S> for PgDekStore<K>
where
    S: Session + PgAccess + Sync,
    K: KeyManagementService<S>,
{
    async fn get_or_create(
        &self,
        session: &S,
        resource: &Resource,
    ) -> Result<VersionedCipher, Error> {
        if let Some(stored) = self.latest(session, resource).await? {
            return self.unwrap(session, resource, &stored).await;
        }
        // Another transaction may make the key while this one waits for
        // the lock: read again after taking it.
        self.lock_resource(session, resource).await?;
        if let Some(stored) = self.latest(session, resource).await? {
            return self.unwrap(session, resource, &stored).await;
        }
        let (dek, encrypted_dek) = self.kms.generate_dek(session, resource.tenant_id()).await?;
        self.insert(session, resource, 1, &encrypted_dek).await?;
        self.cipher(&dek, resource, 1, self.algorithm)
    }

    async fn get(
        &self,
        session: &S,
        resource: &Resource,
        version: u32,
    ) -> Result<VersionedCipher, Error> {
        let sql = format!(
            "SELECT version, encrypted_dek, algorithm FROM {} \
             WHERE tenant_id = $1 AND kind = $2 AND resource_id = $3 AND version = $4",
            self.table
        );
        let row = session
            .connection()
            .query_opt(
                &sql,
                &[
                    &resource.tenant_id(),
                    &resource.kind(),
                    resource.id(),
                    &stored_version(version)?,
                ],
            )
            .await?;
        match row {
            Some(row) => self.unwrap(session, resource, &Stored::read(&row)?).await,
            None => Err(Error::DekNotFound {
                resource: resource.clone(),
                version: Some(version),
            }),
        }
    }

    async fn get_all(&self, session: &S, resource: &Resource) -> Result<Keyring, Error> {
        let sql = format!(
            "SELECT version, encrypted_dek, algorithm FROM {} \
             WHERE tenant_id = $1 AND kind = $2 AND resource_id = $3 ORDER BY version",
            self.table
        );
        let rows = session
            .connection()
            .query(
                &sql,
                &[&resource.tenant_id(), &resource.kind(), resource.id()],
            )
            .await?;
        let mut keyring = None;
        for row in &rows {
            let stored = Stored::read(row)?;
            let cipher = self.unwrap(session, resource, &stored).await?;
            let (version, cipher) = cipher.into_parts();
            keyring = Some(match keyring {
                None => Keyring::new(version, cipher),
                Some(keyring) => Keyring::with(keyring, version, cipher),
            });
        }
        keyring.ok_or_else(|| Error::DekNotFound {
            resource: resource.clone(),
            version: None,
        })
    }

    async fn rewrap(&self, session: &S, tenant_id: &str) -> Result<u64, Error> {
        let select = format!(
            "SELECT kind, resource_id, version, encrypted_dek FROM {} WHERE tenant_id = $1",
            self.table
        );
        let update = format!(
            "UPDATE {} SET encrypted_dek = $1 \
             WHERE tenant_id = $2 AND kind = $3 AND resource_id = $4 AND version = $5",
            self.table
        );
        let rows = session.connection().query(&select, &[&tenant_id]).await?;
        let mut count = 0;
        for row in &rows {
            let kind: String = row.try_get("kind")?;
            let resource_id: Value = row.try_get("resource_id")?;
            let version: i32 = row.try_get("version")?;
            let encrypted_dek: Vec<u8> = row.try_get("encrypted_dek")?;
            let rewrapped = self
                .kms
                .rewrap_dek(session, tenant_id, &encrypted_dek)
                .await?;
            session
                .connection()
                .execute(
                    &update,
                    &[&rewrapped, &tenant_id, &kind, &resource_id, &version],
                )
                .await?;
            count += 1;
        }
        Ok(count)
    }

    async fn delete(&self, session: &S, resource: &Resource) -> Result<(), Error> {
        let sql = format!(
            "DELETE FROM {} WHERE tenant_id = $1 AND kind = $2 AND resource_id = $3",
            self.table
        );
        session
            .connection()
            .execute(
                &sql,
                &[&resource.tenant_id(), &resource.kind(), resource.id()],
            )
            .await?;
        Ok(())
    }
}

impl<K> PgDekStore<K> {
    /// The cipher of a stored row: the DEK unwrapped through the KMS, over
    /// the resource.
    async fn unwrap<S>(
        &self,
        session: &S,
        resource: &Resource,
        stored: &Stored,
    ) -> Result<VersionedCipher, Error>
    where
        S: Session + PgAccess + Sync,
        K: KeyManagementService<S>,
    {
        let dek = self
            .kms
            .decrypt_dek(session, resource.tenant_id(), &stored.encrypted_dek)
            .await?;
        self.cipher(&dek, resource, stored.version, stored.algorithm)
    }
}
