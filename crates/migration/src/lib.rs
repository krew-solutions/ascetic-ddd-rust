//! Migrations on PostgreSQL (ADR-0029).
//!
//! A schema put in place by idempotent DDL at every start is not lock-free
//! when it has nothing to do: `CREATE INDEX IF NOT EXISTS` takes a share lock
//! on its table, `ALTER TABLE … ADD COLUMN IF NOT EXISTS` an access-exclusive
//! one, both before they look whether there is anything to do, and both are
//! held to the commit — a replica starting beside live traffic stalls it, or
//! deadlocks a live transaction. So a schema moves here by migrations: each
//! component — a host's service, a module, one outbox or inbox table — owns an
//! ordered set of them, and the database's ledger [`LEDGER`] records which are
//! applied.
//!
//! - A start against a current schema only reads the ledger, outside any
//!   transaction: it locks no application table.
//! - A pending migration is a transaction of its own: under the component's
//!   advisory lock, it takes its one pre-existing table — its target — in
//!   `ACCESS EXCLUSIVE` mode first, so that it waits only while it holds
//!   nothing live traffic asks for; it runs; and before it commits the runner
//!   reads its own locks and refuses it when it locked any other pre-existing
//!   table — the shape that deadlocks a live transaction. A component's first
//!   migration, its baseline, creates its tables and is exempt.
//! - Blocked (`55P03`, `40P01`), it is rolled back and retried after a
//!   jittered backoff, within a budget.
//! - A migration applied under another text is refused: the database is
//!   recreated, never patched by hand.
//!
//! Everything is transaction-scoped (ADR-0009): the runner works through a
//! pool that recycles connections without running SQL, and through PgBouncer
//! in transaction pooling.

use std::fmt;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ascetic_ddd_session::pg::tokio_postgres::error::SqlState;
use ascetic_ddd_session::pg::{Identifier, PgAccess, PgError};
use ascetic_ddd_session::{Session, SessionError, SessionPool};

/// The table of the database's applied migrations.
pub const LEDGER: &str = "ascetic_ddd_migrations";

/// One step of a component's schema.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Migration {
    id: String,
    target: Option<Identifier>,
    sql: String,
    checksum: String,
}

impl Migration {
    /// The step `id`, which runs `sql`, creating only new relations or —
    /// for a component's first — its tables; its checksum over `sql`.
    pub fn new(id: impl Into<String>, sql: impl Into<String>) -> Self {
        let sql = sql.into();
        let checksum = checksum_of(&sql);
        Self {
            id: id.into(),
            target: None,
            sql,
            checksum,
        }
    }

    /// The step changes the pre-existing table `target`: the runner takes
    /// it in `ACCESS EXCLUSIVE` mode before the step's SQL, and the step
    /// may lock no other pre-existing table.
    #[must_use]
    pub fn on(mut self, target: Identifier) -> Self {
        self.target = Some(target);
        self
    }

    /// The checksum over `template`, the text the code owns, instead of
    /// the SQL rendered from it: for a component whose table names vary.
    #[must_use]
    pub fn checked_over(mut self, template: &str) -> Self {
        self.checksum = checksum_of(template);
        self
    }

    /// The step's id within its component.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The checksum the ledger keeps for it.
    pub fn checksum(&self) -> &str {
        &self.checksum
    }
}

/// A component's migrations refused by [`Component::new`], and why.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InvalidComponent {
    /// The component's name.
    pub name: String,
    /// What is wrong.
    pub reason: String,
}

impl fmt::Display for InvalidComponent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "component `{}`: {}", self.name, self.reason)
    }
}

impl std::error::Error for InvalidComponent {}

/// A named, ordered set of migrations owned by one piece of code; its first
/// is its baseline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Component {
    name: String,
    migrations: Vec<Migration>,
}

impl Component {
    /// The component `name` of `migrations`, in the order they apply.
    ///
    /// # Errors
    /// An empty name, no migrations, or two of one id.
    pub fn new(
        name: impl Into<String>,
        migrations: Vec<Migration>,
    ) -> Result<Self, InvalidComponent> {
        let name = name.into();
        let refuse = |reason: String| InvalidComponent {
            name: name.clone(),
            reason,
        };
        if name.is_empty() {
            return Err(refuse("it has no name".to_owned()));
        }
        if migrations.is_empty() {
            return Err(refuse("it has no migrations".to_owned()));
        }
        for (index, migration) in migrations.iter().enumerate() {
            if migration.id.is_empty() {
                return Err(refuse(format!("migration {index} has no id")));
            }
            if migrations[..index].iter().any(|m| m.id == migration.id) {
                return Err(refuse(format!("`{}` is there twice", migration.id)));
            }
        }
        Ok(Self { name, migrations })
    }

    /// Its name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Its migrations, in the order they apply.
    pub fn migrations(&self) -> &[Migration] {
        &self.migrations
    }
}

/// How long the runner waits, and for how long it tries.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Options {
    /// The longest one step waits for a lock before it is retried.
    pub lock_timeout: Duration,
    /// How long one migration is tried before the start gives up.
    pub budget: Duration,
    /// The first pause between two tries, doubled after each.
    pub backoff: Duration,
    /// The longest pause.
    pub backoff_ceiling: Duration,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            lock_timeout: Duration::from_millis(500),
            budget: Duration::from_secs(30),
            backoff: Duration::from_millis(50),
            backoff_ceiling: Duration::from_secs(1),
        }
    }
}

/// What a run did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Applied {
    /// The migrations it applied, in order, each with how long it held its
    /// locks.
    pub applied: Vec<(String, Duration)>,
    /// The ledger's rows of the component the code does not know: a newer
    /// binary's, or another branch's.
    pub unknown: Vec<String>,
}

/// Why a run stopped.
#[derive(Debug)]
pub enum MigrationError {
    /// The migration is in the ledger under another text: the database was
    /// migrated by other code than this, and is recreated, never patched.
    Changed {
        /// The component.
        component: String,
        /// The migration.
        migration: String,
    },
    /// The migration locked pre-existing tables other than its target; it
    /// was rolled back.
    TwoTables {
        /// The component.
        component: String,
        /// The migration.
        migration: String,
        /// The other tables it locked.
        tables: Vec<String>,
    },
    /// The migration could not take its locks within the budget.
    Blocked {
        /// The component.
        component: String,
        /// The migration.
        migration: String,
        /// How long it was tried.
        waited: Duration,
        /// Who held its target, as last seen.
        holders: Vec<String>,
    },
    /// A statement failed.
    Failed {
        /// The component.
        component: String,
        /// The migration, when it was one of its statements.
        migration: Option<String>,
        /// The server's error.
        error: PgError,
    },
    /// The session failed.
    Session(SessionError),
}

impl From<SessionError> for MigrationError {
    fn from(error: SessionError) -> Self {
        Self::Session(error)
    }
}

impl fmt::Display for MigrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Changed {
                component,
                migration,
            } => write!(
                f,
                "migration `{migration}` of `{component}` was applied under another text: \
                 recreate the database"
            ),
            Self::TwoTables {
                component,
                migration,
                tables,
            } => write!(
                f,
                "migration `{migration}` of `{component}` locked pre-existing tables other \
                 than its target ({}): one table per migration",
                tables.join(", ")
            ),
            Self::Blocked {
                component,
                migration,
                waited,
                holders,
            } => write!(
                f,
                "migration `{migration}` of `{component}` could not take its locks in {waited:?}; \
                 held by: {}",
                if holders.is_empty() {
                    "unknown".to_owned()
                } else {
                    holders.join("; ")
                }
            ),
            Self::Failed {
                component,
                migration: Some(migration),
                error,
            } => write!(f, "migration `{migration}` of `{component}`: {error}"),
            Self::Failed {
                component,
                migration: None,
                error,
            } => write!(f, "the ledger of `{component}`: {error}"),
            Self::Session(error) => write!(f, "migrations: {error}"),
        }
    }
}

impl std::error::Error for MigrationError {}

/// Brings the database's schema of `component` up to its migrations: a read
/// of the ledger when it is current, each pending migration applied in a
/// transaction of its own otherwise.
///
/// # Errors
/// [`MigrationError`].
pub async fn migrate<P>(
    pool: &P,
    component: &Component,
    options: &Options,
) -> Result<Applied, MigrationError>
where
    P: SessionPool,
    P::Session: PgAccess,
{
    let recorded = read(pool, component).await?;
    let mut done = Applied::default();
    if let Some(rows) = &recorded {
        for (id, checksum) in rows {
            match component.migrations.iter().find(|m| &m.id == id) {
                Some(known) if &known.checksum != checksum => {
                    return Err(MigrationError::Changed {
                        component: component.name.clone(),
                        migration: id.clone(),
                    });
                }
                Some(_) => {}
                None => done.unknown.push(id.clone()),
            }
        }
    }
    let applied = |id: &str| {
        recorded
            .as_ref()
            .is_some_and(|rows| rows.iter().any(|(row, _)| row == id))
    };
    if component.migrations.iter().all(|m| applied(&m.id)) {
        return Ok(done);
    }
    retried(pool, component, Work::Ledger, options).await?;
    for (index, migration) in component.migrations.iter().enumerate() {
        if applied(&migration.id) {
            continue;
        }
        let held = retried(pool, component, Work::Step(migration, index == 0), options).await?;
        if let Some(held) = held {
            done.applied.push((migration.id.clone(), held));
        }
    }
    Ok(done)
}

/// Forgets the ledger's rows of the component named `name`: for a component
/// whose tables are dropped with it — a test's.
///
/// # Errors
/// [`MigrationError::Failed`].
pub async fn forget<P>(pool: &P, name: &str) -> Result<(), MigrationError>
where
    P: SessionPool,
    P::Session: PgAccess,
{
    pool.session(async |session: P::Session| {
        let connection = session.connection();
        let exists = connection
            .query_one("SELECT to_regclass($1::text) IS NOT NULL", &[&LEDGER])
            .await
            .map_err(|error| ledger_failed(name, error))?;
        if exists.get::<_, bool>(0) {
            connection
                .execute(
                    &format!("DELETE FROM {LEDGER} WHERE component = $1"),
                    &[&name],
                )
                .await
                .map_err(|error| ledger_failed(name, error))?;
        }
        Ok(())
    })
    .await
}

/// The ledger's rows of the component, read outside any transaction; none
/// when there is no ledger yet.
async fn read<P>(
    pool: &P,
    component: &Component,
) -> Result<Option<Vec<(String, String)>>, MigrationError>
where
    P: SessionPool,
    P::Session: PgAccess,
{
    pool.session(async |session: P::Session| {
        let connection = session.connection();
        let exists = connection
            .query_one("SELECT to_regclass($1::text) IS NOT NULL", &[&LEDGER])
            .await
            .map_err(|error| ledger_failed(&component.name, error))?;
        if !exists.get::<_, bool>(0) {
            return Ok(None);
        }
        let rows = connection
            .query(
                &format!("SELECT migration, checksum FROM {LEDGER} WHERE component = $1"),
                &[&component.name],
            )
            .await
            .map_err(|error| ledger_failed(&component.name, error))?;
        Ok(Some(
            rows.iter().map(|row| (row.get(0), row.get(1))).collect(),
        ))
    })
    .await
}

/// Creates the ledger when there is none, under its advisory lock: two
/// runners on a fresh database do not race for it.
async fn create_ledger<S: Session + PgAccess>(
    tx: &S,
    component: &Component,
    options: &Options,
) -> Result<Option<Duration>, MigrationError> {
    let connection = tx.connection();
    let failed = |error| ledger_failed(&component.name, error);
    connection
        .batch_execute(&format!(
            "SET LOCAL lock_timeout = '{}ms'",
            options.lock_timeout.as_millis()
        ))
        .await
        .map_err(failed)?;
    connection
        .execute("SELECT pg_advisory_xact_lock($1)", &[&advisory_key(LEDGER)])
        .await
        .map_err(failed)?;
    connection
        .batch_execute(&format!(
            "CREATE TABLE IF NOT EXISTS {LEDGER} (
                component TEXT NOT NULL,
                migration TEXT NOT NULL,
                checksum TEXT NOT NULL,
                applied_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                PRIMARY KEY (component, migration)
            )"
        ))
        .await
        .map_err(failed)?;
    Ok(None)
}

/// One try at `migration`: how long it held its locks, or none when another
/// runner had applied it.
async fn step<S: Session + PgAccess>(
    tx: &S,
    component: &Component,
    migration: &Migration,
    baseline: bool,
    options: &Options,
) -> Result<Option<Duration>, MigrationError> {
    let connection = tx.connection();
    let failed = |error| MigrationError::Failed {
        component: component.name.clone(),
        migration: Some(migration.id.clone()),
        error,
    };
    connection
        .batch_execute(&format!(
            "SET LOCAL lock_timeout = '{}ms'",
            options.lock_timeout.as_millis()
        ))
        .await
        .map_err(failed)?;
    connection
        .execute(
            "SELECT pg_advisory_xact_lock($1)",
            &[&advisory_key(&format!("{LEDGER}:{}", component.name))],
        )
        .await
        .map_err(failed)?;
    // Another runner may have applied it while this one waited.
    let recorded = connection
        .query_opt(
            &format!("SELECT checksum FROM {LEDGER} WHERE component = $1 AND migration = $2"),
            &[&component.name, &migration.id],
        )
        .await
        .map_err(failed)?;
    if let Some(row) = recorded {
        return if row.get::<_, String>(0) == migration.checksum {
            Ok(None)
        } else {
            Err(MigrationError::Changed {
                component: component.name.clone(),
                migration: migration.id.clone(),
            })
        };
    }
    // The tables as they stand before the step: what it may not lock but
    // its target. Not `pg_class.xmin`, which an `ALTER TABLE` rewrites.
    let before: Vec<u32> = connection
        .query_one(
            "SELECT coalesce(array_agg(c.oid), '{}') FROM pg_class c \
             JOIN pg_namespace n ON n.oid = c.relnamespace \
             WHERE c.relkind IN ('r', 'p') \
               AND n.nspname NOT IN ('pg_catalog', 'information_schema') \
               AND n.nspname NOT LIKE 'pg\\_toast%'",
            &[],
        )
        .await
        .map_err(failed)?
        .get(0);
    let started = Instant::now();
    if let Some(target) = &migration.target {
        connection
            .batch_execute(&format!("LOCK TABLE {target} IN ACCESS EXCLUSIVE MODE"))
            .await
            .map_err(failed)?;
    }
    connection
        .batch_execute(&migration.sql)
        .await
        .map_err(failed)?;
    if !baseline {
        let target = migration.target.as_ref().map(Identifier::as_str);
        let others: Vec<String> = connection
            .query(
                "SELECT DISTINCT t.relname::text FROM pg_locks l \
                 JOIN pg_class r ON r.oid = l.relation \
                 LEFT JOIN pg_index i ON i.indexrelid = r.oid \
                 JOIN pg_class t ON t.oid = coalesce(i.indrelid, r.oid) \
                 WHERE l.pid = pg_backend_pid() AND l.locktype = 'relation' \
                   AND t.oid = ANY($1) AND t.relname <> $2 \
                   AND ($3::text IS NULL OR t.oid <> to_regclass($3::text)) \
                 ORDER BY 1",
                &[&before, &LEDGER, &target],
            )
            .await
            .map_err(failed)?
            .iter()
            .map(|row| row.get(0))
            .collect();
        if !others.is_empty() {
            return Err(MigrationError::TwoTables {
                component: component.name.clone(),
                migration: migration.id.clone(),
                tables: others,
            });
        }
    }
    connection
        .execute(
            &format!("INSERT INTO {LEDGER} (component, migration, checksum) VALUES ($1, $2, $3)"),
            &[&component.name, &migration.id, &migration.checksum],
        )
        .await
        .map_err(failed)?;
    Ok(Some(started.elapsed()))
}

/// What one transaction of a run does: create the ledger, or try a step —
/// the component's first, its baseline, or a later one.
#[derive(Clone, Copy)]
enum Work<'a> {
    Ledger,
    Step(&'a Migration, bool),
}

/// Runs `work` in a transaction of its own until it is not blocked, or the
/// budget is spent: blocked, it is rolled back and tried again after a
/// jittered pause taken outside the session.
async fn retried<P>(
    pool: &P,
    component: &Component,
    work: Work<'_>,
    options: &Options,
) -> Result<Option<Duration>, MigrationError>
where
    P: SessionPool,
    P::Session: PgAccess,
{
    let migration = match work {
        Work::Ledger => None,
        Work::Step(migration, _) => Some(migration),
    };
    let started = Instant::now();
    let mut pause = options.backoff;
    loop {
        let outcome = pool
            .session(async |session: P::Session| {
                session
                    .atomic(async |tx: P::Session| match work {
                        Work::Ledger => create_ledger(&tx, component, options).await,
                        Work::Step(migration, baseline) => {
                            step(&tx, component, migration, baseline, options).await
                        }
                    })
                    .await
            })
            .await;
        match outcome {
            Err(error) if blocked(&error) => {
                let holders = match migration.and_then(|m| m.target.as_ref()) {
                    Some(target) => holders(pool, target).await,
                    None => Vec::new(),
                };
                let waited = started.elapsed();
                let id = migration.map_or(LEDGER, |m| m.id.as_str());
                if waited >= options.budget {
                    return Err(MigrationError::Blocked {
                        component: component.name.clone(),
                        migration: id.to_owned(),
                        waited,
                        holders,
                    });
                }
                log::warn!(
                    "migration `{id}` of `{}` blocked ({error}); held by: {}; trying again",
                    component.name,
                    holders.join("; ")
                );
                tokio::time::sleep(jittered(pause)).await;
                pause = (pause * 2).min(options.backoff_ceiling);
            }
            outcome => return outcome,
        }
    }
}

/// Whether the failure is one of the moment: a lock not taken in time, or
/// a deadlock this transaction was chosen to end.
fn blocked(error: &MigrationError) -> bool {
    let code = match error {
        MigrationError::Failed { error, .. } => error.as_db_error().map(|db| db.code().clone()),
        _ => None,
    };
    code.is_some_and(|code| {
        code == SqlState::LOCK_NOT_AVAILABLE || code == SqlState::T_R_DEADLOCK_DETECTED
    })
}

/// Who holds `target`, as the server lists them: what a blocked start
/// names.
async fn holders<P>(pool: &P, target: &Identifier) -> Vec<String>
where
    P: SessionPool,
    P::Session: PgAccess,
{
    let read = pool
        .session(async |session: P::Session| {
            session
                .connection()
                .query(
                    "SELECT format('pid %s %s %s since %s', a.pid, \
                       coalesce(nullif(a.application_name, ''), '-'), a.state, a.xact_start) \
                     FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid \
                     WHERE l.relation = to_regclass($1::text) AND l.granted \
                       AND l.pid <> pg_backend_pid()",
                    &[&target.as_str()],
                )
                .await
                .map_err(|error| ledger_failed(target.as_str(), error))
        })
        .await;
    read.map(|rows| rows.iter().map(|row| row.get(0)).collect())
        .unwrap_or_default()
}

fn ledger_failed(component: &str, error: PgError) -> MigrationError {
    MigrationError::Failed {
        component: component.to_owned(),
        migration: None,
        error,
    }
}

/// `pause` spread over its second half, so that the runners of a fleet
/// started together do not try again together.
fn jittered(pause: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|now| now.subsec_nanos())
        .unwrap_or(0);
    let half = pause / 2;
    let spread = u64::try_from(half.as_nanos()).unwrap_or(u64::MAX).max(1);
    half + Duration::from_nanos(u64::from(nanos) % spread)
}

/// FNV-1a over `text`, 64 bits.
fn fnv(text: &str) -> u64 {
    text.bytes().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(byte)).wrapping_mul(0x0100_0000_01b3)
    })
}

/// The checksum the ledger keeps for `text`: what tells an edited
/// migration, not a seal.
fn checksum_of(text: &str) -> String {
    format!("fnv1a64:{:016x}:{}", fnv(text), text.len())
}

/// The advisory lock of `name`.
fn advisory_key(name: &str) -> i64 {
    i64::from_ne_bytes(fnv(name).to_ne_bytes())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_component_names_each_migration_once() {
        let twice = Component::new(
            "orders",
            vec![
                Migration::new("a", "SELECT 1"),
                Migration::new("a", "SELECT 2"),
            ],
        );
        assert!(twice.is_err());
        assert!(Component::new("orders", Vec::new()).is_err());
        assert!(Component::new("", vec![Migration::new("a", "SELECT 1")]).is_err());
        assert!(Component::new("orders", vec![Migration::new("a", "SELECT 1")]).is_ok());
    }

    #[test]
    fn the_checksum_follows_the_text_or_its_template() {
        let one = Migration::new("a", "CREATE TABLE t1 (id INT)");
        let edited = Migration::new("a", "CREATE TABLE t1 (id BIGINT)");
        assert_ne!(one.checksum(), edited.checksum());
        let template = "CREATE TABLE {table} (id INT)";
        let first = Migration::new("a", "CREATE TABLE t1 (id INT)").checked_over(template);
        let second = Migration::new("a", "CREATE TABLE t2 (id INT)").checked_over(template);
        assert_eq!(first.checksum(), second.checksum());
    }

    #[test]
    fn a_pause_is_spread_over_its_second_half() {
        for _ in 0..100 {
            let pause = jittered(Duration::from_millis(100));
            assert!(pause >= Duration::from_millis(50) && pause < Duration::from_millis(100));
        }
    }
}
