# ascetic-ddd-migration

A database's schema moved by migrations (ADR-0029): each component — a host's
service, a module, one table of this library's adapters — owns an ordered set of
migrations, recorded in the database's ledger `ascetic_ddd_migrations` once
applied.

- A start against a current schema only reads the ledger: no lock on any
  application table, so a replica starts beside live traffic.
- A pending migration is a transaction of its own: under the component's
  advisory lock, it takes its one pre-existing table — its target — in
  `ACCESS EXCLUSIVE` mode first, runs, and is refused if it locked any other
  pre-existing table. Blocked, it is retried within a budget.
- A migration applied under another text is refused: the database is
  recreated, never patched.

```ignore
let component = Component::new("orders", vec![
    Migration::new("20261027T120000Z_baseline", include_str!("baseline.sql")),
    Migration::new("20261102T090000Z_order_note", "ALTER TABLE orders ADD COLUMN note TEXT")
        .on(Identifier::new("orders")?),
])?;
migrate(&pool, &component, &Options::default()).await?;
```
