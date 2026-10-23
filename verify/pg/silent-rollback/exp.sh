#!/usr/bin/env bash
# The experiments behind ADR-0028: what PostgreSQL answers when a transaction
# a statement aborted is closed, and what a probe sent with COMMIT tells.
# One session each; the tables are prefixed `discriminator_` and dropped.
set -u
URL=${ASCETIC_DDD_TEST_PG_URL:-postgresql://devel:devel@localhost:5432/devel_karmabot_test}
P() { psql "$URL" -X -v ON_ERROR_STOP=0 "$@"; }

reset() {
P -q <<'SQL'
DROP TABLE IF EXISTS discriminator_held, discriminator_child, discriminator_parent;
CREATE TABLE discriminator_held (id int PRIMARY KEY, holder int NOT NULL, role text NOT NULL);
CREATE UNIQUE INDEX discriminator_held_once ON discriminator_held (holder, role);
CREATE TABLE discriminator_parent (id int PRIMARY KEY);
CREATE TABLE discriminator_child (id int PRIMARY KEY,
  parent int REFERENCES discriminator_parent DEFERRABLE INITIALLY DEFERRED);
SQL
}

echo "=== EXP 1: COMMIT of an aborted transaction answers ROLLBACK, with no error ==="
reset
P <<'SQL'
BEGIN;
INSERT INTO discriminator_held VALUES (1, 7, 'admin');
-- the shape of the incident: ON CONFLICT names the key, a second unique index refuses
INSERT INTO discriminator_held VALUES (2, 7, 'admin') ON CONFLICT (id) DO NOTHING;
COMMIT;
\echo 'durable rows (expected 0: the first insert went with the rollback):'
SELECT count(*) FROM discriminator_held;
SQL

echo "=== EXP 2: SELECT 1; COMMIT in one message — healthy commits, aborted is refused and stays open ==="
reset
P <<'SQL'
BEGIN;
INSERT INTO discriminator_held VALUES (1, 7, 'admin');
SELECT 1 \; COMMIT;
\echo 'healthy: durable rows (expected 1):'
SELECT count(*) FROM discriminator_held;
BEGIN;
INSERT INTO discriminator_held VALUES (2, 8, 'admin');
INSERT INTO discriminator_held VALUES (3, 8, 'admin');
SELECT 1 \; COMMIT;
\echo 'aborted: the probe failed 25P02 above, the COMMIT behind it never ran; the block is still open:'
SELECT 'still aborted' AS expected_25P02;
ROLLBACK;
\echo 'durable rows (expected 1):'
SELECT count(*) FROM discriminator_held;
SQL

echo "=== EXP 3: RELEASE in an aborted block fails 25P02; ROLLBACK TO restores it ==="
reset
P <<'SQL'
BEGIN;
INSERT INTO discriminator_held VALUES (1, 7, 'admin');
SAVEPOINT sp1;
INSERT INTO discriminator_held VALUES (2, 7, 'admin');
RELEASE SAVEPOINT sp1;
ROLLBACK TO SAVEPOINT sp1;
INSERT INTO discriminator_held VALUES (3, 7, 'operator');
COMMIT;
\echo 'durable rows (expected 2: the outer insert and the one after the restore):'
SELECT count(*) FROM discriminator_held;
SQL

echo "=== EXP 4: what does not tell an aborted block — an empty query; BEGIN inside a block is a warning ==="
reset
P <<'SQL'
BEGIN;
INSERT INTO discriminator_held VALUES (1, 7, 'admin');
INSERT INTO discriminator_held VALUES (2, 7, 'admin');
\echo 'an empty query in the aborted block (no error expected):'
;
ROLLBACK;
BEGIN;
\echo 'BEGIN inside an open block (a WARNING, no error):'
BEGIN;
ROLLBACK;
SQL

echo "=== EXP 5: a COMMIT the server refuses ends the block; a ROLLBACK after it is a warning ==="
reset
P <<'SQL'
BEGIN;
INSERT INTO discriminator_child VALUES (1, 42);
SELECT 1 \; COMMIT;
\echo 'after the refused COMMIT (23503 expected above), a ROLLBACK warns that no transaction is in progress:'
ROLLBACK;
\echo 'and the connection takes a new transaction:'
BEGIN;
SELECT 'healthy' AS state;
COMMIT;
SQL

P -q -c 'DROP TABLE IF EXISTS discriminator_held, discriminator_child, discriminator_parent'
