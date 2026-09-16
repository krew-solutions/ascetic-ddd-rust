#!/usr/bin/env bash
# Two-session experiments against PostgreSQL for the lost-wake discriminator.
set -u
URL=${ASCETIC_DDD_TEST_PG_URL:-postgresql://devel:devel@localhost:5432/devel_karmabot_test}
P() { psql "$URL" -X -q -v ON_ERROR_STOP=0 "$@"; }
reset() {
P <<'SQL'
DROP TABLE IF EXISTS discriminator_inbox, discriminator_slots;
CREATE TABLE discriminator_inbox (id text PRIMARY KEY, processed_position bigint, waiting_for text, deps text[]);
CREATE TABLE discriminator_slots (slot smallint PRIMARY KEY, served_at timestamptz NOT NULL DEFAULT now());
INSERT INTO discriminator_slots VALUES (0), (1);
INSERT INTO discriminator_inbox (id, deps) VALUES ('m', '{d}'), ('d', NULL), ('behind', NULL);
SQL
}
# run A (stdin script) in background, B after $1 seconds, print both labelled
ab() { local delay=$1 a=$2 b=$3
  ( P -f "$D/$a" 2>&1 | sed 's/^/  A| /' ) &
  sleep "$delay"
  ( P -f "$D/$b" 2>&1 | sed 's/^/  B| /' )
  wait
}
D=$(mktemp -d)
w() { cat > "$D/$1"; }

echo "=== EXP 1: the lost wake at the SQL level (READ COMMITTED) ==="
reset
w a1.sql <<'SQL'
BEGIN;
SELECT 'A: check d' AS step, processed_position FROM discriminator_inbox WHERE id='d';
UPDATE discriminator_inbox SET waiting_for='d' WHERE id='m';
SELECT pg_sleep(1.5);
COMMIT;
SQL
w b1.sql <<'SQL'
\timing on
SET lock_timeout = '3s';
WITH marked AS (UPDATE discriminator_inbox SET processed_position=1 WHERE id='d' RETURNING processed_position),
     woken AS (UPDATE discriminator_inbox SET waiting_for=NULL WHERE waiting_for='d' RETURNING id)
SELECT marked.processed_position, woken.id AS woken FROM marked LEFT JOIN woken ON true;
SQL
ab 0.3 a1.sql b1.sql
P -c "SELECT id, processed_position, waiting_for FROM discriminator_inbox ORDER BY id"

echo; echo "=== EXP 2a: advisory lock INSIDE the mark statement (CTE): does it block, and is the snapshot fresh after? ==="
reset
w a2.sql <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(7);
UPDATE discriminator_inbox SET waiting_for='d' WHERE id='m';
SELECT pg_sleep(1.5);
COMMIT;
SQL
w b2.sql <<'SQL'
\timing on
BEGIN;
WITH l AS (SELECT pg_advisory_xact_lock(7) AS k),
     marked AS (UPDATE discriminator_inbox SET processed_position=1 WHERE id='d' AND (SELECT k FROM l) IS NULL RETURNING processed_position),
     woken AS (UPDATE discriminator_inbox SET waiting_for=NULL WHERE waiting_for='d' AND (SELECT k FROM l) IS NULL RETURNING id)
SELECT clock_timestamp()::time AS at, marked.processed_position, woken.id AS woken FROM marked LEFT JOIN woken ON true;
-- a fresh statement in the same transaction:
UPDATE discriminator_inbox SET waiting_for=NULL WHERE waiting_for='d' RETURNING id AS woken_by_fresh_statement;
COMMIT;
SQL
ab 0.3 a2.sql b2.sql

echo; echo "=== EXP 2b: advisory lock as a SEPARATE statement before the mark (the D3 marker side) ==="
reset
w b2b.sql <<'SQL'
\timing on
BEGIN;
SELECT pg_advisory_xact_lock(7);
WITH marked AS (UPDATE discriminator_inbox SET processed_position=1 WHERE id='d' RETURNING processed_position),
     woken AS (UPDATE discriminator_inbox SET waiting_for=NULL WHERE waiting_for='d' RETURNING id)
SELECT marked.processed_position, woken.id AS woken FROM marked LEFT JOIN woken ON true;
COMMIT;
SQL
ab 0.3 a2.sql b2b.sql

echo; echo "=== EXP 3: INSERT ... ON CONFLICT DO NOTHING against (a) a row locked FOR UPDATE, (b) a row with an uncommitted UPDATE ==="
reset
w a3a.sql <<'SQL'
BEGIN; SELECT id FROM discriminator_inbox WHERE id='d' FOR UPDATE; SELECT pg_sleep(1.5); COMMIT;
SQL
w a3b.sql <<'SQL'
BEGIN; UPDATE discriminator_inbox SET processed_position=1 WHERE id='d'; SELECT pg_sleep(1.5); COMMIT;
SQL
w b3.sql <<'SQL'
\timing on
INSERT INTO discriminator_inbox (id) VALUES ('d') ON CONFLICT (id) DO NOTHING;
SQL
echo "-- (a) locked FOR UPDATE:"; ab 0.3 a3a.sql b3.sql
echo "-- (b) uncommitted UPDATE:"; reset; ab 0.3 a3b.sql b3.sql

echo; echo "=== EXP 4: transaction-level advisory lock taken in a subtransaction: released by ROLLBACK TO? kept by RELEASE? ==="
w a4a.sql <<'SQL'
BEGIN; SAVEPOINT s; SELECT pg_advisory_xact_lock(42); ROLLBACK TO SAVEPOINT s; SELECT pg_sleep(1.5); COMMIT;
SQL
w a4b.sql <<'SQL'
BEGIN; SAVEPOINT s; SELECT pg_advisory_xact_lock(42); RELEASE SAVEPOINT s; SELECT pg_sleep(1.5); COMMIT;
SQL
w b4.sql <<'SQL'
SELECT pg_try_advisory_xact_lock(42) AS free_after_subxact;
SQL
echo "-- (a) after ROLLBACK TO SAVEPOINT:"; ab 0.3 a4a.sql b4.sql
echo "-- (b) after RELEASE SAVEPOINT:"; ab 0.3 a4b.sql b4.sql
