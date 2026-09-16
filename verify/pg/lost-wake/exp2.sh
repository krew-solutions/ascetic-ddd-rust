#!/usr/bin/env bash
set -u
D=$(mktemp -d)
URL=${ASCETIC_DDD_TEST_PG_URL:-postgresql://devel:devel@localhost:5432/devel_karmabot_test}
P() { psql "$URL" -X -q -v ON_ERROR_STOP=0 "$@"; }
reset() {
P <<'SQL'
DROP TABLE IF EXISTS discriminator_inbox, discriminator_slots;
CREATE TABLE discriminator_inbox (id text PRIMARY KEY, processed_position bigint, waiting_for text, deps text[]);
CREATE TABLE discriminator_slots (slot smallint PRIMARY KEY, served_at timestamptz NOT NULL DEFAULT now());
INSERT INTO discriminator_slots VALUES (0, now() - interval '1 hour'), (1, now());
INSERT INTO discriminator_inbox (id, deps) VALUES ('m', '{d}'), ('d', NULL), ('behind', NULL);
SQL
}
ab() { local delay=$1 a=$2 b=$3
  ( P -f "$D/$a" 2>&1 | sed 's/^/  A| /' ) &
  sleep "$delay"
  ( P -f "$D/$b" 2>&1 | sed 's/^/  B| /' )
  wait
}
w() { cat > "$D/$1"; }

echo "=== EXP 2a (corrected): lock inside the mark CTE blocks, yet the woken UPDATE of the same statement misses m ==="
reset
w a2.sql <<'SQL'
BEGIN; SELECT pg_advisory_xact_lock(7); UPDATE discriminator_inbox SET waiting_for='d' WHERE id='m'; SELECT pg_sleep(1.5); COMMIT;
SQL
w b2c.sql <<'SQL'
\timing on
BEGIN;
WITH l AS (SELECT pg_advisory_xact_lock(7), 1 AS one),
     marked AS (UPDATE discriminator_inbox SET processed_position=1 WHERE id='d' AND (SELECT one FROM l) = 1 RETURNING processed_position),
     woken AS (UPDATE discriminator_inbox SET waiting_for=NULL WHERE waiting_for='d' AND (SELECT one FROM l) = 1 RETURNING id)
SELECT marked.processed_position, woken.id AS woken FROM marked LEFT JOIN woken ON true;
UPDATE discriminator_inbox SET waiting_for=NULL WHERE waiting_for='d' RETURNING id AS woken_by_fresh_statement;
COMMIT;
SQL
ab 0.3 a2.sql b2c.sql

echo; echo "=== EXP 5a: SERIALIZABLE, two concurrent takes of different slots (both read and write discriminator_slots) ==="
reset
w a5.sql <<'SQL'
BEGIN ISOLATION LEVEL SERIALIZABLE;
WITH taken AS (SELECT s.slot FROM discriminator_slots s ORDER BY s.served_at LIMIT 1 FOR UPDATE OF s SKIP LOCKED),
     touched AS (UPDATE discriminator_slots SET served_at = now() WHERE slot IN (SELECT slot FROM taken))
SELECT 'A took' AS who, slot FROM taken;
SELECT pg_sleep(1);
COMMIT;
SQL
w b5.sql <<'SQL'
BEGIN ISOLATION LEVEL SERIALIZABLE;
WITH taken AS (SELECT s.slot FROM discriminator_slots s ORDER BY s.served_at LIMIT 1 FOR UPDATE OF s SKIP LOCKED),
     touched AS (UPDATE discriminator_slots SET served_at = now() WHERE slot IN (SELECT slot FROM taken))
SELECT 'B took' AS who, slot FROM taken;
SELECT pg_sleep(1.2);
COMMIT;
SQL
ab 0.3 a5.sql b5.sql

echo; echo "=== EXP 5b: SERIALIZABLE, FOR UPDATE SKIP LOCKED on a slot row committed by another take after our snapshot ==="
reset
w a5b.sql <<'SQL'
SELECT pg_sleep(0.5);
UPDATE discriminator_slots SET served_at = now() WHERE slot = 0;
SQL
w b5b.sql <<'SQL'
BEGIN ISOLATION LEVEL SERIALIZABLE;
SELECT count(*) AS snapshot_taken FROM discriminator_slots;
SELECT pg_sleep(1);
WITH taken AS (SELECT s.slot FROM discriminator_slots s ORDER BY s.served_at LIMIT 1 FOR UPDATE OF s SKIP LOCKED)
SELECT 'B took' AS who, slot FROM taken;
COMMIT;
SQL
ab 0.0 a5b.sql b5b.sql

echo; echo "=== EXP 6: two checkers each holding one dependency lock and wanting the other's (the long-held-lock design) ==="
w a6.sql <<'SQL'
BEGIN; SELECT pg_advisory_xact_lock(1); SELECT pg_sleep(0.5); SELECT pg_advisory_xact_lock(2); COMMIT;
SQL
w b6.sql <<'SQL'
BEGIN; SELECT pg_advisory_xact_lock(2); SELECT pg_sleep(0.5); SELECT pg_advisory_xact_lock(1); COMMIT;
SQL
ab 0.1 a6.sql b6.sql

echo; echo "=== EXP 7: FOR SHARE on d while its mark is uncommitted: blocks, then returns the LATEST version (EvalPlanQual) ==="
reset
w a7.sql <<'SQL'
BEGIN; SELECT id FROM discriminator_inbox WHERE id='d' FOR UPDATE; SELECT pg_sleep(0.5); UPDATE discriminator_inbox SET processed_position=9 WHERE id='d'; SELECT pg_sleep(1); COMMIT;
SQL
w b7a.sql <<'SQL'
\timing on
SELECT processed_position IS NOT NULL AS processed FROM discriminator_inbox WHERE id='d' FOR SHARE;
SQL
w b7b.sql <<'SQL'
\timing on
SELECT 1 AS found FROM discriminator_inbox WHERE id='d' AND processed_position IS NOT NULL FOR SHARE;
SQL
w b7c.sql <<'SQL'
\timing on
SELECT processed_position IS NOT NULL AS processed FROM discriminator_inbox WHERE id='d' FOR KEY SHARE;
SQL
echo "-- (a) FOR SHARE, no qual on the changing column:"; ab 0.3 a7.sql b7a.sql
echo "-- (b) FOR SHARE with the qual on the changing column:"; reset; ab 0.3 a7.sql b7b.sql
echo "-- (c) FOR KEY SHARE:"; reset; ab 0.8 a7.sql b7c.sql

echo; echo "=== EXP 8a: lock protocol, checker first, dependency NOT YET ARRIVED (path 2); the insert of d and the marker ==="
reset
P -c "DELETE FROM discriminator_inbox WHERE id='d'"
w a8.sql <<'SQL'
BEGIN;
SELECT pg_advisory_xact_lock(hashtextextended('d', 0));
SELECT 'A re-check' AS step, count(*) AS d_rows, count(processed_position) AS d_processed FROM discriminator_inbox WHERE id='d';
UPDATE discriminator_inbox SET waiting_for='d' WHERE id='m';
SELECT pg_sleep(1.5);
COMMIT;
SQL
w b8.sql <<'SQL'
\timing on
INSERT INTO discriminator_inbox (id) VALUES ('d');
BEGIN;
SELECT id FROM discriminator_inbox WHERE id='d' FOR UPDATE;
SELECT pg_advisory_xact_lock(hashtextextended('d', 0));
WITH marked AS (UPDATE discriminator_inbox SET processed_position=1 WHERE id='d' RETURNING processed_position),
     woken AS (UPDATE discriminator_inbox SET waiting_for=NULL WHERE waiting_for='d' RETURNING id)
SELECT marked.processed_position, woken.id AS woken FROM marked LEFT JOIN woken ON true;
COMMIT;
SQL
ab 0.3 a8.sql b8.sql
P -c "SELECT id, processed_position, waiting_for FROM discriminator_inbox ORDER BY id"

echo; echo "=== EXP 8b: lock protocol, marker first; the checker's re-check after the lock sees the mark ==="
reset
w a8b.sql <<'SQL'
BEGIN;
SELECT id FROM discriminator_inbox WHERE id='d' FOR UPDATE;
SELECT pg_advisory_xact_lock(hashtextextended('d', 0));
WITH marked AS (UPDATE discriminator_inbox SET processed_position=1 WHERE id='d' RETURNING processed_position),
     woken AS (UPDATE discriminator_inbox SET waiting_for=NULL WHERE waiting_for='d' RETURNING id)
SELECT marked.processed_position, woken.id AS woken FROM marked LEFT JOIN woken ON true;
SELECT pg_sleep(1.5);
COMMIT;
SQL
w b8b.sql <<'SQL'
\timing on
BEGIN;
SELECT 'B plain check' AS step, processed_position FROM discriminator_inbox WHERE id='d';
SELECT pg_advisory_xact_lock(hashtextextended('d', 0));
SELECT 'B re-check' AS step, processed_position FROM discriminator_inbox WHERE id='d';
COMMIT;
SQL
ab 0.3 a8b.sql b8b.sql

echo; echo "=== EXP 10: the waiter's own wake with SKIP LOCKED never waits on an expirer holding m ==="
reset
P -c "UPDATE discriminator_inbox SET waiting_for='d' WHERE id='m'; UPDATE discriminator_inbox SET processed_position=1 WHERE id='d'"
w a10.sql <<'SQL'
BEGIN; SELECT id FROM discriminator_inbox WHERE id='m' FOR UPDATE; SELECT pg_sleep(1.5); COMMIT;
SQL
w b10.sql <<'SQL'
\timing on
UPDATE discriminator_inbox SET waiting_for=NULL WHERE ctid IN (SELECT ctid FROM discriminator_inbox WHERE id='m' AND waiting_for='d' FOR UPDATE SKIP LOCKED) RETURNING id;
SQL
ab 0.3 a10.sql b10.sql

echo; echo "=== EXP 11: the marker finding dependents by their committed dependency list blocks on the checker's head lock ==="
reset
w a11.sql <<'SQL'
BEGIN; SELECT id FROM discriminator_inbox WHERE id='m' FOR UPDATE; UPDATE discriminator_inbox SET waiting_for='d' WHERE id='m'; SELECT pg_sleep(1.5); COMMIT;
SQL
w b11.sql <<'SQL'
\timing on
UPDATE discriminator_inbox SET waiting_for=NULL WHERE processed_position IS NULL AND deps @> '{d}' RETURNING id, waiting_for;
SQL
ab 0.3 a11.sql b11.sql

echo; echo "=== EXP 9: cost of one advisory-lock statement vs SELECT 1 (client round trip incl. psql), 20 each ==="
{ echo '\timing on'; echo 'BEGIN;'; for i in $(seq 1 20); do echo "SELECT pg_advisory_xact_lock($i);"; done; for i in $(seq 1 20); do echo "SELECT 1;"; done; echo 'COMMIT;'; } > "$D/t9.sql"
P -f "$D/t9.sql" 2>&1 | grep '^Time' | awk 'NR>=1 && NR<=20 {gsub(",",".",$2); a[NR]=$2} NR>20 && NR<=40 {gsub(",",".",$2); b[NR-20]=$2} END {n=asort(a); m=asort(b); printf "advisory lock: median %.3f ms (min %.3f)\nSELECT 1:      median %.3f ms (min %.3f)\n", a[int(n/2)+1], a[1], b[int(m/2)+1], b[1]}'
P -c "DROP TABLE IF EXISTS discriminator_inbox, discriminator_slots"
