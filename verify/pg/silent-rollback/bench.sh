#!/usr/bin/env bash
# The cost of ADR-0028's probe: a transaction closed with COMMIT against one
# closed with `SELECT 1; COMMIT` in the same message, empty and writing, one
# client, pgbench's own latency. The table is prefixed `discriminator_` and
# dropped.
set -u
URL=${ASCETIC_DDD_TEST_PG_URL:-postgresql://devel:devel@localhost:5432/devel_karmabot_test}
SECONDS_EACH=${SECONDS_EACH:-10}
D=$(mktemp -d)
psql "$URL" -X -q -c 'DROP TABLE IF EXISTS discriminator_bench' \
  -c 'CREATE TABLE discriminator_bench (id bigserial PRIMARY KEY, v int)'

printf 'BEGIN;\nCOMMIT;\n' > "$D/empty-commit.sql"
printf 'BEGIN;\nSELECT 1 \\; COMMIT;\n' > "$D/empty-probe.sql"
printf 'BEGIN;\nINSERT INTO discriminator_bench (v) VALUES (1);\nCOMMIT;\n' > "$D/write-commit.sql"
printf 'BEGIN;\nINSERT INTO discriminator_bench (v) VALUES (1);\nSELECT 1 \\; COMMIT;\n' > "$D/write-probe.sql"

for round in 1 2; do
  for script in empty-commit empty-probe write-commit write-probe; do
    latency=$(pgbench "$URL" -n -c 1 -T "$SECONDS_EACH" -f "$D/$script.sql" 2>/dev/null \
      | sed -n 's/^latency average = //p')
    echo "round $round  $script  latency $latency"
  done
done
psql "$URL" -X -q -c 'DROP TABLE IF EXISTS discriminator_bench'
