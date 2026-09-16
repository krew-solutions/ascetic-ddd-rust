# PostgreSQL experiments

Two-session scripts that establish facts the ADRs rely on and that a unit
test cannot show by itself: what a statement sees under READ COMMITTED
while another transaction is in flight, which locks block which, what
SERIALIZABLE does to concurrent takes. Each script creates tables prefixed
`discriminator_` in the test database and drops them. Run against a
database of your own:

```bash
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db ./verify/pg/lost-wake/exp.sh
ASCETIC_DDD_TEST_PG_URL=postgresql://user:pass@localhost/db ./verify/pg/lost-wake/exp2.sh
```

`lost-wake/` — the experiments behind ADR-0009, numbered as the ADR cites
them: session A runs a script in the background, session B starts after a
delay, and both outputs are printed with `\timing` where the point is who
waited for whom.
