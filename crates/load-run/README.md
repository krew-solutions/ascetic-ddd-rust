# ascetic-ddd-load-run

The scheduler's shell (ADR-0047 decision 16): a command log run against a
stand. The pure machine (`ascetic-ddd-scheduler`) decides; this crate gives it
time and carries its attempts.

- `clock` — the one clock: a real one from T0, its sleeps a timed wait on a
  thread of its own; a virtual one (feature `testing`) moved only when every
  task is idle.
- `stamps` — the frontier under its lock: every report of an attempt a clock
  read and a push under it; an attempt dropped unended a harness fault.
- `run` — the machine's loop, `sim`'s with time read from the clock.
- `rest` — the REST carrier: an attempt over hyper's own HTTP/1.1 connection,
  its bytes counted (`Sent` at the last), pooled by source and endpoint; its
  looks around the deadline, each a verified turn of its runtime's driver
  (`turn`).
- `decorate` — the host's decorator: what only it can mint, once an attempt.
- `source`, `sink`, `carrier`, `stand` — the ports: the log's lines, the
  journal, what carries an attempt, and the stand's routes and connections.
