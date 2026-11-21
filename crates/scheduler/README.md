# ascetic-ddd-scheduler

The scheduler's pure machine (ADR-0047): it takes a validated command log's
lines as they are read, binds each dependency when its line is read — through
`command-log`'s own window, so that it binds as the validator checked — and
releases each step at `max(at, ready + think)`, never before it is read. It
judges nothing itself and knows no clock: every input carries the instant its
source saw it, and every output is a function of the inputs in their order.

A shell (`load-run`) gives it lines, the instants the bytes left and the
answers came, and carries out what it asks: send this attempt, wake me then,
read up to there. `sim` is a deterministic stand for tests: scripted latencies
and judgements, virtual time.
