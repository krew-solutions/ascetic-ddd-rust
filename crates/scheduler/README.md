# ascetic-ddd-scheduler

The scheduler's pure machine (ADR-0047): it takes a validated command log's
lines as they are read, binds each dependency when its line is read — through
`command-log`'s own window, so that it binds as the validator checked — and
releases each step at `max(at, ready + think)`, never before it is read. It
knows no clock: every input carries the instant its source saw it, and every
output is a function of the inputs in their order.

`judge` turns what the shell saw of an attempt — an answer the command log's
`answer` read, a reset, no answer by the deadline — into the machine's
judgement, by a decision list with no JSON (ADR-0047 decision 15).

A shell (`load-run`) gives it lines, the instants the bytes left and the
answers came, and carries out what it asks: send this attempt, wake me then,
read up to there. `sim` is a deterministic stand for tests: scripted latencies
and judgements, virtual time.
