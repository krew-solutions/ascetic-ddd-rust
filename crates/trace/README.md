# ascetic-ddd-trace

Observers that record what an outbox and an inbox report, one line of JSON
per event, in the order the events happened. A recorded run is the input of
`verify/tla/trace2tla.py`, which turns it into a behaviour of the protocol
model that TLC then checks step by step; see `verify/tla/README.md`, "Trace
validation".

`JsonTrace` implements both `OutboxObserver` and `InboxObserver`, so one
recorder can watch a whole bridge — an outbox feeding an inbox — and the
order of the lines is the order across both. Each line names the observer
that reported it. `TraceFile` writes a recorder's lines to
`$ASCETIC_DDD_TRACE_DIR/<stem>.jsonl` when dropped, and records nothing
when the variable is not set, so a test suite attaches one unconditionally.

Fields are the event's own — transaction ids, positions, horizons, outcomes,
the `dispatch` call — and nothing about the model: the mapping onto the
model's steps is the generator's business.
