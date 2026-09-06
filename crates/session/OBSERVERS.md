# Observing a session

An observer watches a session; it does not take part in it. That is one
sentence, but it decides two things about the API, and both are visible in the
signature:

```rust
pub trait SessionObserver: Send + Sync {
    fn on_scope_started(&self, _event: &ScopeStarted) {}
    fn on_scope_ended(&self, _event: &ScopeEnded) {}
}
```

**No `async`.** An observer runs on the completion path of a scope. If it could
await, a slow metrics backend would add its latency to every transaction, and an
unreachable one would hold connections open.

**No `Result`.** In the Go port a failing `Notify` aborts the surrounding
transaction, which turns a logging outage into failed business operations.
Here an observer has nothing to report and nothing to abort.

Transports extend the trait with what only they can produce —
`pg::PgObserver` with the statements a PostgreSQL session executes,
`rest::RestObserver` with the requests a REST session makes — so one session is
still watched by one observer.

## The easy case: no IO

Logging, tracing spans, counters in memory: implement and go.

```rust
struct Log;

impl SessionObserver for Log {
    fn on_scope_ended(&self, event: &ScopeEnded) {
        tracing::debug!(?event.kind, event.depth, ?event.outcome, "scope ended");
    }
}

impl PgObserver for Log {
    fn on_query_ended(&self, event: &QueryEnded<'_>) {
        tracing::debug!(elapsed = ?event.elapsed, event.statement, "query");
    }
}

let sessions = PgSessionPool::new(pool).observed_by(Log);
```

Several observers compose into one value — that is the composite signal of the
other ports, expressed without a registry:

```rust
let sessions = PgSessionPool::new(pool).observed_by((Log, Metrics::new(…)));
```

## The real case: the observer has to do IO

Pushing to a Prometheus pushgateway, shipping spans to a collector, writing to a
remote log — all of it is `await`, and none of it belongs inside a scope. Put a
channel between them: the observer enqueues, a task of its own does the IO.

The code below is `tests/observer_channel.rs`, which runs on every `cargo test`.

### The observer enqueues

```rust
struct Metrics {
    samples: mpsc::Sender<Sample>,
    dropped: Arc<AtomicUsize>,
}

impl SessionObserver for Metrics {
    fn on_scope_ended(&self, event: &ScopeEnded) {
        let sample = Sample {
            depth: event.depth,
            kind: event.kind,
            outcome: event.outcome,
        };

        if self.samples.try_send(sample).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}
```

Three details carry the weight:

* **`try_send`, never `send`.** `send` waits when the queue is full, and waiting
  is the one thing an observer may not do. `try_send` returns immediately.
* **A bounded channel.** An unbounded one never blocks either, but a consumer
  that falls behind then grows the queue until the process dies. A bound turns
  a stalled backend into lost samples, which is the cheaper failure.
* **Count the drops.** Silently discarded telemetry is worse than none: the
  graph looks fine while it lies. A counter makes the loss visible in the same
  system that lost it.

The event is borrowed and lives only for the call, so the observer copies what
it needs into an owned `Sample`. That is also the moment to decide what the
backend actually needs — usually far less than the event carries.

### The consumer awaits

```rust
let consumer = tokio::spawn(async move {
    while let Some(sample) = inbox.recv().await {
        push_to_gateway(&sample).await;   // здесь можно и ждать, и падать
    }
});
```

Failures live here, where they cost nothing: retry, back off, give up. The
session never learns.

### Shutting down

The sender lives inside the observer, the observer inside the pool. Dropping the
pool closes the channel, `recv()` returns `None`, and the consumer finishes the
queue and exits — so a graceful shutdown is `drop(pool); consumer.await;` and
nothing is lost that had already been enqueued.

## What this costs

The pattern is not free, and the cost is worth stating plainly: an observer that
needs IO is no longer a single `impl` — it is a channel, a task and a shutdown
order. In exchange, a metrics outage cannot slow down or fail a transaction, and
that trade is the reason the trait is shaped this way.

If the IO is cheap, synchronous and local — a UDP statsd packet, a line to a
file — none of this is needed: send it straight from the observer.
