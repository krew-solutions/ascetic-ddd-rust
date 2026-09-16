//! Observers that record what the outbox and the inbox report as lines of
//! JSON, for validation against the protocol models in `verify/tla`.
//!
//! [`JsonTrace`] implements both observer traits, so one recorder can watch
//! an outbox feeding an inbox and keep the order across the two. Each line
//! names the observer that reported it, `"observer": "outbox"` or
//! `"inbox"`, and otherwise carries the event's own fields. [`TraceFile`]
//! writes a recorder's lines to a file when dropped, if
//! `ASCETIC_DDD_TRACE_DIR` is set, so that a test suite attaches one
//! unconditionally and records only when asked.
//!
//! ```no_run
//! use std::sync::Arc;
//! use ascetic_ddd_trace::TraceFile;
//! # fn build(observer: Arc<ascetic_ddd_trace::JsonTrace>) {}
//!
//! let trace = TraceFile::from_env("outbox-roundtrip");
//! build(trace.recorder()); // `.observed_by(trace.recorder())` on the outbox or inbox
//! // ... the run ...
//! drop(trace); // $ASCETIC_DDD_TRACE_DIR/outbox-roundtrip.jsonl, if the variable is set
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use ascetic_ddd_inbox::observer as inbox;
use ascetic_ddd_inbox::{CausalDependency, InboxMessage, InboxObserver, Outcome, Snapshot};
use ascetic_ddd_outbox::observer as outbox;
use ascetic_ddd_outbox::{OutboxMessage, OutboxObserver};
use serde_json::{Value, json};

/// Records every event as a JSON object, in order.
#[derive(Debug, Default)]
pub struct JsonTrace {
    lines: Mutex<Vec<Value>>,
}

impl JsonTrace {
    /// An empty recorder.
    pub fn new() -> Self {
        JsonTrace::default()
    }

    /// What was recorded so far, in order.
    pub fn lines(&self) -> Vec<Value> {
        self.lines.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Writes the lines to `path`, one JSON object per line, creating the
    /// directory if needed.
    pub fn write_to(&self, path: &Path) -> io::Result<()> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let text = self
            .lines()
            .iter()
            .map(|line| format!("{line}\n"))
            .collect::<String>();
        std::fs::write(path, text)
    }

    fn record(&self, line: Value) {
        self.lines
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(line);
    }
}

fn outbox_id(message: &OutboxMessage) -> Value {
    message.metadata["message_id"].clone()
}

/// A row's identity as one string.
fn inbox_id(message: &InboxMessage) -> String {
    format!(
        "{}/{}/{}/{}",
        message.tenant_id, message.stream_type, message.stream_id, message.stream_position
    )
}

fn snapshot_json(snapshot: &Snapshot) -> Value {
    json!({ "xmin": snapshot.xmin, "xmax": snapshot.xmax, "xip": snapshot.in_progress })
}

fn dependency_id(dependency: &CausalDependency) -> String {
    format!(
        "{}/{}/{}/{}",
        dependency.tenant_id,
        dependency.stream_type,
        dependency.stream_id,
        dependency.stream_position
    )
}

impl OutboxObserver for JsonTrace {
    fn on_published(&self, event: &outbox::Published<'_>) {
        self.record(json!({
            "observer": "outbox",
            "event": "published",
            "message_id": outbox_id(event.message),
            "uri": event.message.uri,
            "xid": event.receipt.transaction_id,
            "position": event.receipt.position,
        }));
    }
    fn on_fetched(&self, event: &outbox::Fetched<'_>) {
        self.record(json!({
            "observer": "outbox",
            "event": "fetched",
            "group": event.group,
            "slot": event.slot,
            "slots": event.slots,
            "horizon": event.horizon,
            "limit": event.limit,
            "messages": event.messages.iter().map(outbox_id).collect::<Vec<_>>(),
        }));
    }
    fn on_handled(&self, event: &outbox::Handled<'_>) {
        self.record(json!({
            "observer": "outbox",
            "event": "handled",
            "group": event.group,
            "slot": event.slot,
            "message_id": outbox_id(event.message),
            "ok": event.outcome.is_ok(),
        }));
    }
    fn on_acked(&self, event: &outbox::Acked<'_>) {
        self.record(json!({
            "observer": "outbox",
            "event": "acked",
            "group": event.group,
            "slot": event.slot,
            "xid": event.position.transaction_id,
            "position": event.position.offset,
        }));
    }
    fn on_dispatched(&self, event: &outbox::Dispatched<'_>) {
        self.record(json!({
            "observer": "outbox",
            "event": "dispatched",
            "group": event.group,
            "slot": event.slot,
            "outcome": match event.outcome {
                Ok(true) => "batch",
                Ok(false) => "nothing",
                Err(_) => "rolled_back",
            },
        }));
    }
}

impl InboxObserver for JsonTrace {
    fn on_received(&self, event: &inbox::Received<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "received",
            "id": inbox_id(event.message),
            "message_id": event.message.message_id(),
            "deps": event.message.causal_dependencies().iter().map(dependency_id).collect::<Vec<_>>(),
            "received_position": event.receipt.map(|receipt| receipt.received_position),
            "xid": event.receipt.map(|receipt| receipt.transaction_id),
        }));
    }
    fn on_waiting(&self, event: &inbox::Waiting<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "waiting",
            "worker": event.worker.id,
            "of": event.worker.of,
            "call": event.call,
            "id": inbox_id(event.message),
            "dependency": dependency_id(event.dependency),
            "snapshot": snapshot_json(event.snapshot),
        }));
    }
    fn on_deferred(&self, event: &inbox::Deferred<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "deferred",
            "worker": event.worker.id,
            "of": event.worker.of,
            "call": event.call,
            "id": inbox_id(event.message),
            "snapshot": snapshot_json(event.snapshot),
        }));
    }
    fn on_fetched(&self, event: &inbox::Fetched<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "fetched",
            "worker": event.worker.id,
            "of": event.worker.of,
            "call": event.call,
            "id": event.message.map(inbox_id),
            "snapshot": snapshot_json(event.snapshot),
        }));
    }
    fn on_handled(&self, event: &inbox::Handled<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "handled",
            "worker": event.worker.id,
            "of": event.worker.of,
            "call": event.call,
            "id": inbox_id(event.message),
            "ok": event.outcome.is_ok(),
        }));
    }
    fn on_marked(&self, event: &inbox::Marked<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "marked",
            "worker": event.worker.id,
            "of": event.worker.of,
            "call": event.call,
            "id": inbox_id(event.message),
            "processed_position": event.processed_position,
            "woken": event.woken.iter().map(inbox_id).collect::<Vec<_>>(),
        }));
    }
    fn on_expired(&self, event: &inbox::Expired<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "expired",
            "worker": event.worker.id,
            "of": event.worker.of,
            "call": event.call,
            "ids": event.messages.iter().map(inbox_id).collect::<Vec<_>>(),
        }));
    }
    fn on_failed(&self, event: &inbox::Failed<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "failed",
            "worker": event.worker.id,
            "of": event.worker.of,
            "call": event.call,
            "id": inbox_id(event.message),
            "attempts": event.attempts,
            "parked": event.parked,
            "retry_after_ms": event.retry_after.as_millis() as u64,
            "error": event.error.to_string(),
        }));
    }
    fn on_dispatched(&self, event: &inbox::Dispatched<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "dispatched",
            "worker": event.worker.id,
            "of": event.worker.of,
            "call": event.call,
            "outcome": match event.outcome {
                Ok(Outcome::Processed) => "processed",
                Ok(Outcome::Failed { .. }) => "failed",
                Ok(Outcome::Nothing) => "nothing",
                Err(_) => "rolled_back",
            },
        }));
    }
    fn on_unparked(&self, event: &inbox::Unparked<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "unparked",
            "id": inbox_id(event.message),
        }));
    }
    fn on_resolved(&self, event: &inbox::Resolved<'_>) {
        self.record(json!({
            "observer": "inbox",
            "event": "resolved",
            "id": inbox_id(event.message),
            "processed_position": event.processed_position,
            "woken": event.woken.iter().map(inbox_id).collect::<Vec<_>>(),
        }));
    }
}

/// A recorder whose lines go to `$ASCETIC_DDD_TRACE_DIR/<stem>.jsonl` when
/// it is dropped; with the variable unset, nothing is written. A run that
/// panics leaves what it had.
#[derive(Debug)]
pub struct TraceFile {
    recorder: Arc<JsonTrace>,
    path: Option<PathBuf>,
}

impl TraceFile {
    /// A recorder for the run named `stem`; the file name's prefix, before
    /// the first dash, tells `trace2tla.py` which model to check against:
    /// `outbox-`, `inbox-` or `bridge-`.
    pub fn from_env(stem: &str) -> Self {
        TraceFile {
            recorder: Arc::new(JsonTrace::new()),
            path: std::env::var_os("ASCETIC_DDD_TRACE_DIR")
                .map(|dir| PathBuf::from(dir).join(format!("{stem}.jsonl"))),
        }
    }

    /// The recorder, to attach with `observed_by`.
    pub fn recorder(&self) -> Arc<JsonTrace> {
        Arc::clone(&self.recorder)
    }
}

impl Drop for TraceFile {
    fn drop(&mut self) {
        if let Some(path) = &self.path {
            self.recorder
                .write_to(path)
                .expect("the trace file can be written");
        }
    }
}
