//! The plan's rules the validator checks a log by and the scheduler runs it by
//! (ADR-0047 K1e-2), in one copy: a step's barrier key, its intended start,
//! the timeout and safety of its action, the steps it pins until its release
//! is decided. The bound the validator commits and the machine that runs the
//! plan agree on them by construction, as they do on the binding rule through
//! [`crate::window`].

use crate::record::{Action, Header, Hole, Name, Record, Safety, StepId, Subject};
use crate::validate::holes;

/// A barrier's key: the kind, scope, label and occurrence its members share.
/// Every step is a member of its own; a subject's index is not in it.
pub type MemberKey = (String, Vec<u8>, String, u32);

/// The key a step is a member of.
pub fn member_key(step: &StepId) -> MemberKey {
    (
        step.subject.kind.clone(),
        step.subject.scope.clone(),
        step.label.clone(),
        step.n,
    )
}

/// A barrier dependency's key.
pub fn barrier_key(kind: &str, scope: &[u8], label: &str, n: u32) -> MemberKey {
    (kind.to_owned(), scope.to_vec(), label.to_owned(), n)
}

/// A step's intended start (decision 3): its plan, or its dependencies met
/// and its think after them, whichever is later; `ready` is 0 with none.
pub fn intended(at: u64, ready: u64, think: u64) -> u64 {
    at.max(ready.saturating_add(think))
}

/// How long an action may take (decision 7): an operation's, a channel's or a
/// host call's timeout from its release, an observation's from its cause's
/// intended start; 0 for one its table does not name.
pub fn timeout(header: &Header, action: &Action) -> u64 {
    match action {
        Action::Rest(r) => header
            .operations
            .iter()
            .find(|o| o.name == r.op)
            .map_or(0, |o| o.timeout),
        Action::Bus(b) => header
            .channels
            .iter()
            .find(|c| c.name == b.channel)
            .map_or(0, |c| c.timeout),
        Action::Host(call) => header
            .calls
            .iter()
            .find(|c| c.name == call.op)
            .map_or(0, |c| c.timeout),
        Action::Observe(o) => o.timeout,
    }
}

/// What the harness may resend (decision 11): an operation's or a host
/// call's safety; a bus frame, resent byte for byte, idempotent; an
/// observation never.
pub fn safety(header: &Header, action: &Action) -> Safety {
    match action {
        Action::Rest(r) => header
            .operations
            .iter()
            .find(|o| o.name == r.op)
            .map_or(Safety::Never, |o| o.safety),
        Action::Host(call) => header
            .calls
            .iter()
            .find(|c| c.name == call.op)
            .map_or(Safety::Never, |c| c.safety),
        Action::Bus(_) => Safety::Idempotent,
        Action::Observe(_) => Safety::Never,
    }
}

/// The captured values a step sends: whose, which slot, from which step.
pub fn captured(r: &Record) -> Vec<(&Subject, &Name, &StepId)> {
    holes(r)
        .into_iter()
        .filter_map(|h| match h {
            Hole::Captured {
                subject,
                slot,
                from,
            } => Some((subject, slot, from)),
            _ => None,
        })
        .collect()
}

/// The step an observation's deadline runs from, when it is another.
pub fn cause(r: &Record) -> Option<&StepId> {
    match &r.action {
        Action::Observe(o) if o.cause != r.step => Some(&o.cause),
        _ => None,
    }
}

/// The steps a step pins until its release is decided: its captured values'
/// producers and its observation's cause.
pub fn pins(r: &Record) -> Vec<&StepId> {
    let mut pins: Vec<&StepId> = captured(r).into_iter().map(|(_, _, from)| from).collect();
    pins.extend(cause(r));
    pins
}
