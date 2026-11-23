//! An attempt judged (ADR-0047 decision 15): what the shell saw of it — an
//! answer read by the command log's `answer`, a reset or a cut answer after a
//! byte of the request left, or no answer by the deadline — into the
//! machine's [`Judgement`], first match:
//!
//! 1. a reset, a cut answer, no answer by the deadline: Unknown;
//! 2. the code: the string at the profile's pointer of a problem document;
//! 3. an `expect` response whose status equals, whose problem equals the code
//!    strictly — `None` is "no readable code", so a gateway's 404
//!    `route.not_found` never passes for a service's 404 — and whose body
//!    predicates hold: Expected, with what the step captures;
//! 4. a poll: a 2xx where every `until` holds: Expected;
//! 5. the code in the operation's `not_run`: NotTaken, its `Retry-After`
//!    beside it;
//! 6. a status of 500 or more, or below 200: Unknown, the service's or the
//!    gateway's own;
//! 7. an empty `expect` — a sent-only step's, the only kind the validator lets
//!    carry one — but a poll's: Expected;
//! 8. else Unexpected; for a poll, "not yet".
//!
//! A request no byte of which left is no attempt's end and is never judged.
//! The judge sees no clock, attempt or safety, and no JSON: the command log
//! reads the answer.

#![deny(
    clippy::indexing_slicing,
    clippy::string_slice,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreachable
)]

use std::fmt;

use ascetic_ddd_command_log::answer::{PredicateError, Read};
use ascetic_ddd_command_log::fills::{Captured, Fills};
use ascetic_ddd_command_log::record::{Action, Name, Outcome, Probe, Record, Subject};

pub use crate::machine::{CODE, Code};
use crate::machine::{Judgement, Seen};

/// What the shell saw of an attempt whose bytes left.
#[derive(Debug)]
pub enum Transport<'a> {
    /// An answer, whole.
    Answered(Read<'a>),
    /// A reset, or an answer cut short.
    Broken,
    /// No answer by the deadline.
    Deadline,
}

/// What of the operation and the stand the judge reads.
#[derive(Clone, Copy, Debug)]
pub struct Contract<'a> {
    /// The problem codes that certify a refusal as not run.
    pub not_run: &'a [Name],
    /// Where a problem document holds its code: the profile's pointer.
    pub problem: &'a str,
}

/// Which rule judged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Why {
    /// A reset, or an answer cut short.
    Broken,
    /// No answer by the deadline.
    Deadline,
    /// The `expect` response at this place.
    Listed(usize),
    /// A poll's `until`.
    Polled,
    /// A code in `not_run`.
    NotRun,
    /// A status no answer certifies.
    Uncertain,
    /// A sent-only step's answer.
    Sent,
    /// None of the above.
    Unlisted,
}

/// An attempt's judgement and what beside it the journal carries.
#[derive(Clone, PartialEq, Eq)]
pub struct Judged {
    /// For the machine.
    pub judgement: Judgement,
    /// Which rule.
    pub why: Why,
    /// The answer's status, if one came.
    pub status: Option<u16>,
    /// Its problem's code, if it had one.
    pub code: Option<Code>,
    /// The wait its `Retry-After` asks for, in microseconds.
    pub retry_after: Option<u64>,
    /// What an expected answer did not give: whose, which.
    pub missing: Vec<(Subject, Name)>,
}

impl Judged {
    /// What of it the machine takes beside the judgement.
    pub fn seen(&self) -> Seen {
        Seen {
            status: self.status,
            code: self.code.clone(),
            after: self.retry_after,
            censored: false,
            found: None,
        }
    }
}

impl fmt::Debug for Judged {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Judged")
            .field("judgement", &self.judgement)
            .field("why", &self.why)
            .field("status", &self.status)
            .field("code", &self.code)
            .field("retry_after", &self.retry_after)
            .field("missing", &self.missing)
            .finish()
    }
}

fn is_poll(record: &Record) -> bool {
    matches!(&record.action, Action::Observe(o) if matches!(o.probe, Probe::Rest(_)))
}

/// An attempt of `record` judged; an error only for a predicate the plan
/// wrote wrong, which the validator refuses.
pub fn judge(
    record: &Record,
    contract: Contract<'_>,
    fills: &Fills<'_>,
    transport: &Transport<'_>,
) -> Result<Judged, PredicateError> {
    let bare = |judgement, why| Judged {
        judgement,
        why,
        status: None,
        code: None,
        retry_after: None,
        missing: vec![],
    };
    let read = match transport {
        Transport::Broken => return Ok(bare(Judgement::Unknown, Why::Broken)),
        Transport::Deadline => return Ok(bare(Judgement::Unknown, Why::Deadline)),
        Transport::Answered(read) => read,
    };
    let status = read.status();
    let code = read.code(contract.problem);
    let judged = |judgement, why| Judged {
        judgement,
        why,
        status: Some(status),
        code: code.as_deref().map(Code::of),
        retry_after: None,
        missing: vec![],
    };
    let expected = |why| {
        let mut taken = Vec::new();
        let mut missing = Vec::new();
        for c in &record.capture {
            match read.take(c) {
                Some(value) => taken.push(Captured {
                    subject: c.subject.clone(),
                    slot: c.slot.clone(),
                    value,
                }),
                None => missing.push((c.subject.clone(), c.slot.clone())),
            }
        }
        Judged {
            missing,
            ..judged(Judgement::Expected(taken), why)
        }
    };
    for (i, o) in record.expect.iter().enumerate() {
        if let Outcome::Response {
            status: s,
            problem,
            body,
        } = o
            && *s == status
            && *problem == code
            && read.holds(body, fills)?
        {
            return Ok(expected(Why::Listed(i)));
        }
    }
    let poll = is_poll(record);
    if let Action::Observe(o) = &record.action
        && poll
        && (200..300).contains(&status)
        && read.holds(&o.until, fills)?
    {
        return Ok(expected(Why::Polled));
    }
    if code.as_ref().is_some_and(|c| contract.not_run.contains(c)) {
        return Ok(Judged {
            retry_after: read.retry_after(),
            ..judged(Judgement::NotTaken, Why::NotRun)
        });
    }
    if !(200..500).contains(&status) {
        return Ok(judged(Judgement::Unknown, Why::Uncertain));
    }
    if record.expect.is_empty() && !poll {
        return Ok(expected(Why::Sent));
    }
    Ok(judged(Judgement::Unexpected, Why::Unlisted))
}
