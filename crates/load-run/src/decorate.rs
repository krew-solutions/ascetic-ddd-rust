//! The host's decorator (ADR-0047 decision 16): what an attempt's request
//! carries that only the host can mint — a DPoP proof, a token — once per
//! attempt, never per step and never per connection tried.

use std::future::Future;

use ascetic_ddd_command_log::record::{Caller, Operation, Record};
use ascetic_ddd_command_log::request::Frame;

use crate::carrier::Unstated;
use crate::stand::Route;

/// The most one attempt's decorations hold, as its decorator states them
/// (ADR-0047 decision 22, K2b-7e-2b-3b): how many headers, and their names'
/// and values' bytes — held where they enter, a decoration past them a
/// fault.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Decorations {
    /// Headers.
    pub headers: u64,
    /// Their names' and values' bytes.
    pub bytes: u64,
}

/// What a decorator sees of one attempt.
#[derive(Debug)]
pub struct Decoration<'a> {
    /// Its step.
    pub record: &'a Record,
    /// Which attempt, from 1.
    pub attempt: u32,
    /// Who calls, when not the stand's default.
    pub caller: Option<&'a Caller>,
    /// Its operation.
    pub operation: &'a Operation,
    /// Where it goes.
    pub route: &'a Route,
    /// Its frame, final but for the decorated headers.
    pub frame: &'a Frame,
    /// Its correlation id.
    pub correlation: &'a str,
    /// When the frontier censors it, microseconds since T0.
    pub deadline: u64,
    /// Now, on the wall clock: microseconds since the Unix epoch.
    pub wall: i64,
}

/// The host's decorator: headers by name, each one of the operation's
/// `decorated`; anything else, or a value not a header's, a harness fault.
pub trait Decorator: Send + Sync + 'static {
    /// Whether it decorates at all: an operation that declares a decorated
    /// header needs one that does.
    fn decorates(&self) -> bool {
        true
    }

    /// The headers of one attempt, or why there are none.
    fn decorate<'a>(
        &'a self,
        attempt: Decoration<'a>,
    ) -> impl Future<Output = Result<Vec<(String, String)>, String>> + Send + 'a;

    /// The most it decorates an attempt of a record of `shape` with
    /// (decision 22, K2b-7e-2b-3b) — a function of the record, a proof's
    /// size growing with its target; one it cannot state, unstated.
    fn bound(&self, shape: &ascetic_ddd_command_log::shape::Shape)
    -> Result<Decorations, Unstated>;
}

/// No decorator: an operation that declares a decorated header is refused
/// before T0.
#[derive(Clone, Copy, Debug, Default)]
pub struct Undecorated;

impl Decorator for Undecorated {
    fn decorates(&self) -> bool {
        false
    }

    fn decorate<'a>(
        &'a self,
        _: Decoration<'a>,
    ) -> impl Future<Output = Result<Vec<(String, String)>, String>> + Send + 'a {
        std::future::ready(Ok(Vec::new()))
    }

    fn bound(&self, _: &ascetic_ddd_command_log::shape::Shape) -> Result<Decorations, Unstated> {
        Ok(Decorations::default())
    }
}
