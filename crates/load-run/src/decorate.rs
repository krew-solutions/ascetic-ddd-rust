//! The host's decorator (ADR-0047 decision 16): what an attempt's request
//! carries that only the host can mint — a DPoP proof, a token — once per
//! attempt, never per step and never per connection tried.

use std::future::Future;

use ascetic_ddd_command_log::record::{Caller, Operation, Record};
use ascetic_ddd_command_log::request::Frame;

use crate::stand::Route;

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
}
