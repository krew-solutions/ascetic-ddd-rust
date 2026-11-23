//! Every hole of a step resolved (ADR-0047 decision 15): what the namespace
//! renders, what the run's secret keys, the run's two instants, and the
//! values earlier steps captured.
//!
//! A captured value is one JSON value's text, as an answer held it — a
//! header's or `Location`'s text written as a JSON string. A JSON token takes
//! it as it is (a string re-quoted, a number as written); a plain value — a
//! path parameter, a query value, a header — takes a string's content or a
//! number's text. `answer` takes only values every use can carry, so a frame
//! never fails on a server's value.

use std::fmt;

// The run's namespace, as `Fills` takes it: its users need not name the
// faker for it.
pub use ascetic_ddd_faker::Namespace;

use crate::json::{self, Json};
use crate::record::{Hole, Name, Subject, TimeBase, Value};
use crate::render::{Filled, RunSecret, render, render_time};

/// A value a step's answer gave for a slot of a subject (decision 5): one
/// JSON value's text. Its `Debug` shows whose and which, never the value.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Captured {
    /// Whose.
    pub subject: Subject,
    /// Which.
    pub slot: Name,
    /// The value, as one JSON value's text.
    pub value: Vec<u8>,
}

impl fmt::Debug for Captured {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Captured")
            .field("subject", &self.subject)
            .field("slot", &self.slot)
            .field("value", &format_args!("{} bytes", self.value.len()))
            .finish()
    }
}

/// What a step's holes are filled from.
pub struct Fills<'a> {
    /// The run's namespace: public renderings.
    pub namespace: Namespace,
    /// The run's secret: key material.
    pub secret: &'a RunSecret,
    /// The run's start, microseconds since the Unix epoch on the wall clock.
    pub run_start: i64,
    /// The frame's first dispatch, microseconds since the Unix epoch: a call's
    /// frame is built once and resent byte for byte, so its `Sent` times are
    /// its first attempt's.
    pub sent: i64,
    /// The values its captured holes take, as the machine handed them.
    pub captured: &'a [Captured],
}

impl fmt::Debug for Fills<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Fills")
            .field("run_start", &self.run_start)
            .field("sent", &self.sent)
            .field("captured", &self.captured)
            .finish_non_exhaustive()
    }
}

/// Why a hole could not be filled. It names a slot, never a value.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HoleError {
    /// No value was handed for a captured hole.
    Missing {
        /// Its slot.
        slot: Name,
    },
    /// Bytes where a text is needed: a UUID's bytes or key material in a
    /// plain value.
    NotText,
    /// A captured value that is not a string or a number.
    NotScalar {
        /// Its slot.
        slot: Name,
    },
    /// A captured value that is not one JSON value.
    NotJson {
        /// Its slot.
        slot: Name,
    },
    /// A time outside the calendar.
    Time,
}

impl fmt::Display for HoleError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            HoleError::Missing { slot } => write!(f, "no value for the captured slot {slot}"),
            HoleError::NotText => f.write_str("bytes where a text is needed"),
            HoleError::NotScalar { slot } => {
                write!(f, "the captured slot {slot} is not a string or a number")
            }
            HoleError::NotJson { slot } => write!(f, "the captured slot {slot} is not JSON"),
            HoleError::Time => f.write_str("a time outside the years 0000 to 9999"),
        }
    }
}

impl std::error::Error for HoleError {}

impl Fills<'_> {
    /// A hole's value, as a payload's span takes it.
    pub fn filled(&self, hole: &Hole) -> Result<Filled, HoleError> {
        match hole {
            Hole::Rendered {
                of,
                role,
                rendering,
            } => Ok(render(self.namespace, self.secret, of, role, *rendering)),
            Hole::Time {
                base,
                offset,
                rendering,
            } => {
                let base = match base {
                    TimeBase::RunStart => self.run_start,
                    TimeBase::Sent => self.sent,
                };
                render_time(base, *offset, *rendering).map_err(|_| HoleError::Time)
            }
            Hole::Captured { subject, slot, .. } => {
                let c = self
                    .captured
                    .iter()
                    .find(|c| c.subject == *subject && c.slot == *slot)
                    .ok_or_else(|| HoleError::Missing { slot: slot.clone() })?;
                match json::parse(&c.value) {
                    Ok(Json::String(s)) => Ok(Filled::Text(s)),
                    Ok(Json::Number(n)) => Ok(Filled::Json(n.text().to_owned())),
                    Ok(_) => Err(HoleError::NotScalar { slot: slot.clone() }),
                    Err(_) => Err(HoleError::NotJson { slot: slot.clone() }),
                }
            }
        }
    }

    /// A plain value's text: a path parameter, a query value, a header.
    pub fn text(&self, value: &Value) -> Result<String, HoleError> {
        match value {
            Value::Literal(s) => Ok(s.clone()),
            Value::Hole(h) => match self.filled(h)? {
                Filled::Text(s) | Filled::Json(s) => Ok(s),
                Filled::Integer(i) => Ok(i.to_string()),
                Filled::Bytes(_) => Err(HoleError::NotText),
            },
        }
    }
}
