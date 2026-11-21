//! Inputs in the order the machine takes them (ADR-0047 decision 4): by the
//! instant their source saw them, then line, then kind — whatever order they
//! arrived in — released up to a watermark, the least instant any source may
//! still report.

use std::collections::BTreeMap;

use crate::machine::{Input, Line};

/// A buffer of stamped inputs.
#[derive(Debug, Default)]
pub struct Ordered {
    held: BTreeMap<(u64, Line, u8, u64), Input>,
    seq: u64,
}

impl Ordered {
    /// An empty buffer.
    pub fn new() -> Ordered {
        Ordered::default()
    }

    /// An input its source saw at `at`.
    pub fn push(&mut self, at: u64, input: Input) {
        self.seq = self.seq.saturating_add(1);
        self.held
            .insert((at, input.line(), input.kind(), self.seq), input);
    }

    /// The next input stamped no later than `watermark`, in order.
    pub fn pop(&mut self, watermark: u64) -> Option<(u64, Input)> {
        let (&key, _) = self.held.first_key_value()?;
        if key.0 > watermark {
            return None;
        }
        self.held.pop_first().map(|(k, v)| (k.0, v))
    }

    /// Whether nothing is held.
    pub fn is_empty(&self) -> bool {
        self.held.is_empty()
    }

    /// The earliest instant held.
    pub fn first(&self) -> Option<u64> {
        self.held.first_key_value().map(|(k, _)| k.0)
    }
}
