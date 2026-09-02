//! Shared helpers for the saga integration tests.
//!
//! Python keeps per-test state in class attributes (`SuccessActivity.call_count`)
//! and resets them in `setUp()`. Rust runs the tests of one binary in parallel
//! threads, so the same state lives in atomics guarded by a per-file lock that
//! each test acquires while it runs.

#![allow(dead_code)]

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard, PoisonError};

/// Counter shared by every instance of an activity type.
///
/// The counterpart of a Python class attribute such as `call_count`.
#[derive(Debug, Default)]
pub struct Counter(AtomicUsize);

impl Counter {
    /// Creates a counter set to zero.
    pub const fn new() -> Self {
        Counter(AtomicUsize::new(0))
    }

    /// Increments the counter.
    pub fn increment(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }

    /// Returns the current value.
    pub fn get(&self) -> usize {
        self.0.load(Ordering::SeqCst)
    }

    /// Resets the counter to zero.
    pub fn reset(&self) {
        self.0.store(0, Ordering::SeqCst);
    }
}

/// Switch shared by every instance of an activity type.
///
/// The counterpart of a Python class attribute such as `should_fail`.
#[derive(Debug, Default)]
pub struct Flag(AtomicBool);

impl Flag {
    /// Creates a flag that is not set.
    pub const fn new() -> Self {
        Flag(AtomicBool::new(false))
    }

    /// Sets the flag.
    pub fn set(&self, value: bool) {
        self.0.store(value, Ordering::SeqCst);
    }

    /// Returns the current value.
    pub fn get(&self) -> bool {
        self.0.load(Ordering::SeqCst)
    }
}

/// Acquires the file-wide test lock, ignoring poisoning from a failed test.
pub fn acquire(lock: &'static Mutex<()>) -> MutexGuard<'static, ()> {
    lock.lock().unwrap_or_else(PoisonError::into_inner)
}
