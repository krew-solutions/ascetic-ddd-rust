//! Work log - record of completed activity work.

use crate::activity::{Activity, ActivityType};
use crate::work_result::WorkResult;

/// Record of completed work from an activity.
///
/// Stores the activity type and its result, enabling compensation to be
/// performed later if the saga needs to be rolled back.
#[derive(Clone, Debug)]
pub struct WorkLog {
    activity_type: ActivityType,
    result: WorkResult,
}

impl WorkLog {
    /// Records the work performed by `activity`.
    ///
    /// Only the activity *type* is retained, not the instance -- the backward
    /// path creates a fresh instance to compensate with.
    pub fn new(activity: &dyn Activity, result: WorkResult) -> Self {
        WorkLog {
            activity_type: activity.activity_type(),
            result,
        }
    }

    /// Records work performed by the given activity type.
    ///
    /// Used when restoring a slip from its serialized form, where no activity
    /// instance is at hand.
    pub fn with_activity_type(activity_type: ActivityType, result: WorkResult) -> Self {
        WorkLog {
            activity_type,
            result,
        }
    }

    /// The result dictionary from the activity's work.
    pub fn result(&self) -> &WorkResult {
        &self.result
    }

    /// The type of activity that performed this work.
    pub fn activity_type(&self) -> ActivityType {
        self.activity_type
    }
}
