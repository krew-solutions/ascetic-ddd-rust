//! Routing slip - the document that flows through the saga.

use std::collections::VecDeque;
use std::sync::Arc;

use futures::lock::Mutex;

use crate::error::{Result, SagaError};
use crate::work_item::WorkItem;
use crate::work_log::WorkLog;

/// A routing slip shared by several owners.
///
/// Nested slips ([`FallbackActivity`] alternatives, [`ParallelActivity`]
/// branches) are handed to an activity, mutated by it and later compensated
/// through the same handle -- shared mutable state that Python gets for free
/// from reference semantics.
///
/// [`FallbackActivity`]: crate::fallback_activity::FallbackActivity
/// [`ParallelActivity`]: crate::parallel_activity::ParallelActivity
pub type SharedRoutingSlip = Arc<Mutex<RoutingSlip>>;

/// The routing slip that flows through the saga.
///
/// Contains:
///
/// * a queue of pending work items (forward path);
/// * a stack of completed work logs (backward path).
///
/// The routing slip carries all transaction context and can be serialized for
/// transmission between distributed systems -- see
/// [`routing_slip_serialization`][crate::routing_slip_serialization].
#[derive(Debug, Default)]
pub struct RoutingSlip {
    completed_work_logs: Vec<WorkLog>,
    next_work_items: VecDeque<WorkItem>,
}

impl RoutingSlip {
    /// Creates a routing slip that will process the given work items in order.
    pub fn new(work_items: impl IntoIterator<Item = WorkItem>) -> Self {
        RoutingSlip {
            completed_work_logs: Vec::new(),
            next_work_items: work_items.into_iter().collect(),
        }
    }

    /// Wraps the slip into a handle that can be shared between owners.
    pub fn into_shared(self) -> SharedRoutingSlip {
        Arc::new(Mutex::new(self))
    }

    /// True if all work items have been processed.
    pub fn is_completed(&self) -> bool {
        self.next_work_items.is_empty()
    }

    /// True if some work has been completed (and can be compensated).
    pub fn is_in_progress(&self) -> bool {
        !self.completed_work_logs.is_empty()
    }

    /// Processes the next work item in the queue.
    ///
    /// Returns `true` if the work was successful, `false` otherwise. A failing
    /// activity -- one returning `None` *or* an error -- is reported as `false`
    /// and its error is swallowed, so that the caller can start compensation
    /// without special-casing the failure mode. This mirrors the `except
    /// Exception: pass` of the Python implementation.
    ///
    /// # Errors
    ///
    /// [`SagaError::InvalidOperation`] if there are no more work items.
    pub async fn process_next(&mut self) -> Result<bool> {
        if self.is_completed() {
            return Err(SagaError::invalid_operation(
                "No more work items to process",
            ));
        }

        let current_item = self
            .next_work_items
            .pop_front()
            .expect("the queue is not empty");
        let activity = current_item.activity_type().create();

        match activity.do_work(&current_item).await {
            Ok(Some(work_log)) => {
                self.completed_work_logs.push(work_log);
                Ok(true)
            }
            Ok(None) | Err(_) => Ok(false),
        }
    }

    /// Address of the next activity's work queue, or `None` if completed.
    pub fn progress_uri(&self) -> Option<String> {
        let work_item = self.next_work_items.front()?;
        let activity = work_item.activity_type().create();
        Some(activity.work_item_queue_address().to_owned())
    }

    /// Address of the last completed activity's compensation queue.
    pub fn compensation_uri(&self) -> Option<String> {
        let work_log = self.completed_work_logs.last()?;
        let activity = work_log.activity_type().create();
        Some(activity.compensation_queue_address().to_owned())
    }

    /// Undoes the last completed work item.
    ///
    /// Returns `true` if compensation succeeded and the backward path should
    /// continue, `false` if compensation added new work and the forward path
    /// should resume.
    ///
    /// # Errors
    ///
    /// [`SagaError::InvalidOperation`] if there is no work to undo; any error
    /// raised by the compensating activity is propagated unchanged.
    pub async fn undo_last(&mut self) -> Result<bool> {
        if !self.is_in_progress() {
            return Err(SagaError::invalid_operation("No work to undo"));
        }

        let current_item = self
            .completed_work_logs
            .pop()
            .expect("the stack is not empty");
        let activity = current_item.activity_type().create();

        activity.compensate(&current_item, self).await
    }

    /// Completed work logs, oldest first (for inspection/testing).
    pub fn completed_work_logs(&self) -> &[WorkLog] {
        &self.completed_work_logs
    }

    /// Queue of pending work items (for inspection/testing).
    pub fn pending_work_items(&self) -> &VecDeque<WorkItem> {
        &self.next_work_items
    }

    /// Appends a work item to the forward path.
    ///
    /// Compensation uses this to add new work and resume forward; Python
    /// mutates the deque returned by `pending_work_items` instead.
    pub fn add_work_item(&mut self, work_item: WorkItem) {
        self.next_work_items.push_back(work_item);
    }

    /// Puts a work item at the front of the forward path.
    pub fn add_next_work_item(&mut self, work_item: WorkItem) {
        self.next_work_items.push_front(work_item);
    }

    /// Pushes a completed work log onto the backward path.
    ///
    /// Used when restoring a slip from its serialized form.
    pub fn add_completed_work_log(&mut self, work_log: WorkLog) {
        self.completed_work_logs.push(work_log);
    }
}

impl FromIterator<WorkItem> for RoutingSlip {
    fn from_iter<I: IntoIterator<Item = WorkItem>>(work_items: I) -> Self {
        RoutingSlip::new(work_items)
    }
}
