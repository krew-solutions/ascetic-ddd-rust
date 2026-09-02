//! Parallel activity - executes multiple routing slips concurrently.

use async_trait::async_trait;
use futures::future::join_all;

use crate::activity::{Activity, ActivityType};
use crate::error::Result;
use crate::routing_slip::{RoutingSlip, SharedRoutingSlip};
use crate::value::Value;
use crate::work_item::WorkItem;
use crate::work_item_arguments::WorkItemArguments;
use crate::work_log::WorkLog;
use crate::work_result::WorkResult;

/// Activity that executes multiple routing slips in parallel (fork/join).
///
/// Based on Section 8 of Garcia-Molina & Salem's "Sagas" (1987).
///
/// Each branch is a full [`RoutingSlip`] with its own forward/backward paths.
///
/// Behavior:
///
/// * executes all branch routing slips concurrently;
/// * fail-fast: on the first failure, compensates every branch;
/// * compensation: all branches are compensated in parallel.
///
/// ```
/// use ascetic_ddd_saga::{ParallelActivity, RoutingSlip, WorkItem, WorkItemArguments};
/// use ascetic_ddd_saga::examples::{ReserveCarActivity, ReserveHotelActivity};
///
/// let work_item = WorkItem::of::<ParallelActivity>(ParallelActivity::arguments([
///     RoutingSlip::new([WorkItem::of::<ReserveHotelActivity>(
///         WorkItemArguments::from([("roomType", "Suite")]),
///     )])
///     .into_shared(),
///     RoutingSlip::new([WorkItem::of::<ReserveCarActivity>(
///         WorkItemArguments::from([("vehicleType", "Compact")]),
///     )])
///     .into_shared(),
/// ]));
/// ```
#[derive(Debug, Default)]
pub struct ParallelActivity;

impl ParallelActivity {
    /// Argument key holding the branches to execute.
    pub const BRANCHES: &'static str = "branches";

    /// Result key holding the executed branches.
    pub const EXECUTED_BRANCHES: &'static str = "_branches";

    /// Builds the arguments this activity expects.
    pub fn arguments(branches: impl IntoIterator<Item = SharedRoutingSlip>) -> WorkItemArguments {
        let branches: Vec<SharedRoutingSlip> = branches.into_iter().collect();
        WorkItemArguments::from([(Self::BRANCHES, Value::any(branches))])
    }

    /// Executes a single branch routing slip to completion.
    async fn execute_branch(&self, branch: &SharedRoutingSlip) -> Result<bool> {
        let mut branch = branch.lock().await;

        while !branch.is_completed() {
            if !branch.process_next().await? {
                // Branch failed - compensate this branch.
                while branch.is_in_progress() {
                    branch.undo_last().await?;
                }
                return Ok(false);
            }
        }

        Ok(true)
    }

    /// Compensates all branches in parallel.
    async fn compensate_branches(&self, branches: &[SharedRoutingSlip]) {
        join_all(branches.iter().map(|branch| self.compensate_branch(branch))).await;
    }

    /// Compensates a single branch.
    async fn compensate_branch(&self, branch: &SharedRoutingSlip) {
        let mut branch = branch.lock().await;

        while branch.is_in_progress() {
            // Errors are swallowed: a branch that cannot be compensated must
            // not prevent its siblings from being compensated.
            if branch.undo_last().await.is_err() {
                break;
            }
        }
    }
}

#[async_trait]
impl Activity for ParallelActivity {
    /// Executes all branch routing slips in parallel.
    ///
    /// `work_item` must carry [`ParallelActivity::BRANCHES`] - the list of
    /// [`SharedRoutingSlip`] to execute. Returns the work log holding the
    /// branches, or `None` if any branch failed.
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        let branches: &Vec<SharedRoutingSlip> = work_item.arguments().get_any(Self::BRANCHES)?;

        // Execute all branches in parallel.
        let results = join_all(branches.iter().map(|branch| self.execute_branch(branch))).await;

        // Check for failures.
        if results.iter().any(|result| !matches!(result, Ok(true))) {
            // Fail-fast: compensate all branches (completed and partial).
            self.compensate_branches(branches).await;
            return Ok(None);
        }

        // All succeeded - store branches for future compensation.
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([(Self::EXECUTED_BRANCHES, Value::any(branches.clone()))]),
        )))
    }

    /// Compensates all branches in parallel, then continues the backward path.
    async fn compensate(
        &self,
        work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        let branches: &Vec<SharedRoutingSlip> =
            work_log.result().get_any(Self::EXECUTED_BRANCHES)?;
        self.compensate_branches(branches).await;
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./parallel"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./parallelCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}
