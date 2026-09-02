//! Fallback activity - tries alternative routing slips until one succeeds.

use async_trait::async_trait;

use crate::activity::{Activity, ActivityType};
use crate::error::Result;
use crate::routing_slip::{RoutingSlip, SharedRoutingSlip};
use crate::value::Value;
use crate::work_item::WorkItem;
use crate::work_item_arguments::WorkItemArguments;
use crate::work_log::WorkLog;
use crate::work_result::WorkResult;

/// Activity that tries alternative routing slips until one succeeds.
///
/// Based on Section 6 "Recovery Blocks" of Garcia-Molina & Salem's "Sagas" (1987).
///
/// Each alternative is a full [`RoutingSlip`] with its own forward/backward paths.
///
/// Behavior:
///
/// * tries each alternative routing slip in order;
/// * stops on the first success;
/// * if an alternative fails, it compensates itself before the next one is tried;
/// * only the successful alternative needs compensation.
///
/// ```
/// use ascetic_ddd_saga::{FallbackActivity, RoutingSlip, WorkItem, WorkItemArguments};
/// use ascetic_ddd_saga::examples::{ReserveCarActivity, ReserveHotelActivity};
///
/// let work_item = WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
///     RoutingSlip::new([WorkItem::of::<ReserveCarActivity>(
///         WorkItemArguments::from([("vehicleType", "Compact")]),
///     )])
///     .into_shared(),
///     RoutingSlip::new([WorkItem::of::<ReserveHotelActivity>(
///         WorkItemArguments::from([("roomType", "Suite")]),
///     )])
///     .into_shared(),
/// ]));
/// ```
#[derive(Debug, Default)]
pub struct FallbackActivity;

impl FallbackActivity {
    /// Argument key holding the alternatives to try.
    pub const ALTERNATIVES: &'static str = "alternatives";

    /// Result key holding the alternative that succeeded.
    pub const SUCCEEDED: &'static str = "_succeeded";

    /// Builds the arguments this activity expects.
    pub fn arguments(
        alternatives: impl IntoIterator<Item = SharedRoutingSlip>,
    ) -> WorkItemArguments {
        let alternatives: Vec<SharedRoutingSlip> = alternatives.into_iter().collect();
        WorkItemArguments::from([(Self::ALTERNATIVES, Value::any(alternatives))])
    }

    /// Executes an alternative routing slip to completion.
    async fn execute_alternative(&self, alternative: &SharedRoutingSlip) -> Result<bool> {
        let mut alternative = alternative.lock().await;

        while !alternative.is_completed() {
            if !alternative.process_next().await? {
                // Alternative failed - compensate and report the failure.
                while alternative.is_in_progress() {
                    alternative.undo_last().await?;
                }
                return Ok(false);
            }
        }

        Ok(true)
    }
}

#[async_trait]
impl Activity for FallbackActivity {
    /// Tries alternative routing slips until one succeeds.
    ///
    /// `work_item` must carry [`FallbackActivity::ALTERNATIVES`] - the list of
    /// [`SharedRoutingSlip`] to try. Returns the work log holding the
    /// successful alternative, or `None` if all of them failed.
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        let alternatives: &Vec<SharedRoutingSlip> =
            work_item.arguments().get_any(Self::ALTERNATIVES)?;

        for alternative in alternatives {
            if self.execute_alternative(alternative).await? {
                // Store which alternative succeeded for future compensation.
                return Ok(Some(WorkLog::new(
                    self,
                    WorkResult::from([(Self::SUCCEEDED, Value::any(alternative.clone()))]),
                )));
            }
        }

        // All alternatives failed.
        Ok(None)
    }

    /// Compensates the successful alternative, then continues the backward path.
    async fn compensate(
        &self,
        work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        let succeeded: &SharedRoutingSlip = work_log.result().get_any(Self::SUCCEEDED)?;
        let mut succeeded = succeeded.lock().await;

        while succeeded.is_in_progress() {
            succeeded.undo_last().await?;
        }

        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./fallback"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./fallbackCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}
