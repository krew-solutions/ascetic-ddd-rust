//! Reserve car activity - example activity for the travel booking saga.

use async_trait::async_trait;

use crate::activity::{Activity, ActivityType};
use crate::error::Result;
use crate::examples::rng::SeededRng;
use crate::routing_slip::RoutingSlip;
use crate::work_item::WorkItem;
use crate::work_log::WorkLog;
use crate::work_result::WorkResult;

static RNG: SeededRng = SeededRng::new(2);

/// Activity for reserving a rental car.
///
/// This is typically the least risky step in a travel booking saga, as car
/// reservations are usually easy to cancel.
#[derive(Debug, Default)]
pub struct ReserveCarActivity;

#[async_trait]
impl Activity for ReserveCarActivity {
    /// Reserves a car.
    ///
    /// `work_item` must carry `vehicleType`; the result carries `reservationId`.
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        let _vehicle_type = work_item.arguments().get_str("vehicleType")?;
        let reservation_id = RNG.next_reservation_id();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("reservationId", reservation_id)]),
        )))
    }

    /// Cancels the car reservation and continues the backward path.
    async fn compensate(
        &self,
        work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        let _reservation_id = work_log.result().get_i64("reservationId")?;
        Ok(true)
    }

    /// Queue address for car reservation requests.
    fn work_item_queue_address(&self) -> &str {
        "sb://./carReservations"
    }

    /// Queue address for car cancellation requests.
    fn compensation_queue_address(&self) -> &str {
        "sb://./carCancellations"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }

    /// Canonical name used by an activity type resolver for serialization.
    fn type_name(&self) -> Option<&str> {
        Some("ReserveCarActivity")
    }
}
