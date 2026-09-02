//! Reserve hotel activity - example activity for the travel booking saga.

use async_trait::async_trait;

use crate::activity::{Activity, ActivityType};
use crate::error::Result;
use crate::examples::rng::SeededRng;
use crate::routing_slip::RoutingSlip;
use crate::work_item::WorkItem;
use crate::work_log::WorkLog;
use crate::work_result::WorkResult;

static RNG: SeededRng = SeededRng::new(1);

/// Activity for reserving a hotel room.
///
/// This is a moderate risk step in a travel booking saga, as hotels typically
/// allow cancellation until 24 hours before check-in.
#[derive(Debug, Default)]
pub struct ReserveHotelActivity;

#[async_trait]
impl Activity for ReserveHotelActivity {
    /// Reserves a hotel room.
    ///
    /// `work_item` must carry `roomType`; the result carries `reservationId`.
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        let _room_type = work_item.arguments().get_str("roomType")?;
        let reservation_id = RNG.next_reservation_id();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("reservationId", reservation_id)]),
        )))
    }

    /// Cancels the hotel reservation and continues the backward path.
    async fn compensate(
        &self,
        work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        let _reservation_id = work_log.result().get_i64("reservationId")?;
        Ok(true)
    }

    /// Queue address for hotel reservation requests.
    fn work_item_queue_address(&self) -> &str {
        "sb://./hotelReservations"
    }

    /// Queue address for hotel cancellation requests.
    fn compensation_queue_address(&self) -> &str {
        "sb://./hotelCancellations"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }

    /// Canonical name used by an activity type resolver for serialization.
    fn type_name(&self) -> Option<&str> {
        Some("ReserveHotelActivity")
    }
}
