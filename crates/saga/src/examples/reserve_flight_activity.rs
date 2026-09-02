//! Reserve flight activity - example activity for the travel booking saga.

use async_trait::async_trait;

use crate::activity::{Activity, ActivityType};
use crate::error::Result;
use crate::examples::rng::SeededRng;
use crate::routing_slip::RoutingSlip;
use crate::work_item::WorkItem;
use crate::work_log::WorkLog;
use crate::work_result::WorkResult;

static RNG: SeededRng = SeededRng::new(3);

/// Activity for reserving a flight.
///
/// This is the highest risk step in a travel booking saga, as flights often
/// have strict refund policies.
#[derive(Debug, Default)]
pub struct ReserveFlightActivity;

#[async_trait]
impl Activity for ReserveFlightActivity {
    /// Reserves a flight.
    ///
    /// `work_item` must carry `destination`; the result carries `reservationId`.
    /// A missing `destination` fails the work, which is what makes the
    /// compensation path observable in the examples.
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        let _destination = work_item.arguments().get_str("destination")?;
        let reservation_id = RNG.next_reservation_id();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("reservationId", reservation_id)]),
        )))
    }

    /// Cancels the flight reservation and continues the backward path.
    async fn compensate(
        &self,
        work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        let _reservation_id = work_log.result().get_i64("reservationId")?;
        Ok(true)
    }

    /// Queue address for flight reservation requests.
    fn work_item_queue_address(&self) -> &str {
        "sb://./flightReservations"
    }

    /// Queue address for flight cancellation requests.
    fn compensation_queue_address(&self) -> &str {
        "sb://./flightCancellations"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }

    /// Canonical name used by an activity type resolver for serialization.
    fn type_name(&self) -> Option<&str> {
        Some("ReserveFlightActivity")
    }
}

/// Flight activity that always fails - for demonstrating compensation.
///
/// Python derives it from [`ReserveFlightActivity`]; Rust has no inheritance,
/// so it delegates to an embedded instance instead -- including
/// [`type_name()`][Activity::type_name], which stays `"ReserveFlightActivity"`.
#[derive(Debug, Default)]
pub struct FailingReserveFlightActivity(ReserveFlightActivity);

#[async_trait]
impl Activity for FailingReserveFlightActivity {
    /// Attempts to reserve a flight (always fails).
    ///
    /// This activity intentionally reads a non-existent key to demonstrate the
    /// saga's compensation mechanism.
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        // This fails with SagaError::MissingKey, the counterpart of KeyError.
        work_item.arguments().require("fatzbatz")?;
        self.0.do_work(work_item).await
    }

    async fn compensate(&self, work_log: &WorkLog, routing_slip: &mut RoutingSlip) -> Result<bool> {
        self.0.compensate(work_log, routing_slip).await
    }

    fn work_item_queue_address(&self) -> &str {
        self.0.work_item_queue_address()
    }

    fn compensation_queue_address(&self) -> &str {
        self.0.compensation_queue_address()
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }

    fn type_name(&self) -> Option<&str> {
        self.0.type_name()
    }
}
