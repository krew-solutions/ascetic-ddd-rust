//! End-to-end demonstration of routing slip serialization.
//!
//! Shows how a saga can be paused on one service, transmitted over a message
//! bus as JSON, and resumed on another service. Two scenarios are included:
//!
//! * [`run_travel_booking_with_serialization`] - forward path, one round-trip
//!   after the first activity, then continue processing on the receiving side;
//! * [`run_compensation_with_serialization`] - forward path until a deliberate
//!   failure, then a round-trip to a "compensation service" that runs the
//!   backward path.
//!
//! Run as an example binary:
//!
//! ```text
//! cargo run --example serialization_example
//! ```

use crate::activity_resolver::MapBasedResolver;
use crate::error::{Result, SagaError};
use crate::examples::reserve_car_activity::ReserveCarActivity;
use crate::examples::reserve_flight_activity::{
    FailingReserveFlightActivity, ReserveFlightActivity,
};
use crate::examples::reserve_hotel_activity::ReserveHotelActivity;
use crate::routing_slip::RoutingSlip;
use crate::routing_slip_serialization::{from_serializable, to_serializable};
use crate::serializable_routing_slip::SerializableRoutingSlip;
use crate::work_item::WorkItem;
use crate::work_item_arguments::WorkItemArguments;

/// Resolver that knows every example activity.
///
/// A real distributed deployment would typically use narrower per-service
/// resolvers; a single orchestrator-wide resolver is used here to keep the
/// example self-contained.
pub fn make_orchestrator_resolver() -> MapBasedResolver {
    let mut resolver = MapBasedResolver::new();
    resolver.register_type::<ReserveCarActivity>("ReserveCarActivity");
    resolver.register_type::<ReserveHotelActivity>("ReserveHotelActivity");
    resolver.register_type::<ReserveFlightActivity>("ReserveFlightActivity");
    // FailingReserveFlightActivity reuses its parent's canonical name on
    // purpose: on the wire there is no difference between "succeed" and "fail"
    // implementations -- the receiving service decides which type to bind by
    // what is registered.
    resolver.register_type::<FailingReserveFlightActivity>("FailingReserveFlightActivity");
    resolver
}

/// Round-trips a routing slip through JSON, simulating a message bus.
pub fn transmit(routing_slip: &RoutingSlip, resolver: &MapBasedResolver) -> Result<RoutingSlip> {
    let wire = serde_json::to_string(&to_serializable(routing_slip, resolver)?)
        .map_err(SagaError::activity)?;
    println!("---- on the wire ----");
    println!("{wire}");
    println!("---------------------");

    let serializable: SerializableRoutingSlip =
        serde_json::from_str(&wire).map_err(SagaError::activity)?;
    from_serializable(&serializable, resolver)
}

/// Forward-path scenario with one mid-saga handoff.
///
/// Sequence:
///
/// 1. the orchestrator builds the routing slip and processes the car step;
/// 2. the slip is serialized, "shipped" as JSON, and reconstructed on a
///    downstream service;
/// 3. the downstream service finishes hotel + flight.
///
/// Returns the routing slip in its final, fully-completed state.
pub async fn run_travel_booking_with_serialization() -> Result<RoutingSlip> {
    let resolver = make_orchestrator_resolver();

    let mut routing_slip = RoutingSlip::new([
        WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([
            ("vehicleType", "SUV"),
            ("pickupDate", "2024-01-15"),
        ])),
        WorkItem::of::<ReserveHotelActivity>(WorkItemArguments::from([
            ("roomType", "Suite"),
            ("checkInDate", "2024-01-15"),
        ])),
        WorkItem::of::<ReserveFlightActivity>(WorkItemArguments::from([
            ("destination", "LAX"),
            ("flightDate", "2024-01-15"),
        ])),
    ]);

    println!("\n=== Travel booking saga: process car on orchestrator ===");
    routing_slip.process_next().await?;

    println!("\n=== Hand off to downstream service ===");
    let mut routing_slip = transmit(&routing_slip, &resolver)?;

    println!("\n=== Resume on downstream service: hotel, then flight ===");
    while !routing_slip.is_completed() {
        routing_slip.process_next().await?;
    }

    println!(
        "Done. completed={}, in_progress={}",
        routing_slip.completed_work_logs().len(),
        routing_slip.is_in_progress(),
    );
    Ok(routing_slip)
}

/// Compensation-path scenario with a handoff to a compensation service.
///
/// Sequence:
///
/// 1. the saga runs forward until [`FailingReserveFlightActivity`] fails;
/// 2. the orchestrator serializes the slip and "ships" it to the compensation
///    service;
/// 3. the compensation service runs the backward path to completion.
///
/// Returns the routing slip after compensation -- no completed logs left.
pub async fn run_compensation_with_serialization() -> Result<RoutingSlip> {
    let resolver = make_orchestrator_resolver();

    let mut routing_slip = RoutingSlip::new([
        WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([("vehicleType", "SUV")])),
        WorkItem::of::<ReserveHotelActivity>(WorkItemArguments::from([("roomType", "Suite")])),
        WorkItem::of::<FailingReserveFlightActivity>(WorkItemArguments::from([(
            "destination",
            "LAX",
        )])),
    ]);

    println!("\n=== Compensation saga: run forward path until failure ===");
    while !routing_slip.is_completed() {
        if !routing_slip.process_next().await? {
            println!("Forward step failed -- need to compensate");
            break;
        }
    }

    println!("\n=== Hand off to compensation service ===");
    let mut routing_slip = transmit(&routing_slip, &resolver)?;

    println!("\n=== Run backward path on compensation service ===");
    while routing_slip.is_in_progress() {
        routing_slip.undo_last().await?;
    }

    println!(
        "Done. completed={}, in_progress={}",
        routing_slip.completed_work_logs().len(),
        routing_slip.is_in_progress(),
    );
    Ok(routing_slip)
}
