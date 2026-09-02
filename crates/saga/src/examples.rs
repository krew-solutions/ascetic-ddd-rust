//! Example activities for the travel booking saga.
//!
//! Contains example implementations of saga activities for a travel booking
//! scenario, demonstrating:
//!
//! * [`ReserveCarActivity`] - low-risk, easily cancellable;
//! * [`ReserveHotelActivity`] - moderate risk, cancellable until check-in;
//! * [`ReserveFlightActivity`] - high risk, strict refund policies;
//! * [`FailingReserveFlightActivity`] - always fails, for testing compensation.
//!
//! The activities are ordered by risk (least risky first) to minimize the need
//! for compensation when failures occur.

mod rng;

pub mod reserve_car_activity;
pub mod reserve_flight_activity;
pub mod reserve_hotel_activity;
pub mod serialization_example;

pub use crate::examples::reserve_car_activity::ReserveCarActivity;
pub use crate::examples::reserve_flight_activity::{
    FailingReserveFlightActivity, ReserveFlightActivity,
};
pub use crate::examples::reserve_hotel_activity::ReserveHotelActivity;

// Note: serialization_example is intentionally NOT re-exported here. It is a
// runnable demo, kept behind its own module path so that it reads as a script
// rather than as part of the example activity set.
