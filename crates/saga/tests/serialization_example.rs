//! Integration tests for the `serialization_example` module.
//!
//! Asserts that the runnable example does what its documentation claims, so
//! that anything pointing readers at it stays accurate.

use ascetic_ddd_saga::examples::serialization_example::{
    make_orchestrator_resolver, run_compensation_with_serialization,
    run_travel_booking_with_serialization,
};
use ascetic_ddd_saga::examples::{
    FailingReserveFlightActivity, ReserveCarActivity, ReserveFlightActivity, ReserveHotelActivity,
};
use ascetic_ddd_saga::{ActivityType, ActivityTypeResolver};
use futures::executor::block_on;

#[test]
fn registers_all_example_activities() {
    let resolver = make_orchestrator_resolver();

    assert_eq!(
        resolver.resolve("ReserveCarActivity").unwrap(),
        ActivityType::of::<ReserveCarActivity>(),
    );
    assert_eq!(
        resolver.resolve("ReserveHotelActivity").unwrap(),
        ActivityType::of::<ReserveHotelActivity>(),
    );
    assert_eq!(
        resolver.resolve("ReserveFlightActivity").unwrap(),
        ActivityType::of::<ReserveFlightActivity>(),
    );
    assert_eq!(
        resolver.resolve("FailingReserveFlightActivity").unwrap(),
        ActivityType::of::<FailingReserveFlightActivity>(),
    );
}

/// Each call returns an isolated resolver -- no shared global state.
#[test]
fn returns_a_fresh_resolver_each_call() {
    let mut a = make_orchestrator_resolver();
    let b = make_orchestrator_resolver();

    a.register_type::<ReserveCarActivity>("Renamed");

    assert!(a.resolve("Renamed").is_ok());
    assert!(b.resolve("Renamed").is_err());
}

/// The forward-path scenario completes the saga end-to-end.
#[test]
fn travel_booking_completes_after_handoff() {
    block_on(async {
        let slip = run_travel_booking_with_serialization().await.unwrap();

        assert!(slip.is_completed());
        assert!(slip.is_in_progress());
        assert_eq!(slip.completed_work_logs().len(), 3);
    });
}

/// The compensation-path scenario rolls every completed activity back.
#[test]
fn compensation_rolls_completed_work_back() {
    block_on(async {
        let slip = run_compensation_with_serialization().await.unwrap();

        assert!(!slip.is_in_progress());
        assert_eq!(slip.completed_work_logs().len(), 0);
    });
}
