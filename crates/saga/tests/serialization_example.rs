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
use ascetic_ddd_saga::{
    ActivityType, ActivityTypeResolver, MapBasedResolver, RoutingSlip, SagaError,
    SerializableRoutingSlip, WorkItem, WorkItemArguments, from_serializable, to_serializable,
};
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

/// Service-specific resolvers limit what activities each service can restore.
///
/// The counterpart of Go's `TestSerializationExample_MultipleResolvers` and of
/// the "Multiple Resolvers for Different Services" section of
/// [SERIALIZATION.md](../SERIALIZATION.md).
#[test]
fn multiple_resolvers_for_different_services() {
    block_on(async {
        // The car service knows only the car activity, the flight service only
        // the flight one; the orchestrator knows every activity.
        let mut car_service = MapBasedResolver::new();
        car_service.register_type::<ReserveCarActivity>("ReserveCarActivity");
        let mut flight_service = MapBasedResolver::new();
        flight_service.register_type::<ReserveFlightActivity>("ReserveFlightActivity");
        let orchestrator = make_orchestrator_resolver();

        let mut slip = RoutingSlip::new([
            WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([("vehicleType", "SUV")])),
            WorkItem::of::<ReserveFlightActivity>(WorkItemArguments::from([(
                "destination",
                "LAX",
            )])),
        ]);
        slip.process_next().await.unwrap();

        let wire = serde_json::to_string(&to_serializable(&slip, &orchestrator).unwrap()).unwrap();
        let serializable: SerializableRoutingSlip = serde_json::from_str(&wire).unwrap();

        // The flight service cannot restore the completed car work log...
        assert!(matches!(
            from_serializable(&serializable, &flight_service),
            Err(SagaError::ActivityTypeNotRegistered(name)) if name == "ReserveCarActivity",
        ));
        // ...and the car service cannot restore the pending flight work item.
        assert!(matches!(
            from_serializable(&serializable, &car_service),
            Err(SagaError::ActivityTypeNotRegistered(name)) if name == "ReserveFlightActivity",
        ));

        // The orchestrator knows both, so the saga resumes.
        let mut restored = from_serializable(&serializable, &orchestrator).unwrap();
        restored.process_next().await.unwrap();

        assert!(restored.is_completed());
        assert_eq!(restored.completed_work_logs().len(), 2);
    });
}
