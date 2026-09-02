//! Tests for the example activities.

use ascetic_ddd_saga::examples::{
    FailingReserveFlightActivity, ReserveCarActivity, ReserveFlightActivity, ReserveHotelActivity,
};
use ascetic_ddd_saga::{Activity, RoutingSlip, SagaError, WorkItem, WorkItemArguments};
use futures::executor::block_on;

#[test]
fn reserve_car_do_work_creates_reservation() {
    block_on(async {
        let activity = ReserveCarActivity;
        let work_item = WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([(
            "vehicleType",
            "Compact",
        )]));

        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();

        assert!(work_log.result().contains_key("reservationId"));
        assert!(work_log.result().get_i64("reservationId").is_ok());
    });
}

#[test]
fn reserve_car_compensate_continues_backward() {
    block_on(async {
        let activity = ReserveCarActivity;
        let work_item =
            WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([("vehicleType", "SUV")]));
        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();

        let compensated = activity
            .compensate(&work_log, &mut RoutingSlip::default())
            .await
            .unwrap();

        assert!(compensated);
    });
}

#[test]
fn reserve_car_queue_addresses() {
    let activity = ReserveCarActivity;

    assert_eq!(activity.work_item_queue_address(), "sb://./carReservations");
    assert_eq!(
        activity.compensation_queue_address(),
        "sb://./carCancellations",
    );
}

#[test]
fn reserve_hotel_do_work_creates_reservation() {
    block_on(async {
        let activity = ReserveHotelActivity;
        let work_item =
            WorkItem::of::<ReserveHotelActivity>(WorkItemArguments::from([("roomType", "Suite")]));

        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();

        assert!(work_log.result().get_i64("reservationId").is_ok());
    });
}

#[test]
fn reserve_hotel_compensate_continues_backward() {
    block_on(async {
        let activity = ReserveHotelActivity;
        let work_item = WorkItem::of::<ReserveHotelActivity>(WorkItemArguments::from([(
            "roomType", "Standard",
        )]));
        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();

        let compensated = activity
            .compensate(&work_log, &mut RoutingSlip::default())
            .await
            .unwrap();

        assert!(compensated);
    });
}

#[test]
fn reserve_hotel_queue_addresses() {
    let activity = ReserveHotelActivity;

    assert_eq!(
        activity.work_item_queue_address(),
        "sb://./hotelReservations",
    );
    assert_eq!(
        activity.compensation_queue_address(),
        "sb://./hotelCancellations",
    );
}

#[test]
fn reserve_flight_do_work_creates_reservation() {
    block_on(async {
        let activity = ReserveFlightActivity;
        let work_item = WorkItem::of::<ReserveFlightActivity>(WorkItemArguments::from([(
            "destination",
            "DUS",
        )]));

        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();

        assert!(work_log.result().get_i64("reservationId").is_ok());
    });
}

#[test]
fn reserve_flight_compensate_continues_backward() {
    block_on(async {
        let activity = ReserveFlightActivity;
        let work_item = WorkItem::of::<ReserveFlightActivity>(WorkItemArguments::from([(
            "destination",
            "FRA",
        )]));
        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();

        let compensated = activity
            .compensate(&work_log, &mut RoutingSlip::default())
            .await
            .unwrap();

        assert!(compensated);
    });
}

#[test]
fn reserve_flight_queue_addresses() {
    let activity = ReserveFlightActivity;

    assert_eq!(
        activity.work_item_queue_address(),
        "sb://./flightReservations",
    );
    assert_eq!(
        activity.compensation_queue_address(),
        "sb://./flightCancellations",
    );
}

/// The counterpart of the `KeyError` raised by the Python implementation.
#[test]
fn failing_reserve_flight_do_work_reports_a_missing_key() {
    block_on(async {
        let activity = FailingReserveFlightActivity::default();
        let work_item = WorkItem::of::<FailingReserveFlightActivity>(WorkItemArguments::from([(
            "destination",
            "DUS",
        )]));

        assert!(matches!(
            activity.do_work(&work_item).await,
            Err(SagaError::MissingKey(key)) if key == "fatzbatz",
        ));
    });
}

#[test]
fn failing_reserve_flight_inherits_queue_addresses() {
    let activity = FailingReserveFlightActivity::default();

    assert_eq!(
        activity.work_item_queue_address(),
        "sb://./flightReservations",
    );
    assert_eq!(
        activity.compensation_queue_address(),
        "sb://./flightCancellations",
    );
}

#[test]
fn reserve_car_activity_is_named() {
    assert_eq!(ReserveCarActivity.type_name(), Some("ReserveCarActivity"));
}

#[test]
fn reserve_hotel_activity_is_named() {
    assert_eq!(
        ReserveHotelActivity.type_name(),
        Some("ReserveHotelActivity"),
    );
}

#[test]
fn reserve_flight_activity_is_named() {
    assert_eq!(
        ReserveFlightActivity.type_name(),
        Some("ReserveFlightActivity"),
    );
}

/// FailingReserveFlightActivity reports the name of the activity it wraps.
#[test]
fn failing_reserve_flight_activity_inherits_its_name() {
    assert_eq!(
        FailingReserveFlightActivity::default().type_name(),
        Some("ReserveFlightActivity"),
    );
}

#[test]
fn successful_booking() {
    block_on(async {
        let mut slip = RoutingSlip::new([
            WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([(
                "vehicleType",
                "Compact",
            )])),
            WorkItem::of::<ReserveHotelActivity>(WorkItemArguments::from([("roomType", "Suite")])),
            WorkItem::of::<ReserveFlightActivity>(WorkItemArguments::from([(
                "destination",
                "DUS",
            )])),
        ]);

        while !slip.is_completed() {
            assert!(slip.process_next().await.unwrap());
        }

        assert!(slip.is_completed());
        assert_eq!(slip.completed_work_logs().len(), 3);
    });
}

#[test]
fn failed_booking_triggers_compensation() {
    block_on(async {
        let mut slip = RoutingSlip::new([
            WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([(
                "vehicleType",
                "Compact",
            )])),
            WorkItem::of::<ReserveHotelActivity>(WorkItemArguments::from([("roomType", "Suite")])),
            WorkItem::of::<FailingReserveFlightActivity>(WorkItemArguments::from([(
                "destination",
                "DUS",
            )])),
        ]);

        // Process until failure.
        let mut completed_before_failure = 0;
        while !slip.is_completed() {
            if slip.process_next().await.unwrap() {
                completed_before_failure += 1;
            } else {
                break;
            }
        }

        assert_eq!(completed_before_failure, 2);

        // Compensate.
        let mut compensated = 0;
        while slip.is_in_progress() {
            slip.undo_last().await.unwrap();
            compensated += 1;
        }

        assert_eq!(compensated, 2);
        assert!(!slip.is_in_progress());
    });
}
