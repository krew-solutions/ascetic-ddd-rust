//! Tests for `WorkLog`.

use ascetic_ddd_saga::{
    Activity, ActivityType, Result, RoutingSlip, Value, WorkItem, WorkLog, WorkResult, async_trait,
};

/// Stub activity for testing.
#[derive(Debug, Default)]
struct StubActivity;

#[async_trait]
impl Activity for StubActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        Ok(Some(WorkLog::new(self, WorkResult::from([("id", 123)]))))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./stub"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./stubCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

#[test]
fn create_work_log() {
    let activity = StubActivity;
    let result = WorkResult::from([("reservationId", 12345)]);

    let work_log = WorkLog::new(&activity, result);

    assert_eq!(work_log.activity_type(), ActivityType::of::<StubActivity>(),);
    assert_eq!(work_log.result().get_i64("reservationId").unwrap(), 12345);
}

#[test]
fn result_is_accessible() {
    let activity = StubActivity;
    let result = WorkResult::from([("key", Value::from("value")), ("count", Value::from(42))]);

    let work_log = WorkLog::new(&activity, result);

    assert_eq!(work_log.result().get_str("key").unwrap(), "value");
    assert_eq!(work_log.result().get_i64("count").unwrap(), 42);
}

#[test]
fn activity_type_is_a_type_not_an_instance() {
    let activity1 = StubActivity;
    let activity2 = StubActivity;

    let work_log = WorkLog::new(&activity1, WorkResult::new());

    assert_eq!(work_log.activity_type(), activity2.activity_type());
    assert_eq!(work_log.activity_type(), ActivityType::of::<StubActivity>());
}
