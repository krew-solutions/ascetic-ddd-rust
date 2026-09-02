//! Tests for `WorkItem`.

use ascetic_ddd_saga::{
    Activity, ActivityType, Result, RoutingSlip, WorkItem, WorkItemArguments, WorkLog, WorkResult,
    async_trait,
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
fn create_work_item() {
    let arguments = WorkItemArguments::from([("vehicleType", "SUV")]);
    let work_item = WorkItem::of::<StubActivity>(arguments);

    assert_eq!(
        work_item.activity_type(),
        ActivityType::of::<StubActivity>(),
    );
    assert_eq!(work_item.arguments().get_str("vehicleType").unwrap(), "SUV");
}

#[test]
fn arguments_are_accessible() {
    let arguments = WorkItemArguments::from([("a", 1), ("b", 2), ("c", 3)]);
    let work_item = WorkItem::of::<StubActivity>(arguments);

    assert_eq!(work_item.arguments().get_i64("a").unwrap(), 1);
    assert_eq!(work_item.arguments().get_i64("b").unwrap(), 2);
    assert_eq!(work_item.arguments().get_i64("c").unwrap(), 3);
}
