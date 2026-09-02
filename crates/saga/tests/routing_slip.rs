//! Tests for `RoutingSlip`.

use std::sync::Mutex;

use ascetic_ddd_saga::{
    Activity, ActivityType, Result, RoutingSlip, SagaError, WorkItem, WorkItemArguments, WorkLog,
    WorkResult, async_trait,
};
use futures::executor::block_on;

mod common;

use common::{Counter, acquire};

static LOCK: Mutex<()> = Mutex::new(());
static CALL_COUNT: Counter = Counter::new();
static COMPENSATE_COUNT: Counter = Counter::new();

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = acquire(&LOCK);
    CALL_COUNT.reset();
    COMPENSATE_COUNT.reset();
    guard
}

/// Activity that always succeeds.
#[derive(Debug, Default)]
struct SuccessActivity;

#[async_trait]
impl Activity for SuccessActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        CALL_COUNT.increment();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("id", CALL_COUNT.get() as i64)]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        COMPENSATE_COUNT.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./success"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./successCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

/// Activity that always fails.
#[derive(Debug, Default)]
struct FailingActivity;

#[async_trait]
impl Activity for FailingActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        Err(SagaError::activity("Intentional failure"))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./failing"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./failingCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

fn success_item() -> WorkItem {
    WorkItem::of::<SuccessActivity>(WorkItemArguments::new())
}

fn failing_item() -> WorkItem {
    WorkItem::of::<FailingActivity>(WorkItemArguments::new())
}

#[test]
fn create_empty() {
    let slip = RoutingSlip::default();

    assert!(slip.is_completed());
    assert!(!slip.is_in_progress());
}

#[test]
fn create_with_work_items() {
    let slip = RoutingSlip::new([
        WorkItem::of::<SuccessActivity>(WorkItemArguments::from([("a", 1)])),
        WorkItem::of::<SuccessActivity>(WorkItemArguments::from([("b", 2)])),
    ]);

    assert!(!slip.is_completed());
    assert!(!slip.is_in_progress());
}

#[test]
fn process_next_success() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([success_item()]);

        assert!(slip.process_next().await.unwrap());
        assert!(slip.is_completed());
        assert!(slip.is_in_progress());
    });
}

#[test]
fn process_next_failure() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([failing_item()]);

        assert!(!slip.process_next().await.unwrap());
        assert!(slip.is_completed());
        assert!(!slip.is_in_progress());
    });
}

#[test]
fn process_next_on_empty_is_invalid() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::default();

        assert!(matches!(
            slip.process_next().await,
            Err(SagaError::InvalidOperation(_)),
        ));
    });
}

#[test]
fn process_multiple_items() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([success_item(), success_item(), success_item()]);

        slip.process_next().await.unwrap();
        assert!(!slip.is_completed());
        assert_eq!(slip.completed_work_logs().len(), 1);

        slip.process_next().await.unwrap();
        assert!(!slip.is_completed());
        assert_eq!(slip.completed_work_logs().len(), 2);

        slip.process_next().await.unwrap();
        assert!(slip.is_completed());
        assert_eq!(slip.completed_work_logs().len(), 3);
    });
}

#[test]
fn undo_last_success() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([success_item()]);
        slip.process_next().await.unwrap();

        assert!(slip.undo_last().await.unwrap());
        assert!(!slip.is_in_progress());
        assert_eq!(COMPENSATE_COUNT.get(), 1);
    });
}

#[test]
fn undo_last_on_empty_is_invalid() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([success_item()]);

        assert!(matches!(
            slip.undo_last().await,
            Err(SagaError::InvalidOperation(_)),
        ));
    });
}

#[test]
fn undo_multiple_items() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([success_item(), success_item(), success_item()]);
        slip.process_next().await.unwrap();
        slip.process_next().await.unwrap();
        slip.process_next().await.unwrap();

        assert_eq!(slip.completed_work_logs().len(), 3);

        slip.undo_last().await.unwrap();
        assert_eq!(slip.completed_work_logs().len(), 2);

        slip.undo_last().await.unwrap();
        assert_eq!(slip.completed_work_logs().len(), 1);

        slip.undo_last().await.unwrap();
        assert_eq!(slip.completed_work_logs().len(), 0);
        assert!(!slip.is_in_progress());
    });
}

#[test]
fn progress_uri_returns_next_activity_queue() {
    let slip = RoutingSlip::new([success_item()]);

    assert_eq!(slip.progress_uri().as_deref(), Some("sb://./success"));
}

#[test]
fn progress_uri_is_none_when_completed() {
    let slip = RoutingSlip::default();

    assert_eq!(slip.progress_uri(), None);
}

#[test]
fn compensation_uri_returns_last_activity_queue() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([success_item()]);
        slip.process_next().await.unwrap();

        assert_eq!(
            slip.compensation_uri().as_deref(),
            Some("sb://./successCompensation"),
        );
    });
}

#[test]
fn compensation_uri_is_none_when_not_started() {
    let slip = RoutingSlip::new([success_item()]);

    assert_eq!(slip.compensation_uri(), None);
}

#[test]
fn successful_saga() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([success_item(), success_item(), success_item()]);

        while !slip.is_completed() {
            slip.process_next().await.unwrap();
        }

        assert!(slip.is_completed());
        assert!(slip.is_in_progress());
        assert_eq!(slip.completed_work_logs().len(), 3);
    });
}

#[test]
fn failed_saga_with_compensation() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([success_item(), success_item(), failing_item()]);

        // Process until failure.
        while !slip.is_completed() {
            if !slip.process_next().await.unwrap() {
                break;
            }
        }

        // Compensate.
        while slip.is_in_progress() {
            slip.undo_last().await.unwrap();
        }

        assert!(!slip.is_in_progress());
        assert_eq!(COMPENSATE_COUNT.get(), 2);
    });
}
