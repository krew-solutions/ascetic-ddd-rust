//! Tests for the `Activity` trait.
//!
//! The Python suite also asserts that `Activity` cannot be instantiated and
//! that a subclass missing `do_work`, `compensate`,
//! `work_item_queue_address` or `compensation_queue_address` raises
//! `TypeError`. Rust enforces all of that at compile time: a trait has no
//! constructor, and an `impl` missing a required method does not build. Those
//! five tests therefore have no runtime counterpart.

use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

use ascetic_ddd_saga::{
    Activity, ActivityType, Result, RoutingSlip, WorkItem, WorkItemArguments, WorkLog, WorkResult,
    async_trait,
};
use futures::executor::block_on;

mod common;

use common::acquire;

static LOCK: Mutex<()> = Mutex::new(());
/// Address of the work item last passed to `do_work()`.
static RECEIVED_WORK_ITEM: AtomicUsize = AtomicUsize::new(0);
/// Address of the work log last passed to `compensate()`.
static RECEIVED_WORK_LOG: AtomicUsize = AtomicUsize::new(0);
/// Address of the routing slip last passed to `compensate()`.
static RECEIVED_ROUTING_SLIP: AtomicUsize = AtomicUsize::new(0);

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = acquire(&LOCK);
    RECEIVED_WORK_ITEM.store(0, Ordering::SeqCst);
    RECEIVED_WORK_LOG.store(0, Ordering::SeqCst);
    RECEIVED_ROUTING_SLIP.store(0, Ordering::SeqCst);
    guard
}

fn address<T>(reference: &T) -> usize {
    reference as *const T as usize
}

/// Activity recording what it is called with.
#[derive(Debug, Default)]
struct RecordingActivity;

#[async_trait]
impl Activity for RecordingActivity {
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        RECEIVED_WORK_ITEM.store(address(work_item), Ordering::SeqCst);
        Ok(Some(WorkLog::new(self, WorkResult::from([("id", 123)]))))
    }

    async fn compensate(&self, work_log: &WorkLog, routing_slip: &mut RoutingSlip) -> Result<bool> {
        RECEIVED_WORK_LOG.store(address(work_log), Ordering::SeqCst);
        RECEIVED_ROUTING_SLIP.store(address(routing_slip), Ordering::SeqCst);
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./test"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./testCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

#[test]
fn complete_implementation_can_be_instantiated() {
    let activity = RecordingActivity;

    let activity: &dyn Activity = &activity;

    assert_eq!(activity.work_item_queue_address(), "sb://./test");
    assert_eq!(
        activity.compensation_queue_address(),
        "sb://./testCompensation",
    );
}

/// An activity that does not report a canonical name is the counterpart of a
/// Python activity that does not implement the `NamedActivity` protocol.
#[test]
fn type_name_is_optional() {
    assert_eq!(RecordingActivity.type_name(), None);
}

#[test]
fn do_work_receives_work_item() {
    let _guard = setup();
    block_on(async {
        let activity = RecordingActivity;
        let work_item =
            WorkItem::of::<RecordingActivity>(WorkItemArguments::from([("key", "value")]));

        activity.do_work(&work_item).await.unwrap();

        assert_eq!(
            RECEIVED_WORK_ITEM.load(Ordering::SeqCst),
            address(&work_item)
        );
        assert_eq!(work_item.arguments().get_str("key").unwrap(), "value");
    });
}

#[test]
fn compensate_receives_work_log_and_routing_slip() {
    let _guard = setup();
    block_on(async {
        let activity = RecordingActivity;
        let work_item = WorkItem::of::<RecordingActivity>(WorkItemArguments::new());
        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();
        let mut routing_slip = RoutingSlip::default();

        activity
            .compensate(&work_log, &mut routing_slip)
            .await
            .unwrap();

        assert_eq!(RECEIVED_WORK_LOG.load(Ordering::SeqCst), address(&work_log));
        assert_eq!(
            RECEIVED_ROUTING_SLIP.load(Ordering::SeqCst),
            address(&routing_slip),
        );
    });
}
