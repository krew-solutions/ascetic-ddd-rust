//! Tests for `FallbackActivity`.

use std::sync::Mutex;

use ascetic_ddd_saga::{
    Activity, ActivityType, FallbackActivity, Result, RoutingSlip, SharedRoutingSlip, WorkItem,
    WorkItemArguments, WorkLog, WorkResult, async_trait,
};
use futures::executor::block_on;

mod common;

use common::{Counter, Flag, acquire};

static LOCK: Mutex<()> = Mutex::new(());
static PRIMARY_CALLS: Counter = Counter::new();
static PRIMARY_COMPENSATIONS: Counter = Counter::new();
static PRIMARY_SHOULD_FAIL: Flag = Flag::new();
static BACKUP_CALLS: Counter = Counter::new();
static BACKUP_COMPENSATIONS: Counter = Counter::new();
static BACKUP_SHOULD_FAIL: Flag = Flag::new();
static THIRD_CALLS: Counter = Counter::new();
static THIRD_COMPENSATIONS: Counter = Counter::new();
static CONFIRM_CALLS: Counter = Counter::new();
static CONFIRM_COMPENSATIONS: Counter = Counter::new();

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = acquire(&LOCK);
    for counter in [
        &PRIMARY_CALLS,
        &PRIMARY_COMPENSATIONS,
        &BACKUP_CALLS,
        &BACKUP_COMPENSATIONS,
        &THIRD_CALLS,
        &THIRD_COMPENSATIONS,
        &CONFIRM_CALLS,
        &CONFIRM_COMPENSATIONS,
    ] {
        counter.reset();
    }
    PRIMARY_SHOULD_FAIL.set(false);
    BACKUP_SHOULD_FAIL.set(false);
    guard
}

/// Primary test activity.
#[derive(Debug, Default)]
struct PrimaryActivity;

#[async_trait]
impl Activity for PrimaryActivity {
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        PRIMARY_CALLS.increment();
        if PRIMARY_SHOULD_FAIL.get() {
            return Ok(None);
        }
        let value = work_item
            .arguments()
            .get_str("value")
            .unwrap_or("default")
            .to_owned();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("provider", "primary".to_owned()), ("value", value)]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        PRIMARY_COMPENSATIONS.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./primary"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./primaryCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

/// Backup test activity.
#[derive(Debug, Default)]
struct BackupActivity;

#[async_trait]
impl Activity for BackupActivity {
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        BACKUP_CALLS.increment();
        if BACKUP_SHOULD_FAIL.get() {
            return Ok(None);
        }
        let value = work_item
            .arguments()
            .get_str("value")
            .unwrap_or("default")
            .to_owned();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("provider", "backup".to_owned()), ("value", value)]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        BACKUP_COMPENSATIONS.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./backup"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./backupCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

/// Third fallback option.
#[derive(Debug, Default)]
struct ThirdActivity;

#[async_trait]
impl Activity for ThirdActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        THIRD_CALLS.increment();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("provider", "third")]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        THIRD_COMPENSATIONS.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./third"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./thirdCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

/// Confirmation step activity.
#[derive(Debug, Default)]
struct ConfirmActivity;

#[async_trait]
impl Activity for ConfirmActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        CONFIRM_CALLS.increment();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("confirmed", true)]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        CONFIRM_COMPENSATIONS.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./confirm"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./confirmCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

fn alternative<A: Activity + Default>(value: &str) -> SharedRoutingSlip {
    RoutingSlip::new([WorkItem::of::<A>(WorkItemArguments::from([(
        "value", value,
    )]))])
    .into_shared()
}

fn empty_alternative<A: Activity + Default>() -> SharedRoutingSlip {
    RoutingSlip::new([WorkItem::of::<A>(WorkItemArguments::new())]).into_shared()
}

#[test]
fn primary_succeeds() {
    let _guard = setup();
    block_on(async {
        let activity = FallbackActivity;
        let work_item = WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
            alternative::<PrimaryActivity>("test"),
            alternative::<BackupActivity>("test"),
        ]));

        let result = activity.do_work(&work_item).await.unwrap();

        assert!(result.is_some());
        assert_eq!(PRIMARY_CALLS.get(), 1);
        assert_eq!(BACKUP_CALLS.get(), 0);
    });
}

#[test]
fn primary_fails_backup_succeeds() {
    let _guard = setup();
    PRIMARY_SHOULD_FAIL.set(true);
    block_on(async {
        let activity = FallbackActivity;
        let work_item = WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
            alternative::<PrimaryActivity>("test"),
            alternative::<BackupActivity>("test"),
        ]));

        let result = activity.do_work(&work_item).await.unwrap();

        assert!(result.is_some());
        assert_eq!(PRIMARY_CALLS.get(), 1);
        assert_eq!(BACKUP_CALLS.get(), 1);
    });
}

#[test]
fn multi_step_alternative() {
    let _guard = setup();
    block_on(async {
        let activity = FallbackActivity;
        let work_item =
            WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([RoutingSlip::new([
                WorkItem::of::<PrimaryActivity>(WorkItemArguments::from([("value", "step1")])),
                WorkItem::of::<ConfirmActivity>(WorkItemArguments::new()),
            ])
            .into_shared()]));

        let result = activity.do_work(&work_item).await.unwrap();

        assert!(result.is_some());
        assert_eq!(PRIMARY_CALLS.get(), 1);
        assert_eq!(CONFIRM_CALLS.get(), 1);
    });
}

#[test]
fn all_alternatives_fail() {
    let _guard = setup();
    PRIMARY_SHOULD_FAIL.set(true);
    BACKUP_SHOULD_FAIL.set(true);
    block_on(async {
        let activity = FallbackActivity;
        let work_item = WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
            alternative::<PrimaryActivity>("test"),
            alternative::<BackupActivity>("test"),
        ]));

        let result = activity.do_work(&work_item).await.unwrap();

        assert!(result.is_none());
        assert_eq!(PRIMARY_CALLS.get(), 1);
        assert_eq!(BACKUP_CALLS.get(), 1);
    });
}

#[test]
fn third_alternative_succeeds() {
    let _guard = setup();
    PRIMARY_SHOULD_FAIL.set(true);
    BACKUP_SHOULD_FAIL.set(true);
    block_on(async {
        let activity = FallbackActivity;
        let work_item = WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
            empty_alternative::<PrimaryActivity>(),
            empty_alternative::<BackupActivity>(),
            empty_alternative::<ThirdActivity>(),
        ]));

        let result = activity.do_work(&work_item).await.unwrap();

        assert!(result.is_some());
        assert_eq!(PRIMARY_CALLS.get(), 1);
        assert_eq!(BACKUP_CALLS.get(), 1);
        assert_eq!(THIRD_CALLS.get(), 1);
    });
}

#[test]
fn compensate_primary() {
    let _guard = setup();
    block_on(async {
        let activity = FallbackActivity;
        let work_item = WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
            alternative::<PrimaryActivity>("test"),
            alternative::<BackupActivity>("test"),
        ]));

        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();
        let compensated = activity
            .compensate(&work_log, &mut RoutingSlip::default())
            .await
            .unwrap();

        assert!(compensated);
        assert_eq!(PRIMARY_COMPENSATIONS.get(), 1);
        assert_eq!(BACKUP_COMPENSATIONS.get(), 0);
    });
}

#[test]
fn compensate_backup() {
    let _guard = setup();
    PRIMARY_SHOULD_FAIL.set(true);
    block_on(async {
        let activity = FallbackActivity;
        let work_item = WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
            alternative::<PrimaryActivity>("test"),
            alternative::<BackupActivity>("test"),
        ]));

        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();
        let compensated = activity
            .compensate(&work_log, &mut RoutingSlip::default())
            .await
            .unwrap();

        assert!(compensated);
        assert_eq!(PRIMARY_COMPENSATIONS.get(), 0);
        assert_eq!(BACKUP_COMPENSATIONS.get(), 1);
    });
}

#[test]
fn compensate_multi_step_alternative() {
    let _guard = setup();
    block_on(async {
        let activity = FallbackActivity;
        let work_item =
            WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([RoutingSlip::new([
                WorkItem::of::<PrimaryActivity>(WorkItemArguments::new()),
                WorkItem::of::<ConfirmActivity>(WorkItemArguments::new()),
            ])
            .into_shared()]));

        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();
        assert_eq!(PRIMARY_CALLS.get(), 1);
        assert_eq!(CONFIRM_CALLS.get(), 1);

        let compensated = activity
            .compensate(&work_log, &mut RoutingSlip::default())
            .await
            .unwrap();

        assert!(compensated);
        assert_eq!(PRIMARY_COMPENSATIONS.get(), 1);
        assert_eq!(CONFIRM_COMPENSATIONS.get(), 1);
    });
}

#[test]
fn work_item_queue_address() {
    assert_eq!(
        FallbackActivity.work_item_queue_address(),
        "sb://./fallback"
    );
}

#[test]
fn compensation_queue_address() {
    assert_eq!(
        FallbackActivity.compensation_queue_address(),
        "sb://./fallbackCompensation",
    );
}

#[test]
fn fallback_step_in_routing_slip() {
    let _guard = setup();
    PRIMARY_SHOULD_FAIL.set(true);
    block_on(async {
        let mut slip = RoutingSlip::new([
            WorkItem::of::<ThirdActivity>(WorkItemArguments::new()),
            WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
                alternative::<PrimaryActivity>("try1"),
                alternative::<BackupActivity>("try2"),
            ])),
            WorkItem::of::<ThirdActivity>(WorkItemArguments::new()),
        ]);

        while !slip.is_completed() {
            assert!(slip.process_next().await.unwrap());
        }

        assert!(slip.is_completed());
        assert_eq!(THIRD_CALLS.get(), 2);
        assert_eq!(PRIMARY_CALLS.get(), 1); // Tried and failed.
        assert_eq!(BACKUP_CALLS.get(), 1); // Succeeded.
    });
}

#[test]
fn all_fallbacks_fail_triggers_compensation() {
    let _guard = setup();
    PRIMARY_SHOULD_FAIL.set(true);
    BACKUP_SHOULD_FAIL.set(true);
    block_on(async {
        let mut slip = RoutingSlip::new([
            WorkItem::of::<ThirdActivity>(WorkItemArguments::new()),
            WorkItem::of::<FallbackActivity>(FallbackActivity::arguments([
                empty_alternative::<PrimaryActivity>(),
                empty_alternative::<BackupActivity>(),
            ])),
        ]);

        // First step succeeds.
        assert!(slip.process_next().await.unwrap());
        // Second step (fallback) fails.
        assert!(!slip.process_next().await.unwrap());

        // Compensate the first step.
        while slip.is_in_progress() {
            slip.undo_last().await.unwrap();
        }

        assert_eq!(THIRD_COMPENSATIONS.get(), 1);
    });
}
