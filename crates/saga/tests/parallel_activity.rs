//! Tests for `ParallelActivity`.

use std::sync::Mutex;

use ascetic_ddd_saga::{
    Activity, ActivityType, ParallelActivity, Result, RoutingSlip, SharedRoutingSlip, WorkItem,
    WorkItemArguments, WorkLog, WorkResult, async_trait,
};
use futures::executor::block_on;

mod common;

use common::{Counter, acquire};

static LOCK: Mutex<()> = Mutex::new(());
static BRANCH_A_CALLS: Counter = Counter::new();
static BRANCH_A_COMPENSATIONS: Counter = Counter::new();
static BRANCH_B_CALLS: Counter = Counter::new();
static BRANCH_B_COMPENSATIONS: Counter = Counter::new();
static FAILING_CALLS: Counter = Counter::new();

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = acquire(&LOCK);
    for counter in [
        &BRANCH_A_CALLS,
        &BRANCH_A_COMPENSATIONS,
        &BRANCH_B_CALLS,
        &BRANCH_B_COMPENSATIONS,
        &FAILING_CALLS,
    ] {
        counter.reset();
    }
    guard
}

/// Test activity for branch A.
#[derive(Debug, Default)]
struct BranchAActivity;

#[async_trait]
impl Activity for BranchAActivity {
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        BRANCH_A_CALLS.increment();
        let value = work_item
            .arguments()
            .get_str("value")
            .unwrap_or("default")
            .to_owned();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("branch", "A".to_owned()), ("value", value)]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        BRANCH_A_COMPENSATIONS.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./branchA"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./branchACompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

/// Test activity for branch B.
#[derive(Debug, Default)]
struct BranchBActivity;

#[async_trait]
impl Activity for BranchBActivity {
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
        BRANCH_B_CALLS.increment();
        let value = work_item
            .arguments()
            .get_str("value")
            .unwrap_or("default")
            .to_owned();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("branch", "B".to_owned()), ("value", value)]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        BRANCH_B_COMPENSATIONS.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./branchB"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./branchBCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

/// Test activity that always fails.
#[derive(Debug, Default)]
struct FailingBranchActivity;

#[async_trait]
impl Activity for FailingBranchActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        FAILING_CALLS.increment();
        Ok(None)
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

fn valued<A: Activity + Default>(value: &str) -> WorkItem {
    WorkItem::of::<A>(WorkItemArguments::from([("value", value)]))
}

fn branch<A: Activity + Default>(value: &str) -> SharedRoutingSlip {
    RoutingSlip::new([valued::<A>(value)]).into_shared()
}

#[test]
fn all_branches_succeed() {
    let _guard = setup();
    block_on(async {
        let activity = ParallelActivity;
        let work_item = WorkItem::of::<ParallelActivity>(ParallelActivity::arguments([
            branch::<BranchAActivity>("a1"),
            branch::<BranchBActivity>("b1"),
        ]));

        let result = activity.do_work(&work_item).await.unwrap();

        assert!(result.is_some());
        assert_eq!(BRANCH_A_CALLS.get(), 1);
        assert_eq!(BRANCH_B_CALLS.get(), 1);
    });
}

#[test]
fn multi_step_branches_succeed() {
    let _guard = setup();
    block_on(async {
        let activity = ParallelActivity;
        let work_item = WorkItem::of::<ParallelActivity>(ParallelActivity::arguments([
            RoutingSlip::new([
                valued::<BranchAActivity>("a1"),
                valued::<BranchAActivity>("a2"),
            ])
            .into_shared(),
            branch::<BranchBActivity>("b1"),
        ]));

        let result = activity.do_work(&work_item).await.unwrap();

        assert!(result.is_some());
        assert_eq!(BRANCH_A_CALLS.get(), 2); // Two steps in the first branch.
        assert_eq!(BRANCH_B_CALLS.get(), 1);
    });
}

#[test]
fn one_branch_fails_compensates_all() {
    let _guard = setup();
    block_on(async {
        let activity = ParallelActivity;
        let work_item = WorkItem::of::<ParallelActivity>(ParallelActivity::arguments([
            RoutingSlip::new([
                valued::<BranchAActivity>("a1"),
                WorkItem::of::<FailingBranchActivity>(WorkItemArguments::new()),
            ])
            .into_shared(),
            branch::<BranchBActivity>("b1"),
        ]));

        let result = activity.do_work(&work_item).await.unwrap();

        assert!(result.is_none());
        // Branch A was completed before the failure, so it is compensated.
        assert_eq!(BRANCH_A_CALLS.get(), 1);
        assert_eq!(BRANCH_A_COMPENSATIONS.get(), 1);
    });
}

#[test]
fn compensate_all_branches() {
    let _guard = setup();
    block_on(async {
        let activity = ParallelActivity;
        let work_item = WorkItem::of::<ParallelActivity>(ParallelActivity::arguments([
            RoutingSlip::new([
                valued::<BranchAActivity>("a"),
                valued::<BranchAActivity>("a2"),
            ])
            .into_shared(),
            branch::<BranchBActivity>("b"),
        ]));

        // First execute.
        let work_log = activity.do_work(&work_item).await.unwrap().unwrap();
        assert_eq!(BRANCH_A_CALLS.get(), 2);
        assert_eq!(BRANCH_B_CALLS.get(), 1);

        // Then compensate.
        let compensated = activity
            .compensate(&work_log, &mut RoutingSlip::default())
            .await
            .unwrap();

        assert!(compensated);
        assert_eq!(BRANCH_A_COMPENSATIONS.get(), 2); // Both steps compensated.
        assert_eq!(BRANCH_B_COMPENSATIONS.get(), 1);
    });
}

#[test]
fn work_item_queue_address() {
    assert_eq!(
        ParallelActivity.work_item_queue_address(),
        "sb://./parallel"
    );
}

#[test]
fn compensation_queue_address() {
    assert_eq!(
        ParallelActivity.compensation_queue_address(),
        "sb://./parallelCompensation",
    );
}

#[test]
fn parallel_step_in_routing_slip() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([
            valued::<BranchAActivity>("before"),
            WorkItem::of::<ParallelActivity>(ParallelActivity::arguments([
                RoutingSlip::new([
                    valued::<BranchAActivity>("p1"),
                    valued::<BranchAActivity>("p2"),
                ])
                .into_shared(),
                branch::<BranchBActivity>("p3"),
            ])),
            valued::<BranchBActivity>("after"),
        ]);

        while !slip.is_completed() {
            assert!(slip.process_next().await.unwrap());
        }

        assert!(slip.is_completed());
        // BranchA: 1 (before) + 2 (parallel branch) = 3.
        assert_eq!(BRANCH_A_CALLS.get(), 3);
        // BranchB: 1 (parallel branch) + 1 (after) = 2.
        assert_eq!(BRANCH_B_CALLS.get(), 2);
    });
}

#[test]
fn parallel_failure_triggers_saga_compensation() {
    let _guard = setup();
    block_on(async {
        let mut slip = RoutingSlip::new([
            valued::<BranchAActivity>("first"),
            WorkItem::of::<ParallelActivity>(ParallelActivity::arguments([
                branch::<BranchBActivity>("ok"),
                RoutingSlip::new([WorkItem::of::<FailingBranchActivity>(
                    WorkItemArguments::new(),
                )])
                .into_shared(),
            ])),
        ]);

        // First step succeeds.
        assert!(slip.process_next().await.unwrap());
        assert_eq!(BRANCH_A_CALLS.get(), 1);

        // Second step (parallel) fails.
        assert!(!slip.process_next().await.unwrap());

        // Compensate the first step.
        while slip.is_in_progress() {
            slip.undo_last().await.unwrap();
        }

        assert_eq!(BRANCH_A_COMPENSATIONS.get(), 1);
    });
}
