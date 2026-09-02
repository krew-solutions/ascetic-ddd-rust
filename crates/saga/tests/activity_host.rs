//! Tests for `ActivityHost`.

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use ascetic_ddd_saga::{
    Activity, ActivityHost, ActivityType, FnSender, MessageSender, Result, RoutingSlip, SagaError,
    WorkItem, WorkItemArguments, WorkLog, WorkResult, async_trait,
};
use futures::executor::block_on;

mod common;

use common::{Counter, acquire};

static LOCK: Mutex<()> = Mutex::new(());
static ACTIVITY1_CALLS: Counter = Counter::new();
static ACTIVITY1_COMPENSATIONS: Counter = Counter::new();
static ACTIVITY2_CALLS: Counter = Counter::new();
static ACTIVITY2_COMPENSATIONS: Counter = Counter::new();

fn setup() -> std::sync::MutexGuard<'static, ()> {
    let guard = acquire(&LOCK);
    ACTIVITY1_CALLS.reset();
    ACTIVITY1_COMPENSATIONS.reset();
    ACTIVITY2_CALLS.reset();
    ACTIVITY2_COMPENSATIONS.reset();
    guard
}

/// Collects the URIs a host sends to, standing in for a message bus.
fn recorder() -> (Arc<dyn MessageSender>, Arc<Mutex<Vec<String>>>) {
    let messages: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&messages);
    let sender: Arc<dyn MessageSender> =
        Arc::new(FnSender::new(move |uri: &str, _slip: &RoutingSlip| {
            sink.lock().unwrap().push(uri.to_owned());
            Ok(())
        }));
    (sender, messages)
}

/// First test activity.
#[derive(Debug, Default)]
struct Activity1;

#[async_trait]
impl Activity for Activity1 {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        ACTIVITY1_CALLS.increment();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("id", ACTIVITY1_CALLS.get() as i64)]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        ACTIVITY1_COMPENSATIONS.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./activity1"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./activity1Compensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

/// Second test activity.
#[derive(Debug, Default)]
struct Activity2;

#[async_trait]
impl Activity for Activity2 {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        ACTIVITY2_CALLS.increment();
        Ok(Some(WorkLog::new(
            self,
            WorkResult::from([("id", ACTIVITY2_CALLS.get() as i64)]),
        )))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        ACTIVITY2_COMPENSATIONS.increment();
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./activity2"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./activity2Compensation"
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

fn item<A: Activity + Default>() -> WorkItem {
    WorkItem::of::<A>(WorkItemArguments::new())
}

/// Runs every message until the bus is drained, dispatching it to the first
/// host that accepts it.
async fn run_bus(hosts: &[ActivityHost], messages: &Mutex<Vec<String>>, slip: &mut RoutingSlip) {
    let mut queue: VecDeque<String> = messages.lock().unwrap().drain(..).collect();

    while let Some(uri) = queue.pop_front() {
        for host in hosts {
            if host.accept_message(&uri, slip).await.unwrap() {
                break;
            }
        }
        queue.extend(messages.lock().unwrap().drain(..));
    }
}

#[test]
fn accept_work_item_message() {
    let _guard = setup();
    block_on(async {
        let (sender, _messages) = recorder();
        let host = ActivityHost::of::<Activity1>(sender);
        let mut slip = RoutingSlip::new([item::<Activity1>()]);

        assert!(
            host.accept_message("sb://./activity1", &mut slip)
                .await
                .unwrap()
        );
    });
}

#[test]
fn accept_compensation_message() {
    let _guard = setup();
    block_on(async {
        let (sender, _messages) = recorder();
        let host = ActivityHost::of::<Activity1>(sender);
        let mut slip = RoutingSlip::new([item::<Activity1>()]);
        slip.process_next().await.unwrap();

        assert!(
            host.accept_message("sb://./activity1Compensation", &mut slip)
                .await
                .unwrap()
        );
    });
}

#[test]
fn reject_unknown_message() {
    let _guard = setup();
    block_on(async {
        let (sender, _messages) = recorder();
        let host = ActivityHost::of::<Activity1>(sender);
        let mut slip = RoutingSlip::new([item::<Activity1>()]);

        assert!(
            !host
                .accept_message("sb://./unknown", &mut slip)
                .await
                .unwrap()
        );
    });
}

#[test]
fn reject_other_activity_message() {
    let _guard = setup();
    block_on(async {
        let (sender, _messages) = recorder();
        let host = ActivityHost::of::<Activity1>(sender);
        let mut slip = RoutingSlip::new([item::<Activity2>()]);

        assert!(
            !host
                .accept_message("sb://./activity2", &mut slip)
                .await
                .unwrap()
        );
    });
}

#[test]
fn forward_success_continues_forward() {
    let _guard = setup();
    block_on(async {
        let (sender, messages) = recorder();
        let host = ActivityHost::of::<Activity1>(sender);
        let mut slip = RoutingSlip::new([item::<Activity1>(), item::<Activity2>()]);

        host.process_forward_message(&mut slip).await.unwrap();

        assert_eq!(*messages.lock().unwrap(), ["sb://./activity2"]);
    });
}

#[test]
fn forward_failure_starts_compensation() {
    let _guard = setup();
    block_on(async {
        let (sender, messages) = recorder();
        let host = ActivityHost::of::<FailingActivity>(sender);
        let mut slip = RoutingSlip::new([item::<Activity1>(), item::<FailingActivity>()]);
        slip.process_next().await.unwrap(); // Complete Activity1.

        host.process_forward_message(&mut slip).await.unwrap();

        assert_eq!(*messages.lock().unwrap(), ["sb://./activity1Compensation"]);
    });
}

#[test]
fn forward_completed_does_nothing() {
    let _guard = setup();
    block_on(async {
        let (sender, messages) = recorder();
        let host = ActivityHost::of::<Activity1>(sender);
        let mut slip = RoutingSlip::default();

        host.process_forward_message(&mut slip).await.unwrap();

        assert!(messages.lock().unwrap().is_empty());
    });
}

#[test]
fn backward_continues_backward() {
    let _guard = setup();
    block_on(async {
        let (sender, messages) = recorder();
        let host = ActivityHost::of::<Activity2>(sender);
        let mut slip = RoutingSlip::new([item::<Activity1>(), item::<Activity2>()]);
        slip.process_next().await.unwrap();
        slip.process_next().await.unwrap();

        host.process_backward_message(&mut slip).await.unwrap();

        assert_eq!(*messages.lock().unwrap(), ["sb://./activity1Compensation"]);
    });
}

#[test]
fn backward_not_in_progress_does_nothing() {
    let _guard = setup();
    block_on(async {
        let (sender, messages) = recorder();
        let host = ActivityHost::of::<Activity1>(sender);
        let mut slip = RoutingSlip::new([item::<Activity1>()]);

        host.process_backward_message(&mut slip).await.unwrap();

        assert!(messages.lock().unwrap().is_empty());
    });
}

#[test]
fn distributed_saga_success() {
    let _guard = setup();
    block_on(async {
        let (sender, messages) = recorder();
        let hosts = [
            ActivityHost::of::<Activity1>(Arc::clone(&sender)),
            ActivityHost::of::<Activity2>(Arc::clone(&sender)),
        ];
        let mut slip = RoutingSlip::new([item::<Activity1>(), item::<Activity2>()]);

        // Start the saga.
        sender
            .send(&slip.progress_uri().unwrap(), &slip)
            .await
            .unwrap();

        run_bus(&hosts, &messages, &mut slip).await;

        assert!(slip.is_completed());
        assert_eq!(ACTIVITY1_CALLS.get(), 1);
        assert_eq!(ACTIVITY2_CALLS.get(), 1);
    });
}

#[test]
fn distributed_saga_with_compensation() {
    let _guard = setup();
    block_on(async {
        let (sender, messages) = recorder();
        let hosts = [
            ActivityHost::of::<Activity1>(Arc::clone(&sender)),
            ActivityHost::of::<Activity2>(Arc::clone(&sender)),
            ActivityHost::of::<FailingActivity>(Arc::clone(&sender)),
        ];
        let mut slip = RoutingSlip::new([
            item::<Activity1>(),
            item::<Activity2>(),
            item::<FailingActivity>(),
        ]);

        // Start the saga.
        sender
            .send(&slip.progress_uri().unwrap(), &slip)
            .await
            .unwrap();

        run_bus(&hosts, &messages, &mut slip).await;

        assert!(!slip.is_in_progress());
        assert_eq!(ACTIVITY1_CALLS.get(), 1);
        assert_eq!(ACTIVITY2_CALLS.get(), 1);
        assert_eq!(ACTIVITY1_COMPENSATIONS.get(), 1);
        assert_eq!(ACTIVITY2_COMPENSATIONS.get(), 1);
    });
}
