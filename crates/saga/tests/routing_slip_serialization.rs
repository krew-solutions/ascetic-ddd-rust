//! Tests for `RoutingSlip` <-> `SerializableRoutingSlip` conversion.

use std::sync::Mutex;

use ascetic_ddd_saga::{
    Activity, ActivityType, MapBasedResolver, Result, RoutingSlip, SagaError,
    SerializableRoutingSlip, SerializableWorkItem, SerializableWorkLog, WorkItem,
    WorkItemArguments, WorkLog, WorkResult, async_trait, from_serializable, to_serializable,
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

/// Activity that always succeeds and reports its type name.
#[derive(Debug, Default)]
struct SerializableSuccessActivity;

#[async_trait]
impl Activity for SerializableSuccessActivity {
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

    fn type_name(&self) -> Option<&str> {
        Some("SerializableSuccessActivity")
    }
}

/// Activity that reports no canonical name.
#[derive(Debug, Default)]
struct AnonymousActivity;

#[async_trait]
impl Activity for AnonymousActivity {
    async fn do_work(&self, _work_item: &WorkItem) -> Result<Option<WorkLog>> {
        Ok(Some(WorkLog::new(self, WorkResult::new())))
    }

    async fn compensate(
        &self,
        _work_log: &WorkLog,
        _routing_slip: &mut RoutingSlip,
    ) -> Result<bool> {
        Ok(true)
    }

    fn work_item_queue_address(&self) -> &str {
        "sb://./anon"
    }

    fn compensation_queue_address(&self) -> &str {
        "sb://./anonCompensation"
    }

    fn activity_type(&self) -> ActivityType {
        ActivityType::of::<Self>()
    }
}

fn registered_resolver() -> MapBasedResolver {
    let mut resolver = MapBasedResolver::new();
    resolver.register_type::<SerializableSuccessActivity>("SerializableSuccessActivity");
    resolver
}

fn success_item(arguments: WorkItemArguments) -> WorkItem {
    WorkItem::of::<SerializableSuccessActivity>(arguments)
}

/// Round-trips a slip through JSON, as a message bus would.
fn transmit(slip: &RoutingSlip, resolver: &MapBasedResolver) -> RoutingSlip {
    let wire = serde_json::to_string(&to_serializable(slip, resolver).unwrap()).unwrap();
    let serializable: SerializableRoutingSlip = serde_json::from_str(&wire).unwrap();
    from_serializable(&serializable, resolver).unwrap()
}

#[test]
fn to_serializable_empty_slip() {
    let resolver = MapBasedResolver::new();
    let slip = RoutingSlip::default();

    let serializable = to_serializable(&slip, &resolver).unwrap();

    assert!(serializable.completed_work_logs.is_empty());
    assert!(serializable.next_work_items.is_empty());
}

#[test]
fn to_serializable_pending_items_only() {
    let resolver = registered_resolver();
    let slip = RoutingSlip::new([
        success_item(WorkItemArguments::from([("a", 1)])),
        success_item(WorkItemArguments::from([("b", 2)])),
    ]);

    let serializable = to_serializable(&slip, &resolver).unwrap();

    assert_eq!(serializable.next_work_items.len(), 2);
    assert_eq!(
        serializable.next_work_items[0].activity_type_name,
        "SerializableSuccessActivity",
    );
    assert_eq!(
        serializable.next_work_items[0].arguments,
        WorkItemArguments::from([("a", 1)]),
    );
    assert_eq!(
        serializable.next_work_items[1].arguments,
        WorkItemArguments::from([("b", 2)]),
    );
}

#[test]
fn to_serializable_completed_work_logs() {
    let _guard = setup();
    block_on(async {
        let resolver = registered_resolver();
        let mut slip = RoutingSlip::new([success_item(WorkItemArguments::from([("x", "test")]))]);
        slip.process_next().await.unwrap();

        let serializable = to_serializable(&slip, &resolver).unwrap();

        assert_eq!(serializable.completed_work_logs.len(), 1);
        assert_eq!(
            serializable.completed_work_logs[0].activity_type_name,
            "SerializableSuccessActivity",
        );
        assert_eq!(
            serializable.completed_work_logs[0]
                .result
                .get_i64("id")
                .unwrap(),
            1,
        );
    });
}

#[test]
fn to_serializable_unregistered_anonymous_activity_is_reported() {
    let resolver = MapBasedResolver::new();
    let slip = RoutingSlip::new([WorkItem::of::<AnonymousActivity>(WorkItemArguments::new())]);

    assert!(matches!(
        to_serializable(&slip, &resolver),
        Err(SagaError::ActivityTypeNotRegistered(_)),
    ));
}

#[test]
fn to_serializable_unregistered_named_activity_falls_back() {
    let resolver = MapBasedResolver::new();
    let slip = RoutingSlip::new([success_item(WorkItemArguments::new())]);

    // Intentionally not registered.
    let serializable = to_serializable(&slip, &resolver).unwrap();

    assert_eq!(
        serializable.next_work_items[0].activity_type_name,
        "SerializableSuccessActivity",
    );
}

#[test]
fn from_serializable_empty() {
    let resolver = MapBasedResolver::new();
    let serializable = SerializableRoutingSlip::default();

    let slip = from_serializable(&serializable, &resolver).unwrap();

    assert!(slip.is_completed());
    assert!(!slip.is_in_progress());
}

#[test]
fn from_serializable_pending_items_are_restored() {
    let resolver = registered_resolver();
    let serializable = SerializableRoutingSlip::new(
        [],
        [
            SerializableWorkItem::new(
                "SerializableSuccessActivity",
                WorkItemArguments::from([("a", 1)]),
            ),
            SerializableWorkItem::new(
                "SerializableSuccessActivity",
                WorkItemArguments::from([("b", 2)]),
            ),
        ],
    );

    let slip = from_serializable(&serializable, &resolver).unwrap();

    assert!(!slip.is_completed());
    assert_eq!(slip.pending_work_items().len(), 2);
    assert_eq!(
        slip.pending_work_items()[0].arguments(),
        &WorkItemArguments::from([("a", 1)]),
    );
    assert_eq!(
        slip.pending_work_items()[1].arguments(),
        &WorkItemArguments::from([("b", 2)]),
    );
}

#[test]
fn from_serializable_completed_work_logs_are_restored() {
    let resolver = registered_resolver();
    let serializable = SerializableRoutingSlip::new(
        [SerializableWorkLog::new(
            "SerializableSuccessActivity",
            WorkResult::from([("id", 42)]),
        )],
        [],
    );

    let slip = from_serializable(&serializable, &resolver).unwrap();

    assert!(slip.is_in_progress());
    assert_eq!(slip.completed_work_logs().len(), 1);
    assert_eq!(
        slip.completed_work_logs()[0]
            .result()
            .get_i64("id")
            .unwrap(),
        42
    );
    assert_eq!(
        slip.completed_work_logs()[0].activity_type(),
        ActivityType::of::<SerializableSuccessActivity>(),
    );
}

#[test]
fn from_serializable_unregistered_activity_is_reported() {
    let resolver = MapBasedResolver::new();
    let serializable = SerializableRoutingSlip::new(
        [],
        [SerializableWorkItem::new(
            "UnregisteredActivity",
            WorkItemArguments::new(),
        )],
    );

    assert!(matches!(
        from_serializable(&serializable, &resolver),
        Err(SagaError::ActivityTypeNotRegistered(name)) if name == "UnregisteredActivity",
    ));
}

#[test]
fn round_trip_state_is_preserved() {
    let _guard = setup();
    block_on(async {
        let resolver = registered_resolver();
        let mut original = RoutingSlip::new([
            success_item(WorkItemArguments::from([("step", 1)])),
            success_item(WorkItemArguments::from([("step", 2)])),
            success_item(WorkItemArguments::from([("step", 3)])),
        ]);
        original.process_next().await.unwrap();

        let serializable = to_serializable(&original, &resolver).unwrap();
        let mut restored = from_serializable(&serializable, &resolver).unwrap();

        assert_eq!(restored.completed_work_logs().len(), 1);
        assert_eq!(restored.pending_work_items().len(), 2);

        // Continue processing the restored slip.
        restored.process_next().await.unwrap();
        restored.process_next().await.unwrap();
        assert!(restored.is_completed());
        assert_eq!(restored.completed_work_logs().len(), 3);
    });
}

#[test]
fn round_trip_through_json() {
    let _guard = setup();
    block_on(async {
        let resolver = registered_resolver();
        let mut original =
            RoutingSlip::new([success_item(WorkItemArguments::from([("key", "value")]))]);
        original.process_next().await.unwrap();

        let restored = transmit(&original, &resolver);

        assert_eq!(restored.completed_work_logs().len(), 1);
        assert_eq!(
            restored.completed_work_logs()[0]
                .result()
                .get_i64("id")
                .unwrap(),
            1,
        );
    });
}

#[test]
fn undo_last_works_after_round_trip() {
    let _guard = setup();
    block_on(async {
        let resolver = registered_resolver();
        let mut original = RoutingSlip::new([
            success_item(WorkItemArguments::from([("step", 1)])),
            success_item(WorkItemArguments::from([("step", 2)])),
        ]);
        original.process_next().await.unwrap();
        original.process_next().await.unwrap();

        let mut restored = transmit(&original, &resolver);

        while restored.is_in_progress() {
            restored.undo_last().await.unwrap();
        }

        assert!(!restored.is_in_progress());
        assert_eq!(COMPENSATE_COUNT.get(), 2);
    });
}

/// Multiple handoffs across services preserve correctness end-to-end.
#[test]
fn multi_stage_round_trip() {
    let _guard = setup();
    block_on(async {
        let resolver = registered_resolver();

        // Stage 1: the orchestrator processes the first item, then ships the slip.
        let mut slip = RoutingSlip::new([
            success_item(WorkItemArguments::from([("step", 1)])),
            success_item(WorkItemArguments::from([("step", 2)])),
            success_item(WorkItemArguments::from([("step", 3)])),
        ]);
        slip.process_next().await.unwrap();
        let mut slip = transmit(&slip, &resolver);

        // Stage 2: a downstream service processes the second item, ships again.
        slip.process_next().await.unwrap();
        let mut slip = transmit(&slip, &resolver);

        // Stage 3: another service decides to abort and runs the backward path.
        while slip.is_in_progress() {
            slip.undo_last().await.unwrap();
        }

        assert!(!slip.is_in_progress());
        assert_eq!(CALL_COUNT.get(), 2);
        assert_eq!(COMPENSATE_COUNT.get(), 2);
    });
}

/// The wire format uses camelCase keys for cross-language interop.
#[test]
fn wire_format_uses_camel_case() {
    let serializable = SerializableRoutingSlip::new(
        [SerializableWorkLog::new(
            "ActivityA",
            WorkResult::from([("id", 1)]),
        )],
        [SerializableWorkItem::new(
            "ActivityB",
            WorkItemArguments::from([("k", "v")]),
        )],
    );

    let wire = serde_json::to_value(&serializable).unwrap();

    assert_eq!(
        wire,
        serde_json::json!({
            "completedWorkLogs": [{"activityTypeName": "ActivityA", "result": {"id": 1}}],
            "nextWorkItems": [{"activityTypeName": "ActivityB", "arguments": {"k": "v"}}],
        }),
    );
}

#[test]
fn wire_format_round_trip() {
    let original = SerializableRoutingSlip::new(
        [SerializableWorkLog::new("A", WorkResult::from([("x", 1)]))],
        [SerializableWorkItem::new(
            "B",
            WorkItemArguments::from([("y", 2)]),
        )],
    );

    let restored: SerializableRoutingSlip =
        serde_json::from_value(serde_json::to_value(&original).unwrap()).unwrap();

    assert_eq!(restored, original);
}

#[test]
fn wire_format_missing_keys_default_to_empty() {
    let restored: SerializableRoutingSlip = serde_json::from_str("{}").unwrap();

    assert!(restored.completed_work_logs.is_empty());
    assert!(restored.next_work_items.is_empty());
}

/// A payload produced by the Python implementation is accepted unchanged.
///
/// Captured from `python -m ascetic_ddd.saga.examples.serialization_example`;
/// note the space-separated formatting and the insertion-ordered keys, neither
/// of which the Rust side depends on.
#[test]
fn wire_format_accepts_a_python_payload() {
    let payload = r#"{"completedWorkLogs": [{"activityTypeName": "SerializableSuccessActivity", "result": {"reservationId": 7412}}], "nextWorkItems": [{"activityTypeName": "SerializableSuccessActivity", "arguments": {"roomType": "Suite", "checkInDate": "2024-01-15"}}]}"#;

    let serializable: SerializableRoutingSlip = serde_json::from_str(payload).unwrap();
    let slip = from_serializable(&serializable, &registered_resolver()).unwrap();

    assert_eq!(slip.completed_work_logs().len(), 1);
    assert_eq!(
        slip.completed_work_logs()[0]
            .result()
            .get_i64("reservationId")
            .unwrap(),
        7412,
    );
    assert_eq!(slip.pending_work_items().len(), 1);
    assert_eq!(
        slip.pending_work_items()[0]
            .arguments()
            .get_str("roomType")
            .unwrap(),
        "Suite",
    );
}
