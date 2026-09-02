# ascetic-ddd-saga

Saga pattern implementation using the **routing slip** approach, based on
[Clemens Vasters' article](https://vasters.com/archive/Sagas.html). A port of
`ascetic_ddd.saga` from
[ascetic-ddd-python](https://github.com/krew-solutions/ascetic-ddd-python),
wire-compatible with it and with the Go implementation.

A saga splits a distributed transaction into activities whose effects can be
**compensated** (reversed), instead of holding locks across services.

```rust
use ascetic_ddd_saga::examples::{
    ReserveCarActivity, ReserveFlightActivity, ReserveHotelActivity,
};
use ascetic_ddd_saga::{Result, RoutingSlip, WorkItem, WorkItemArguments};

async fn book_travel() -> Result<()> {
    let mut routing_slip = RoutingSlip::new([
        WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([("vehicleType", "Compact")])),
        WorkItem::of::<ReserveHotelActivity>(WorkItemArguments::from([("roomType", "Suite")])),
        WorkItem::of::<ReserveFlightActivity>(WorkItemArguments::from([("destination", "DUS")])),
    ]);

    while !routing_slip.is_completed() {
        if !routing_slip.process_next().await? {
            // Compensation needed: walk the backward path.
            while routing_slip.is_in_progress() {
                routing_slip.undo_last().await?;
            }
            break;
        }
    }

    Ok(())
}
```

## Module map

Every Python module has a counterpart of the same name:

| Rust module | Python module | Contents |
| --- | --- | --- |
| `activity` | `activity.py` | `Activity` trait, `ActivityType` |
| `activity_host` | `activity_host.py` | `ActivityHost`, `MessageSender`, `FnSender` |
| `activity_resolver` | `activity_resolver.py` | `ActivityTypeResolver`, `MapBasedResolver` |
| `error` | — | `SagaError`, `Result` |
| `examples` | `examples/` | Travel booking example activities |
| `fallback_activity` | `fallback_activity.py` | `FallbackActivity` |
| `parallel_activity` | `parallel_activity.py` | `ParallelActivity` |
| `routing_slip` | `routing_slip.py` | `RoutingSlip`, `SharedRoutingSlip` |
| `routing_slip_serialization` | `routing_slip_serialization.py` | `to_serializable`, `from_serializable` |
| `serializable_routing_slip` | `serializable_routing_slip.py` | Wire-format structs |
| `value` | — | `Value` (the `Any` of `dict[str, Any]`) |
| `work_item` | `work_item.py` | `WorkItem` |
| `work_item_arguments` | `work_item_arguments.py` | `WorkItemArguments` |
| `work_log` | `work_log.py` | `WorkLog` |
| `work_result` | `work_result.py` | `WorkResult` |

## Differences from the Python implementation

The semantics are preserved; these are the adaptations Rust requires.

| Python | Rust | Why |
| --- | --- | --- |
| `WorkItem(SomeActivity, args)` — the class itself | `WorkItem::of::<SomeActivity>(args)` → `ActivityType` (a `TypeId` + factory) | Rust has no first-class type values; an activity type must be `Default` so the slip can instantiate it, as Python's no-argument constructor allows |
| `type(activity)` inside `WorkLog` | `Activity::activity_type()`, always `ActivityType::of::<Self>()` | A concrete type cannot be recovered from a trait object (the Go port solves it the same way) |
| `NamedActivity` protocol | `Activity::type_name() -> Option<&str>`, defaulting to `None` | Rust cannot test at runtime whether a type implements a trait |
| `InvalidOperationError`, `KeyError` | `SagaError::{InvalidOperation, MissingKey, ActivityTypeNotRegistered, UnexpectedType}` | Errors are values |
| exception raised by `do_work()` | `Err(SagaError::Activity(..))`, treated by `process_next()` exactly like `None` | matches the `except Exception: pass` of the original |
| `dict[str, Any]` | `BTreeMap<String, Value>`, where `Value` is JSON data **or** an opaque `Arc<dyn Any>` | only JSON data can cross the wire; the opaque variant carries nested routing slips, as `Any` does in Python |
| nested slips passed by reference | `SharedRoutingSlip = Arc<Mutex<RoutingSlip>>` | `FallbackActivity` and `ParallelActivity` mutate a slip they share with their caller |
| `send(uri, slip)`, sync or coroutine | `MessageSender::send()` (async), or `FnSender` around a closure | one trait covers both cases |
| `asyncio.gather` in `ParallelActivity` | `futures::future::join_all` | runtime-agnostic: the crate depends on no executor |
| `to_dict()` / `from_dict()` | `serde` derives on the `Serializable*` structs | `serde_json::to_string` / `from_str` produce the same camelCase payload |
| `TypeError` for an incomplete subclass | compile error | the five Python tests asserting this have no runtime counterpart |

## Testing

```bash
cargo test    # 114 integration tests (one module per Python test module) + doctests
```

`tests/routing_slip_serialization.rs` also checks that a payload produced by the
Python implementation deserializes unchanged.

## Running the example

```bash
cargo run --example serialization_example
```

The counterpart of
`python -m ascetic_ddd.saga.examples.serialization_example`.
