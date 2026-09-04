# RoutingSlip Serialization Guide

How to serialize and deserialize a `RoutingSlip` for transmission over a
message bus. The Rust counterpart of
[`SERIALIZATION.md`](https://github.com/krew-solutions/ascetic-ddd-go/blob/main/asceticddd/saga/SERIALIZATION.md)
from the Go implementation; the wire format is identical, so services written
in Rust, Go and Python can take part in the same saga.

## Overview

A `RoutingSlip` refers to activities by `ActivityType` — a `TypeId` plus a
factory function, neither of which can be serialized. Instead of a global
registry, the crate uses **dependency injection**: an `ActivityTypeResolver`
translates between activity types and their canonical names.

## Key components

### 1. `ActivityTypeResolver`

Bidirectional mapping between activity type names and activity types:

```rust
pub trait ActivityTypeResolver: Send + Sync {
    fn resolve(&self, type_name: &str) -> Result<ActivityType>;
    fn get_name(&self, activity_type: ActivityType) -> Result<String>;
}
```

### 2. `MapBasedResolver`

The default implementation, backed by in-memory maps:

```rust
let mut resolver = MapBasedResolver::new();
resolver.register_type::<ReserveCarActivity>("ReserveCarActivity");
resolver.register_type::<ReserveHotelActivity>("ReserveHotelActivity");
```

`register_type::<A>(name)` is shorthand for
`register(name, ActivityType::of::<A>())`; use the latter when the activity
type is already at hand as a value.

### 3. `Activity::type_name()` (optional)

An activity may report its own canonical name — the counterpart of the
`NamedActivity` interface in Go and of the `NamedActivity` protocol in Python:

```rust
impl Activity for ReserveCarActivity {
    // ...
    fn type_name(&self) -> Option<&str> {
        Some("ReserveCarActivity")
    }
}
```

**Benefits:**

- enables fallback serialization even without explicit registration;
- deserialization still requires registration, for security;
- makes activity types self-documenting.

The default implementation returns `None`, which is the equivalent of not
implementing the protocol at all: such an activity can only be serialized
after being registered.

## Basic usage

### Step 1: create a resolver and register activities

```rust
use ascetic_ddd_saga::MapBasedResolver;

let mut resolver = MapBasedResolver::new();
resolver.register_type::<ReserveCarActivity>("ReserveCarActivity");
resolver.register_type::<ReserveHotelActivity>("ReserveHotelActivity");
resolver.register_type::<ReserveFlightActivity>("ReserveFlightActivity");
```

### Step 2: serialize the routing slip

```rust
use ascetic_ddd_saga::{RoutingSlip, WorkItem, WorkItemArguments, to_serializable};

let mut routing_slip = RoutingSlip::new([
    WorkItem::of::<ReserveCarActivity>(WorkItemArguments::from([("vehicleType", "SUV")])),
    WorkItem::of::<ReserveHotelActivity>(WorkItemArguments::from([("roomType", "Suite")])),
]);

// Process some work.
routing_slip.process_next().await?;

// Convert to the intermediate form, then to JSON for transmission.
let payload = serde_json::to_vec(&to_serializable(&routing_slip, &resolver)?)?;

bus.publish("saga/routing-slip", &payload).await?;
```

### Step 3: deserialize the routing slip

```rust
use ascetic_ddd_saga::{SerializableRoutingSlip, from_serializable};

let payload = bus.receive("saga/routing-slip").await?;

let serializable: SerializableRoutingSlip = serde_json::from_slice(&payload)?;
let mut routing_slip = from_serializable(&serializable, &resolver)?;

// Continue processing.
routing_slip.process_next().await?;
```

## Serialized format

```json
{
  "completedWorkLogs": [
    {
      "activityTypeName": "ReserveCarActivity",
      "result": {
        "reservationId": 12345
      }
    }
  ],
  "nextWorkItems": [
    {
      "activityTypeName": "ReserveHotelActivity",
      "arguments": {
        "roomType": "Suite",
        "checkInDate": "2024-01-15"
      }
    },
    {
      "activityTypeName": "ReserveFlightActivity",
      "arguments": {
        "destination": "LAX",
        "flightDate": "2024-01-15"
      }
    }
  ]
}
```

`completedWorkLogs` and `nextWorkItems` default to empty lists when absent.
Keys inside `arguments` and `result` are emitted in sorted order (the map is a
`BTreeMap`), which is irrelevant to JSON semantics and does not affect
interoperability with the Go and Python implementations.

**Only JSON data crosses the wire.** A `Value::Any` — the opaque variant that
carries nested routing slips for `FallbackActivity` and `ParallelActivity` —
makes serialization fail, just as `json.dumps()` fails on a `RoutingSlip` in
Python. Fork/join and fallback steps are therefore local to one service.

## Advanced patterns

### Multiple resolvers for different services

Service-specific resolvers limit what each service is able to restore:

```rust
// The orchestrator knows every activity.
let mut orchestrator = MapBasedResolver::new();
orchestrator.register_type::<ReserveCarActivity>("ReserveCarActivity");
orchestrator.register_type::<ReserveHotelActivity>("ReserveHotelActivity");
orchestrator.register_type::<ReserveFlightActivity>("ReserveFlightActivity");

// The car service knows only car activities.
let mut car_service = MapBasedResolver::new();
car_service.register_type::<ReserveCarActivity>("ReserveCarActivity");

// The hotel service knows only hotel activities.
let mut hotel_service = MapBasedResolver::new();
hotel_service.register_type::<ReserveHotelActivity>("ReserveHotelActivity");
```

A slip mentioning an unregistered activity — whether as a pending work item or
as a completed work log — fails with `SagaError::ActivityTypeNotRegistered`
rather than being silently truncated. See
`multiple_resolvers_for_different_services` in
[`tests/serialization_example.rs`](tests/serialization_example.rs).

### Compensation serialization

The same conversion works for the backward path:

```rust
// After a failure, serialize and route to the compensation queue.
let payload = serde_json::to_vec(&to_serializable(&routing_slip, &resolver)?)?;
if let Some(uri) = routing_slip.compensation_uri() {
    bus.publish(&uri, &payload).await?;
}

// On the compensation service.
let serializable: SerializableRoutingSlip = serde_json::from_slice(&payload)?;
let mut routing_slip = from_serializable(&serializable, &resolver)?;

while routing_slip.is_in_progress() {
    routing_slip.undo_last().await?;
}
```

### Testing with isolated resolvers

Each test builds its own resolver, so there is no state to clean up:

```rust
#[test]
fn my_scenario() {
    let mut resolver = MapBasedResolver::new();
    resolver.register_type::<TestActivity>("TestActivity");

    // No interference from other tests, no global state pollution.
}
```

## Design rationale

### Why not a global registry?

1. **No global state.** Each resolver is independent, preventing pollution
   between tests and services.
2. **Better testability.** Tests create isolated resolvers without cleanup.
3. **Explicit dependencies.** Resolvers are passed explicitly, so the
   dependency is visible in the signature.
4. **Service isolation.** Different services can be configured differently.
5. **No synchronization.** A registry shared across threads would need a lock
   (or a `OnceLock` and a rule against late registration); an owned resolver
   needs neither.

### Trade-offs

**Pros:** clean dependency injection, excellent testability, service
isolation, no global state.

**Cons:** slightly more verbose (the resolver must be threaded through); a
resolver is needed on both ends of the wire; an activity must implement
`type_name()` to be serializable without registration.

## Error handling

### Unregistered activity type (deserialization)

```rust
let restored = from_serializable(&serializable, &resolver);
// Err(SagaError::ActivityTypeNotRegistered("UnknownActivity"))
```

**Solution:** register every activity before deserializing.

### Unregistered and unnamed activity type (serialization)

```rust
let serializable = to_serializable(&routing_slip, &resolver);
// Err(SagaError::ActivityTypeNotRegistered("MyActivity"))
```

**Solution:** either register the activity or implement
`Activity::type_name()`.

### Opaque argument (serialization)

```rust
let payload = serde_json::to_vec(&to_serializable(&routing_slip, &resolver)?);
// Err(serde_json::Error: "an opaque Value::Any cannot be serialized")
```

**Solution:** keep nested routing slips local, or model the nested work as
plain data that the receiving service rebuilds.

## Best practices

1. **Register activities at startup**, in one place:

   ```rust
   pub fn make_resolver() -> MapBasedResolver {
       let mut resolver = MapBasedResolver::new();
       resolver.register_type::<Activity1>("Activity1");
       resolver.register_type::<Activity2>("Activity2");
       resolver
   }
   ```

2. **Implement `type_name()`** on every activity meant to cross the wire.

3. **Use descriptive names** — `"ReserveCarActivity"`, not `"car"`. The name is
   a wire contract: renaming the Rust type is safe, renaming the registered
   name is a breaking change.

4. **Share the resolver configuration** through a constructor function rather
   than duplicating registrations per call site.

5. **Test round-trip serialization**, including the backward path:

   ```rust
   let wire = serde_json::to_string(&to_serializable(&original, &resolver)?)?;
   let restored = from_serializable(&serde_json::from_str(&wire)?, &resolver)?;

   assert_eq!(
       restored.pending_work_items().len(),
       original.pending_work_items().len(),
   );
   ```

## See also

- [`tests/activity_resolver.rs`](tests/activity_resolver.rs) — resolver tests
- [`tests/routing_slip_serialization.rs`](tests/routing_slip_serialization.rs) —
  serialization tests, including a payload captured from the Python
  implementation
- [`examples/serialization_example.rs`](examples/serialization_example.rs) —
  a runnable end-to-end demo (`cargo run --example serialization_example`)
- [Documentation of the Python version](https://krew-solutions.github.io/ascetic-ddd-python/modules/saga/index.html)
