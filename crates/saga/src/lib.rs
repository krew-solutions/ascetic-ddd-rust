//! Saga pattern implementation using the routing slip approach.
//!
//! This crate implements the Saga pattern for managing distributed transactions
//! without using traditional two-phase commit. Instead of holding locks across
//! services, a saga splits work into individual activities whose effects can be
//! compensated (reversed) if subsequent steps fail.
//!
//! # Key components
//!
//! * [`Activity`] - the trait saga activities implement (`do_work` + `compensate`);
//! * [`ActivityType`] - handle of an activity implementation (Python's activity class);
//! * [`WorkItem`] - unit of work with arguments for an activity;
//! * [`WorkLog`] - record of completed work, used for compensation;
//! * [`RoutingSlip`] - the document flowing through the saga;
//! * [`ActivityHost`] - processes messages for a specific activity type.
//!
//! # Example
//!
//! ```
//! use ascetic_ddd_saga::{RoutingSlip, WorkItem, WorkItemArguments};
//! use ascetic_ddd_saga::examples::{
//!     ReserveCarActivity, ReserveFlightActivity, ReserveHotelActivity,
//! };
//!
//! # futures::executor::block_on(async {
//! // Create a routing slip with work items.
//! let mut routing_slip = RoutingSlip::new([
//!     WorkItem::of::<ReserveCarActivity>(
//!         WorkItemArguments::from([("vehicleType", "Compact")]),
//!     ),
//!     WorkItem::of::<ReserveHotelActivity>(
//!         WorkItemArguments::from([("roomType", "Suite")]),
//!     ),
//!     WorkItem::of::<ReserveFlightActivity>(
//!         WorkItemArguments::from([("destination", "DUS")]),
//!     ),
//! ]);
//!
//! // Process the saga.
//! while !routing_slip.is_completed() {
//!     if !routing_slip.process_next().await? {
//!         // Compensation needed.
//!         while routing_slip.is_in_progress() {
//!             routing_slip.undo_last().await?;
//!         }
//!         break;
//!     }
//! }
//!
//! assert_eq!(routing_slip.completed_work_logs().len(), 3);
//! # Ok::<(), ascetic_ddd_saga::SagaError>(())
//! # }).unwrap();
//! ```
//!
//! # Relation to the Python implementation
//!
//! This crate is a port of `ascetic_ddd.saga`. The module layout and the
//! semantics are preserved; the differences are those Rust forces:
//!
//! * a Python activity *class* becomes an [`ActivityType`] - a [`TypeId`]
//!   paired with a factory function;
//! * `InvalidOperationError` and `KeyError` become variants of [`SagaError`];
//! * `dict[str, Any]` becomes a map of [`Value`], which is either JSON data or
//!   an opaque object;
//! * the `NamedActivity` protocol becomes the optional
//!   [`Activity::type_name()`] method;
//! * nested routing slips ([`FallbackActivity`], [`ParallelActivity`]) are
//!   shared as [`SharedRoutingSlip`], since Rust has no implicit reference
//!   semantics.
//!
//! [`TypeId`]: std::any::TypeId
//!
//! # See also
//!
//! [Sagas](https://vasters.com/archive/Sagas.html) - the original article by
//! Clemens Vasters.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(clippy::all)]

pub mod activity;
pub mod activity_host;
pub mod activity_resolver;
pub mod error;
pub mod examples;
pub mod fallback_activity;
pub mod parallel_activity;
pub mod routing_slip;
pub mod routing_slip_serialization;
pub mod serializable_routing_slip;
pub mod value;
pub mod work_item;
pub mod work_item_arguments;
pub mod work_log;
pub mod work_result;

pub use crate::activity::{Activity, ActivityType};
pub use crate::activity_host::{ActivityHost, FnSender, MessageSender};
pub use crate::activity_resolver::{ActivityTypeResolver, MapBasedResolver};
pub use crate::error::{BoxError, Result, SagaError};
pub use crate::fallback_activity::FallbackActivity;
pub use crate::parallel_activity::ParallelActivity;
pub use crate::routing_slip::{RoutingSlip, SharedRoutingSlip};
pub use crate::routing_slip_serialization::{from_serializable, to_serializable};
pub use crate::serializable_routing_slip::{
    SerializableRoutingSlip, SerializableWorkItem, SerializableWorkLog,
};
pub use crate::value::Value;
pub use crate::work_item::WorkItem;
pub use crate::work_item_arguments::WorkItemArguments;
pub use crate::work_log::WorkLog;
pub use crate::work_result::WorkResult;

/// Re-exported so that activities can be implemented without depending on
/// `async-trait` directly.
pub use async_trait::async_trait;
