//! Serializable forms of [`RoutingSlip`] and its constituents.
//!
//! Plain data containers with no behavior. A [`RoutingSlip`] holds activity
//! types (not JSON-serializable); these structs hold activity *names* (strings)
//! instead, ready for transmission over a message bus.
//!
//! Field names are mapped to camelCase JSON keys, matching the wire format
//! produced by the Go and Python implementations so that distributed sagas can
//! interoperate across languages.
//!
//! ```
//! use ascetic_ddd_saga::{
//!     SerializableRoutingSlip, SerializableWorkItem, WorkItemArguments,
//! };
//!
//! let serializable = SerializableRoutingSlip::new(
//!     [],
//!     [SerializableWorkItem::new(
//!         "ReserveCarActivity",
//!         WorkItemArguments::from([("vehicleType", "Compact")]),
//!     )],
//! );
//!
//! let wire = serde_json::to_string(&serializable).unwrap();
//!
//! assert_eq!(
//!     wire,
//!     r#"{"completedWorkLogs":[],"nextWorkItems":[{"activityTypeName":"ReserveCarActivity","arguments":{"vehicleType":"Compact"}}]}"#,
//! );
//! assert_eq!(
//!     serde_json::from_str::<SerializableRoutingSlip>(&wire).unwrap(),
//!     serializable,
//! );
//! ```
//!
//! [`RoutingSlip`]: crate::routing_slip::RoutingSlip

use serde::{Deserialize, Serialize};

use crate::work_item_arguments::WorkItemArguments;
use crate::work_result::WorkResult;

/// Serializable counterpart of [`WorkItem`][crate::work_item::WorkItem].
///
/// Stores the activity type name (string) instead of the activity type itself,
/// plus the same arguments dictionary.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializableWorkItem {
    /// Name under which the activity type is registered.
    pub activity_type_name: String,
    /// Arguments for the activity.
    pub arguments: WorkItemArguments,
}

impl SerializableWorkItem {
    /// Creates a serializable work item.
    pub fn new(activity_type_name: impl Into<String>, arguments: WorkItemArguments) -> Self {
        SerializableWorkItem {
            activity_type_name: activity_type_name.into(),
            arguments,
        }
    }
}

/// Serializable counterpart of [`WorkLog`][crate::work_log::WorkLog].
///
/// Stores the activity type name (string) instead of the activity type itself,
/// plus the same result dictionary.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializableWorkLog {
    /// Name under which the activity type is registered.
    pub activity_type_name: String,
    /// Result of the activity's work.
    pub result: WorkResult,
}

impl SerializableWorkLog {
    /// Creates a serializable work log.
    pub fn new(activity_type_name: impl Into<String>, result: WorkResult) -> Self {
        SerializableWorkLog {
            activity_type_name: activity_type_name.into(),
            result,
        }
    }
}

/// Serializable counterpart of [`RoutingSlip`][crate::routing_slip::RoutingSlip].
///
/// Carries the same forward queue (`next_work_items`) and backward stack
/// (`completed_work_logs`), but in name-keyed form ready for JSON.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SerializableRoutingSlip {
    /// Backward stack, oldest first.
    #[serde(default)]
    pub completed_work_logs: Vec<SerializableWorkLog>,
    /// Forward queue, in processing order.
    #[serde(default)]
    pub next_work_items: Vec<SerializableWorkItem>,
}

impl SerializableRoutingSlip {
    /// Creates a serializable routing slip.
    pub fn new(
        completed_work_logs: impl IntoIterator<Item = SerializableWorkLog>,
        next_work_items: impl IntoIterator<Item = SerializableWorkItem>,
    ) -> Self {
        SerializableRoutingSlip {
            completed_work_logs: completed_work_logs.into_iter().collect(),
            next_work_items: next_work_items.into_iter().collect(),
        }
    }
}
