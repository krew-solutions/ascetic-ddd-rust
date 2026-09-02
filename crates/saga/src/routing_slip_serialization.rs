//! Conversion between [`RoutingSlip`] and [`SerializableRoutingSlip`].
//!
//! A [`RoutingSlip`] references activity types directly, which are not
//! JSON-serializable. Conversion to and from the wire format goes through an
//! [`ActivityTypeResolver`] that translates between activity types and their
//! canonical names.

use crate::activity_resolver::ActivityTypeResolver;
use crate::error::Result;
use crate::routing_slip::RoutingSlip;
use crate::serializable_routing_slip::{
    SerializableRoutingSlip, SerializableWorkItem, SerializableWorkLog,
};
use crate::work_item::WorkItem;
use crate::work_log::WorkLog;

/// Converts a routing slip into its serializable form.
///
/// # Errors
///
/// [`SagaError::ActivityTypeNotRegistered`][crate::error::SagaError::ActivityTypeNotRegistered]
/// if any activity type involved is neither registered in the resolver nor
/// reports a canonical name through
/// [`Activity::type_name()`][crate::activity::Activity::type_name].
pub fn to_serializable<R>(
    routing_slip: &RoutingSlip,
    resolver: &R,
) -> Result<SerializableRoutingSlip>
where
    R: ActivityTypeResolver + ?Sized,
{
    let mut completed_work_logs = Vec::with_capacity(routing_slip.completed_work_logs().len());
    for work_log in routing_slip.completed_work_logs() {
        completed_work_logs.push(SerializableWorkLog::new(
            resolver.get_name(work_log.activity_type())?,
            work_log.result().clone(),
        ));
    }

    let mut next_work_items = Vec::with_capacity(routing_slip.pending_work_items().len());
    for work_item in routing_slip.pending_work_items() {
        next_work_items.push(SerializableWorkItem::new(
            resolver.get_name(work_item.activity_type())?,
            work_item.arguments().clone(),
        ));
    }

    Ok(SerializableRoutingSlip::new(
        completed_work_logs,
        next_work_items,
    ))
}

/// Reconstructs a routing slip from its serializable form.
///
/// The result is in the same state as the original: the same completed work
/// logs and the same pending work items, in the same order.
///
/// # Errors
///
/// [`SagaError::ActivityTypeNotRegistered`][crate::error::SagaError::ActivityTypeNotRegistered]
/// if any activity name is not registered in the resolver.
pub fn from_serializable<R>(
    serializable: &SerializableRoutingSlip,
    resolver: &R,
) -> Result<RoutingSlip>
where
    R: ActivityTypeResolver + ?Sized,
{
    let mut routing_slip = RoutingSlip::default();

    for work_item in &serializable.next_work_items {
        routing_slip.add_work_item(WorkItem::new(
            resolver.resolve(&work_item.activity_type_name)?,
            work_item.arguments.clone(),
        ));
    }

    for work_log in &serializable.completed_work_logs {
        routing_slip.add_completed_work_log(WorkLog::with_activity_type(
            resolver.resolve(&work_log.activity_type_name)?,
            work_log.result.clone(),
        ));
    }

    Ok(routing_slip)
}
