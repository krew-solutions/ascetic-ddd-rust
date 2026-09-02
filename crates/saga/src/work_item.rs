//! Work item - unit of work to be processed by an activity.

use crate::activity::{Activity, ActivityType};
use crate::work_item_arguments::WorkItemArguments;

/// A unit of work to be processed by a specific activity type.
///
/// Contains the arguments needed by the activity and the handle of the activity
/// type that will process it.
///
/// ```
/// use ascetic_ddd_saga::{ActivityType, WorkItem, WorkItemArguments};
/// use ascetic_ddd_saga::examples::ReserveCarActivity;
///
/// let work_item = WorkItem::of::<ReserveCarActivity>(
///     WorkItemArguments::from([("vehicleType", "SUV")]),
/// );
///
/// assert_eq!(work_item.activity_type(), ActivityType::of::<ReserveCarActivity>());
/// assert_eq!(work_item.arguments().get_str("vehicleType").unwrap(), "SUV");
/// ```
#[derive(Clone, Debug)]
pub struct WorkItem {
    activity_type: ActivityType,
    arguments: WorkItemArguments,
}

impl WorkItem {
    /// Creates a work item for the given activity type.
    pub fn new(activity_type: ActivityType, arguments: WorkItemArguments) -> Self {
        WorkItem {
            activity_type,
            arguments,
        }
    }

    /// Creates a work item for the activity implementation `A`.
    ///
    /// The counterpart of Python's `WorkItem(SomeActivity, arguments)`.
    pub fn of<A: Activity + Default>(arguments: WorkItemArguments) -> Self {
        WorkItem::new(ActivityType::of::<A>(), arguments)
    }

    /// The type of activity that will process this work item.
    pub fn activity_type(&self) -> ActivityType {
        self.activity_type
    }

    /// The arguments for the activity.
    pub fn arguments(&self) -> &WorkItemArguments {
        &self.arguments
    }
}
