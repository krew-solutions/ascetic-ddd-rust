//! Activity - the unit of business logic a saga is built from.

use std::any::TypeId;
use std::fmt;
use std::hash::{Hash, Hasher};

use async_trait::async_trait;

use crate::error::Result;
use crate::routing_slip::RoutingSlip;
use crate::work_item::WorkItem;
use crate::work_log::WorkLog;

/// A handle to an activity implementation: its identity plus a way to create it.
///
/// Python passes the activity *class* around (`WorkItem(ReserveCarActivity, ...)`)
/// and instantiates it on demand. Rust has no first-class type values, so a
/// class is represented by a [`TypeId`] (identity, used as a resolver map key)
/// paired with a factory function (instantiation).
///
/// Because the routing slip creates activities with no arguments, an activity
/// type must implement [`Default`] -- the equivalent of Python's implicit
/// no-argument constructor.
///
/// ```
/// use ascetic_ddd_saga::{ActivityType, examples::ReserveCarActivity};
///
/// let activity_type = ActivityType::of::<ReserveCarActivity>();
///
/// assert_eq!(activity_type, ActivityType::of::<ReserveCarActivity>());
/// assert_eq!(
///     activity_type.create().work_item_queue_address(),
///     "sb://./carReservations",
/// );
/// ```
#[derive(Clone, Copy)]
pub struct ActivityType {
    type_id: TypeId,
    type_path: &'static str,
    factory: fn() -> Box<dyn Activity>,
}

impl ActivityType {
    /// Returns the handle of the activity implementation `A`.
    pub fn of<A: Activity + Default>() -> Self {
        ActivityType {
            type_id: TypeId::of::<A>(),
            type_path: std::any::type_name::<A>(),
            factory: || Box::new(A::default()),
        }
    }

    /// Creates a new instance of the activity.
    ///
    /// The counterpart of Python's `activity_type()`.
    pub fn create(&self) -> Box<dyn Activity> {
        (self.factory)()
    }

    /// Returns the [`TypeId`] identifying the activity implementation.
    pub fn type_id(&self) -> TypeId {
        self.type_id
    }

    /// Returns the fully qualified path of the activity implementation.
    pub fn type_path(&self) -> &'static str {
        self.type_path
    }

    /// Returns the last segment of the type path.
    ///
    /// The counterpart of Python's `activity_type.__name__`.
    pub fn short_name(&self) -> &'static str {
        self.type_path.rsplit("::").next().unwrap_or(self.type_path)
    }
}

impl PartialEq for ActivityType {
    fn eq(&self, other: &Self) -> bool {
        self.type_id == other.type_id
    }
}

impl Eq for ActivityType {}

impl Hash for ActivityType {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.type_id.hash(state);
    }
}

impl fmt::Debug for ActivityType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("ActivityType")
            .field(&self.type_path)
            .finish()
    }
}

/// A saga activity.
///
/// Each activity encapsulates two operations:
///
/// * [`do_work()`][Activity::do_work] - performs the actual business operation;
/// * [`compensate()`][Activity::compensate] - reverses the operation if the saga
///   fails.
///
/// Activities are executed by [`ActivityHost`][crate::activity_host::ActivityHost]
/// and their results are tracked in the [`RoutingSlip`].
///
/// ```
/// use ascetic_ddd_saga::{
///     Activity, ActivityType, Result, RoutingSlip, WorkItem, WorkLog, WorkResult,
/// };
/// use ascetic_ddd_saga::async_trait;
///
/// #[derive(Default)]
/// struct ReserveTableActivity;
///
/// #[async_trait]
/// impl Activity for ReserveTableActivity {
///     async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>> {
///         let seats = work_item.arguments().get_i64("seats")?;
///         Ok(Some(WorkLog::new(self, WorkResult::from([("seats", seats)]))))
///     }
///
///     async fn compensate(
///         &self,
///         _work_log: &WorkLog,
///         _routing_slip: &mut RoutingSlip,
///     ) -> Result<bool> {
///         Ok(true)
///     }
///
///     fn work_item_queue_address(&self) -> &str {
///         "sb://./tableReservations"
///     }
///
///     fn compensation_queue_address(&self) -> &str {
///         "sb://./tableCancellations"
///     }
///
///     fn activity_type(&self) -> ActivityType {
///         ActivityType::of::<Self>()
///     }
/// }
/// ```
#[async_trait]
pub trait Activity: Send + Sync + 'static {
    /// Executes the activity's business logic.
    ///
    /// Returns the [`WorkLog`] holding the result of the work, or `None` if the
    /// work failed. An `Err` is the counterpart of a raised exception: the
    /// routing slip treats it exactly like `None`, but hosts and callers can
    /// still inspect the cause.
    async fn do_work(&self, work_item: &WorkItem) -> Result<Option<WorkLog>>;

    /// Compensates (undoes) the previously completed work.
    ///
    /// Called during the backward path when the saga needs to be rolled back.
    ///
    /// Returns `true` if compensation was successful and the backward path
    /// should continue, `false` if compensation added new work and the forward
    /// path should resume. `routing_slip` is the current slip, which
    /// compensation may extend with new work items.
    async fn compensate(&self, work_log: &WorkLog, routing_slip: &mut RoutingSlip) -> Result<bool>;

    /// Address of the queue for processing work items (forward path).
    fn work_item_queue_address(&self) -> &str;

    /// Address of the queue for processing compensation (backward path).
    fn compensation_queue_address(&self) -> &str;

    /// Returns the handle of this activity's implementation.
    ///
    /// Always `ActivityType::of::<Self>()`. Python derives it with
    /// `type(activity)`; Rust cannot recover a concrete type from a trait
    /// object, so implementations state it explicitly -- as they do in the Go
    /// port.
    fn activity_type(&self) -> ActivityType;

    /// The canonical name of this activity type, used on the wire.
    ///
    /// This is the counterpart of Python's `NamedActivity` protocol: returning
    /// `Some` makes the activity resolvable by name in
    /// [`ActivityTypeResolver::get_name()`][crate::activity_resolver::ActivityTypeResolver::get_name]
    /// even when it has not been registered explicitly. The default
    /// implementation returns `None`, the equivalent of not implementing the
    /// protocol.
    fn type_name(&self) -> Option<&str> {
        None
    }
}
