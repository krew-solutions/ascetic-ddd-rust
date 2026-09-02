//! Activity host - processes messages for a specific activity type.

use std::sync::Arc;

use async_trait::async_trait;

use crate::activity::{Activity, ActivityType};
use crate::error::Result;
use crate::routing_slip::RoutingSlip;

/// Transport used by an [`ActivityHost`] to forward a routing slip.
///
/// The counterpart of the `send` callback in Python, which may be either a
/// plain function or a coroutine function. The slip is borrowed for the
/// duration of the call: a real implementation serializes it (see
/// [`to_serializable`][crate::routing_slip_serialization::to_serializable]) and
/// publishes the payload to `uri`.
#[async_trait]
pub trait MessageSender: Send + Sync {
    /// Sends the routing slip to the queue identified by `uri`.
    async fn send(&self, uri: &str, routing_slip: &RoutingSlip) -> Result<()>;
}

/// Adapts a synchronous closure into a [`MessageSender`].
///
/// ```
/// use std::sync::Mutex;
///
/// use ascetic_ddd_saga::{FnSender, MessageSender};
///
/// let sent: Mutex<Vec<String>> = Mutex::new(Vec::new());
/// let sender = FnSender::new(|uri: &str, _slip: &_| {
///     sent.lock().unwrap().push(uri.to_owned());
///     Ok(())
/// });
///
/// futures::executor::block_on(sender.send("sb://./queue", &Default::default())).unwrap();
///
/// assert_eq!(sent.into_inner().unwrap(), ["sb://./queue"]);
/// ```
pub struct FnSender<F>(F);

impl<F> FnSender<F>
where
    F: Fn(&str, &RoutingSlip) -> Result<()> + Send + Sync,
{
    /// Wraps the closure.
    pub fn new(send: F) -> Self {
        FnSender(send)
    }
}

#[async_trait]
impl<F> MessageSender for FnSender<F>
where
    F: Fn(&str, &RoutingSlip) -> Result<()> + Send + Sync,
{
    async fn send(&self, uri: &str, routing_slip: &RoutingSlip) -> Result<()> {
        (self.0)(uri, routing_slip)
    }
}

/// Host for processing messages for a specific activity type.
///
/// Manages local execution by:
///
/// * processing forward messages to execute
///   [`do_work()`][Activity::do_work];
/// * processing backward messages to invoke
///   [`compensate()`][Activity::compensate];
/// * routing results to the appropriate next addresses.
///
/// Python parameterises the host with the activity class (`ActivityHost[T]`);
/// here the same role is played by the [`ActivityType`] value it is built with.
pub struct ActivityHost {
    activity_type: ActivityType,
    sender: Arc<dyn MessageSender>,
}

impl ActivityHost {
    /// Creates a host for the given activity type.
    pub fn new(activity_type: ActivityType, sender: Arc<dyn MessageSender>) -> Self {
        ActivityHost {
            activity_type,
            sender,
        }
    }

    /// Creates a host for the activity implementation `A`.
    pub fn of<A: Activity + Default>(sender: Arc<dyn MessageSender>) -> Self {
        ActivityHost::new(ActivityType::of::<A>(), sender)
    }

    /// The type of activity this host manages.
    pub fn activity_type(&self) -> ActivityType {
        self.activity_type
    }

    /// Processes a forward (`do_work`) message.
    ///
    /// If work succeeds, sends the slip to the next activity's work queue.
    /// If work fails, sends it to the compensation queue for rollback.
    pub async fn process_forward_message(&self, routing_slip: &mut RoutingSlip) -> Result<()> {
        if routing_slip.is_completed() {
            return Ok(());
        }

        let uri = if routing_slip.process_next().await? {
            // Success - continue forward.
            routing_slip.progress_uri()
        } else {
            // Failure - start compensation.
            routing_slip.compensation_uri()
        };

        if let Some(uri) = uri {
            self.sender.send(&uri, routing_slip).await?;
        }

        Ok(())
    }

    /// Processes a backward (`compensate`) message.
    ///
    /// If compensation succeeds, continues backward to the previous activity.
    /// If compensation returns `false` (it added new work), resumes forward.
    pub async fn process_backward_message(&self, routing_slip: &mut RoutingSlip) -> Result<()> {
        if !routing_slip.is_in_progress() {
            return Ok(());
        }

        let uri = if routing_slip.undo_last().await? {
            // Continue backward.
            routing_slip.compensation_uri()
        } else {
            // Resume forward (compensation added new work).
            routing_slip.progress_uri()
        };

        if let Some(uri) = uri {
            self.sender.send(&uri, routing_slip).await?;
        }

        Ok(())
    }

    /// Accepts and processes a message if it matches one of this host's queues.
    ///
    /// Returns `true` if the message was accepted and processed.
    pub async fn accept_message(&self, uri: &str, routing_slip: &mut RoutingSlip) -> Result<bool> {
        let activity = self.activity_type.create();

        if activity.compensation_queue_address() == uri {
            self.process_backward_message(routing_slip).await?;
            return Ok(true);
        }

        if activity.work_item_queue_address() == uri {
            self.process_forward_message(routing_slip).await?;
            return Ok(true);
        }

        Ok(false)
    }
}
