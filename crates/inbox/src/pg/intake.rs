//! The intake (ADR-0036 decision 1): every message handed to the inbox is
//! stored as a row, found already stored, or set aside beside it. An error
//! is returned only when the environment fails, so that the source holds
//! the message and redelivers it, in order, once the cause has passed:
//! measured, the members of a consumer group recover by themselves. A
//! message that can never be stored is never an error, since an error holds
//! its source and stops the channel in every member of the group, in turn.

use ascetic_ddd_bus::identity::{STREAM_TYPE, TENANT_ID};
use ascetic_ddd_bus::{Message, uri};
use ascetic_ddd_session::{PgAccess, SessionPool};

use super::PgInbox;
use super::store::{Admitted, Aside};
use crate::error::Error;
use crate::intake::{self, Reason};
use crate::message::InboxMessage;
use crate::observer::{InboxObserver, Receipt, Received, Rejected};

/// What the intake did with a message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Stored as a row, where it landed.
    Stored(Receipt),
    /// Already stored under its identity: a redelivery, a republication, or
    /// one fact in two forms (ADR-0030).
    Duplicate,
    /// Set aside beside the inbox, never stored as a row.
    SetAside {
        /// The digest it is kept under.
        digest: String,
        /// Why.
        reason: Reason,
        /// Whether the same bytes had been set aside before.
        repeat: bool,
    },
}

impl<P, O> PgInbox<P, O>
where
    P: SessionPool + Send + Sync,
    P::Session: PgAccess + Sync,
    O: InboxObserver,
{
    /// Takes `message`, received at `destination` — the inbox channel, its
    /// key included — through the intake.
    ///
    /// # Errors
    /// [`Error`] only when the environment fails — the pool, the session,
    /// the server, a holder gone before it could be read — so that the
    /// source tries again.
    pub async fn intake(&self, destination: &str, message: &Message) -> Result<Admission, Error> {
        let channel = uri::without_key(destination);
        if let Some(detail) = self.bounds.exceeded(message) {
            return self
                .aside(destination, message, None, Reason::Malformed, detail, None)
                .await;
        }
        let row = match intake::row_of(destination, message) {
            Ok(row) => row,
            Err(detail) => {
                return self
                    .aside(destination, message, None, Reason::Malformed, detail, None)
                    .await;
            }
        };
        if !(self.admission)(channel, &row.stream_type) {
            let detail = "the owner of its stream type does not produce this channel";
            return self
                .aside(
                    destination,
                    message,
                    Some(&row),
                    Reason::Foreign,
                    detail,
                    None,
                )
                .await;
        }
        let received = |receipt, held_by| {
            self.observer.on_received(&Received {
                message: &row,
                receipt,
                held_by,
            });
        };
        match self.admit(&row).await? {
            Admitted::Stored(receipt) => {
                received(Some(receipt), None);
                Ok(Admission::Stored(receipt))
            }
            Admitted::Redelivered => {
                received(None, row.message_id());
                Ok(Admission::Duplicate)
            }
            Admitted::Shadowed(held_by) => {
                received(None, held_by.as_deref());
                Ok(Admission::Duplicate)
            }
            Admitted::Differs => {
                let detail = "its identity is held with its message_id and another payload";
                self.aside(
                    destination,
                    message,
                    Some(&row),
                    Reason::Differs,
                    detail,
                    None,
                )
                .await
            }
            Admitted::IdentityTaken {
                channel,
                received_position,
            } => {
                let detail = "its message_id is held at another identity of its stream type";
                self.aside(
                    destination,
                    message,
                    Some(&row),
                    Reason::IdentityTaken,
                    detail,
                    Some((&channel, received_position)),
                )
                .await
            }
            Admitted::Unstorable(detail) => {
                self.aside(
                    destination,
                    message,
                    Some(&row),
                    Reason::Unstorable,
                    detail,
                    None,
                )
                .await
            }
        }
    }

    /// Sets `message` aside, then tells the observer, once the row commits.
    /// The log names the channel, the reason and the digest, in the
    /// library's words: never a header's value, a stream id or a
    /// `message_id`, which may be personal data or the very value refused.
    async fn aside(
        &self,
        destination: &str,
        message: &Message,
        row: Option<&InboxMessage>,
        reason: Reason,
        detail: &'static str,
        holder: Option<(&str, i64)>,
    ) -> Result<Admission, Error> {
        // A message past its bounds is kept cut, never copied whole.
        let frame = match self.bounds.exceeded(message) {
            Some(_) => intake::cut_frame(destination, message),
            None => intake::frame(destination, message),
        };
        let digest = intake::digest(&frame);
        let channel = intake::sanitized(
            Some(uri::without_key(destination).as_bytes()),
            intake::CHANNEL_WIDTH,
        )
        .unwrap_or_default();
        let tenant_id = intake::sanitized(message.header(TENANT_ID), intake::DERIVED_WIDTH);
        let stream_type = intake::sanitized(message.header(STREAM_TYPE), intake::DERIVED_WIDTH);
        let identity = row.map(intake::identity_digest);
        let repeat = self
            .set_aside(&Aside {
                digest: &digest,
                channel: &channel,
                tenant_id: tenant_id.as_deref(),
                stream_type: stream_type.as_deref(),
                identity: identity.as_deref(),
                reason: reason.as_str(),
                detail,
                holder_channel: holder.map(|(channel, _)| channel),
                holder_position: holder.map(|(_, position)| position),
                message: &frame,
            })
            .await?;
        log::error!(
            "inbox: a message of {channel} was set aside ({}: {detail}), digest {digest}{}",
            reason.as_str(),
            if repeat { ", a repeat" } else { "" }
        );
        self.observer.on_rejected(&Rejected {
            channel: &channel,
            tenant_id: tenant_id.as_deref(),
            stream_type: stream_type.as_deref(),
            reason,
            digest: &digest,
            repeat,
        });
        Ok(Admission::SetAside {
            digest,
            reason,
            repeat,
        })
    }
}
