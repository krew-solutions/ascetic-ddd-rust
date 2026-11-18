//! What can go wrong on the bus.

use std::fmt;

/// An error of a transport, boxed: the bus does not name its drivers.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// Errors of the bus and its adapters.
#[derive(Debug)]
pub enum Error {
    /// The URI's scheme has no registered adapter, or the URI has no scheme.
    UnknownScheme(String),
    /// The scheme is already bound to an adapter.
    AlreadyRegistered(String),
    /// A second consumer joined the same `(uri, group)` on an adapter that
    /// allows one — a configuration bug in a monolithic deployment.
    AlreadyInGroup {
        /// The topic.
        uri: String,
        /// The consumer group.
        group: String,
    },
    /// The transport failed.
    Transport(BoxError),
    /// A stage of the wire refused the message on its way out.
    Stage(BoxError),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::UnknownScheme(scheme) => {
                write!(f, "no adapter registered for scheme `{scheme}`")
            }
            Error::AlreadyRegistered(scheme) => {
                write!(f, "scheme `{scheme}` is already registered")
            }
            Error::AlreadyInGroup { uri, group } => {
                write!(f, "a consumer already exists in group `{group}` on `{uri}`")
            }
            Error::Transport(error) => write!(f, "transport: {error}"),
            Error::Stage(error) => write!(f, "stage: {error}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::Transport(error) | Error::Stage(error) => Some(error.as_ref()),
            _ => None,
        }
    }
}

/// Why a message could not be handled, by name (ADR-0040 decision 2): a
/// name of `[a-z0-9_.]{1,64}`, such as `kms.gone`, never a message's text,
/// so that it is safe as a column and as a metric label. [`cause!`](crate::cause!) builds
/// one from a literal and refuses an invalid name at compile time;
/// [`Cause::new`] checks one at run time.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Cause(&'static str);

impl Cause {
    /// The cause of a failure no one named: a plain error.
    pub const UNEXPLAINED: Cause = Cause("bus.unexplained");

    /// `name` as a cause, or none when it is not `[a-z0-9_.]{1,64}`.
    #[must_use]
    pub const fn new(name: &'static str) -> Option<Cause> {
        let bytes = name.as_bytes();
        if bytes.is_empty() || bytes.len() > 64 {
            return None;
        }
        let mut at = 0;
        while at < bytes.len() {
            let byte = bytes[at];
            if !(byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'.')
            {
                return None;
            }
            at += 1;
        }
        Some(Cause(name))
    }

    /// The name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        self.0
    }
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

/// A [`Cause`] from a literal, checked at compile time:
///
/// ```
/// const GONE: ascetic_ddd_bus::Cause = ascetic_ddd_bus::cause!("kms.gone");
/// assert_eq!(GONE.as_str(), "kms.gone");
/// ```
///
/// A name that is not `[a-z0-9_.]{1,64}` does not compile:
///
/// ```compile_fail,E0080
/// let _ = ascetic_ddd_bus::cause!("KMS gone");
/// ```
#[macro_export]
macro_rules! cause {
    ($name:expr) => {
        const {
            match $crate::Cause::new($name) {
                Some(cause) => cause,
                None => panic!("a cause is named by [a-z0-9_.]{{1,64}}"),
            }
        }
    };
}

/// A failure of handling that no retry will mend — a message that cannot
/// be opened, a key that is gone — as opposed to one of the moment. The bus
/// knows no verdicts of its own; it carries this one from whoever can tell,
/// a stage or a handler, to a transport that can act on it: the inbox parks
/// such a message at once instead of retrying it (ADR-0010), under its
/// cause (ADR-0040).
#[derive(Debug)]
pub struct Permanent {
    cause: Cause,
    error: BoxError,
}

impl Permanent {
    /// Marks `error` as permanent, for no named cause.
    pub fn new(error: impl Into<BoxError>) -> Self {
        Permanent::because(Cause::UNEXPLAINED, error)
    }

    /// Marks `error` as permanent, for `cause`.
    pub fn because(cause: Cause, error: impl Into<BoxError>) -> Self {
        Permanent {
            cause,
            error: error.into(),
        }
    }

    /// Why.
    #[must_use]
    pub fn cause(&self) -> Cause {
        self.cause
    }

    /// The error itself.
    pub fn into_inner(self) -> BoxError {
        self.error
    }
}

impl fmt::Display for Permanent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "permanent: {}", self.error)
    }
}

impl std::error::Error for Permanent {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

/// A failure of the moment, under its cause: tried again after the
/// transport's back-off, an attempt spent, and parked under its cause when
/// the attempts run out (ADR-0040 decision 2). A plain error is the same
/// failure without a named cause.
#[derive(Debug)]
pub struct Transient {
    cause: Cause,
    error: BoxError,
}

impl Transient {
    /// Marks `error` as of the moment, for `cause`.
    pub fn because(cause: Cause, error: impl Into<BoxError>) -> Self {
        Transient {
            cause,
            error: error.into(),
        }
    }

    /// Why.
    #[must_use]
    pub fn cause(&self) -> Cause {
        self.cause
    }

    /// The error itself.
    pub fn into_inner(self) -> BoxError {
        self.error
    }
}

impl fmt::Display for Transient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.error, f)
    }
}

impl std::error::Error for Transient {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

/// Whose messages an [`Unavailable`] stops (ADR-0040 decision 3).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Scope {
    /// No message of the channel can be handled now: the stage is down. The
    /// inbox holds the message without spending an attempt.
    Stage,
    /// Only the messages of this message's Tenant cannot: the inbox spends an
    /// attempt, as on a failure of the moment, never sooner than
    /// `retry_after`.
    Tenant,
}

/// The message cannot be handled now, for a cause that is not its own — a
/// KMS down, a Tenant's key throttled — as told by whoever can tell, with
/// whose messages it stops and when to ask again (ADR-0040 decision 3).
#[derive(Debug)]
pub struct Unavailable {
    cause: Cause,
    scope: Scope,
    retry_after: Option<std::time::Duration>,
    by_stage: bool,
    error: BoxError,
}

impl Unavailable {
    /// `error`, unavailable for `cause` over `scope`, to be asked again past
    /// `retry_after` when the teller knows.
    pub fn new(
        cause: Cause,
        scope: Scope,
        retry_after: Option<std::time::Duration>,
        error: impl Into<BoxError>,
    ) -> Self {
        Unavailable {
            cause,
            scope,
            retry_after,
            by_stage: false,
            error: error.into(),
        }
    }

    /// Whether a stage told it, on a message's way in, rather than the
    /// handler: the inbox weighs a stage's refusal against the stage's own
    /// dependency (seedwork ADR-0042 decision 1).
    #[must_use]
    pub fn told_by_stage(&self) -> bool {
        self.by_stage
    }

    /// Marks it told by a stage.
    pub(crate) fn told_by_a_stage(&mut self) {
        self.by_stage = true;
    }

    /// Why.
    #[must_use]
    pub fn cause(&self) -> Cause {
        self.cause
    }

    /// Whose messages it stops.
    #[must_use]
    pub fn scope(&self) -> Scope {
        self.scope
    }

    /// When to ask again, when the teller knows.
    #[must_use]
    pub fn retry_after(&self) -> Option<std::time::Duration> {
        self.retry_after
    }

    /// The error itself.
    pub fn into_inner(self) -> BoxError {
        self.error
    }
}

impl fmt::Display for Unavailable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unavailable: {}", self.error)
    }
}

impl std::error::Error for Unavailable {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.error.as_ref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_cause_is_a_short_lowercase_name_never_text() {
        assert_eq!(Cause::new("kms.gone").map(Cause::as_str), Some("kms.gone"));
        assert_eq!(
            Cause::new("bus.kms_unavailable_2").map(Cause::as_str),
            Some("bus.kms_unavailable_2")
        );
        for refused in ["", "KMS", "kms gone", "kms-gone", "kms/gone", "ключ"] {
            assert_eq!(Cause::new(refused), None, "{refused:?}");
        }
        let longest = "a".repeat(64).leak();
        assert!(Cause::new(longest).is_some());
        let longer = "a".repeat(65).leak();
        assert_eq!(Cause::new(longer), None);
        assert_eq!(cause!("kms.gone"), Cause::new("kms.gone").unwrap());
        assert_eq!(Permanent::new("x").cause(), Cause::UNEXPLAINED);
    }
}
