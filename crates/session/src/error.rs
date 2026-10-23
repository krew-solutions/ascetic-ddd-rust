//! Errors of the session lifecycle.

use std::fmt;

/// Boxed error reported by the underlying driver.
pub type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A failure of the session machinery itself, as opposed to a failure of the
/// work done inside a scope.
///
/// Domain errors never become a `SessionError`: a scope returns its own error
/// type, and the session only requires that it can carry a `SessionError`
/// (`E: From<SessionError>`), because opening or closing a scope can fail on
/// its own.
///
/// Whether another attempt may succeed is [`SessionError::is_transient`],
/// answered without the driver: the adapter that met the failure classifies it
/// where it can see the driver's error, and the verdict travels with it — so
/// that an application layer which never names a driver can tell a moment from
/// a defect (ADR-0028).
#[derive(Debug)]
#[non_exhaustive]
pub enum SessionError {
    /// A connection could not be taken from the pool. Of the moment.
    Acquire(BoxError),
    /// `BEGIN` or `SAVEPOINT` failed.
    Begin {
        /// The driver's error.
        error: BoxError,
        /// Whether another attempt may succeed, as the adapter classified it.
        transient: bool,
    },
    /// `COMMIT` or `RELEASE SAVEPOINT` failed for a reason of its own — the
    /// server refused the commit (a deferred constraint, a serialization
    /// failure), or the connection was lost on the way. The scope was rolled
    /// back; nothing it did is durable.
    Commit {
        /// The driver's error.
        error: BoxError,
        /// Whether another attempt may succeed, as the adapter classified it.
        transient: bool,
    },
    /// `ROLLBACK` or `ROLLBACK TO SAVEPOINT` failed.
    Rollback {
        /// The driver's error.
        error: BoxError,
        /// Whether another attempt may succeed, as the adapter classified it.
        transient: bool,
    },
    /// The scope returned success, but a statement inside it had failed and
    /// the failure was absorbed: the server aborts a transaction on the first
    /// failed statement, so there was nothing left to commit. The scope was
    /// rolled back — the whole transaction, or a nested scope back to its
    /// savepoint, the enclosing transaction going on — and nothing it did is
    /// durable (ADR-0028).
    ///
    /// Always a defect of the code that absorbed the failure, whatever the
    /// failure was: a refusal meant to be absorbed is absorbed in a nested
    /// scope of its own, whose rollback undoes that statement alone.
    Aborted {
        /// The failure that aborted the transaction, when the adapter saw it.
        cause: Option<AbortCause>,
    },

    /// A second scope was opened on a session that already has one open.
    ///
    /// Nesting is expressed by opening a scope on the session the previous
    /// scope handed out, not on the one that opened it. Two scopes living
    /// side by side on the same connection would share one savepoint stack:
    /// releasing the older one silently destroys the newer, and the newer then
    /// fails to release with a driver error that says nothing about the cause.
    /// Since the session is shared by `&`, the compiler cannot rule this out —
    /// so it is refused at run time instead.
    ScopeAlreadyOpen,
    /// A scope's future was dropped before the scope closed — a timeout, the
    /// losing branch of a `select!` — so its transaction was never committed
    /// or rolled back; or a rollback failed, leaving the connection in a state
    /// nobody knows. The session is poisoned: an enclosing scope is refused at
    /// commit and rolled back, a handle carried out of the dropped scope
    /// refuses new scopes, and the connection is discarded rather than
    /// returned to the pool.
    Abandoned,
}

impl SessionError {
    /// Whether another attempt may succeed: a connection the pool could not
    /// hand out, or a scope boundary that failed for a reason of the moment.
    /// An absorbed failure ([`SessionError::Aborted`]) never is, whatever it
    /// was: retrying runs the same code, which absorbs it again.
    #[must_use]
    pub fn is_transient(&self) -> bool {
        match self {
            SessionError::Acquire(_) => true,
            SessionError::Begin { transient, .. }
            | SessionError::Commit { transient, .. }
            | SessionError::Rollback { transient, .. } => *transient,
            SessionError::Aborted { .. }
            | SessionError::ScopeAlreadyOpen
            | SessionError::Abandoned => false,
        }
    }
}

/// The statement failure that aborted a transaction, as the server reported
/// it: what an absorbed failure was, so that its defect can be found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AbortCause {
    /// The SQLSTATE, `23505` for a unique violation.
    pub sqlstate: String,
    /// The server's primary message.
    pub message: String,
    /// The constraint the statement violated, when it was one.
    pub constraint: Option<String>,
}

impl fmt::Display for AbortCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} ({})", self.message, self.sqlstate)?;
        if let Some(constraint) = &self.constraint {
            write!(f, " on `{constraint}`")?;
        }
        Ok(())
    }
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SessionError::Acquire(error) => write!(f, "cannot acquire a connection: {error}"),
            SessionError::Begin { error, .. } => write!(f, "cannot open a scope: {error}"),
            SessionError::Commit { error, .. } => write!(f, "cannot commit a scope: {error}"),
            SessionError::Rollback { error, .. } => write!(f, "cannot roll a scope back: {error}"),
            SessionError::Aborted { cause } => {
                f.write_str(
                    "a statement failed inside the scope and the scope still reported \
                     success: the transaction was aborted and rolled back, nothing is durable",
                )?;
                match cause {
                    Some(cause) => write!(f, "; the failure absorbed: {cause}"),
                    None => Ok(()),
                }
            }
            SessionError::ScopeAlreadyOpen => write!(
                f,
                "a scope is already open on this session: open a nested scope on \
                 the session the outer scope handed out, or use another session",
            ),
            SessionError::Abandoned => write!(
                f,
                "a scope was dropped before it closed: its transaction is abandoned \
                 and the connection is discarded",
            ),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SessionError::Acquire(error)
            | SessionError::Begin { error, .. }
            | SessionError::Commit { error, .. }
            | SessionError::Rollback { error, .. } => Some(&**error),
            SessionError::Aborted { .. }
            | SessionError::ScopeAlreadyOpen
            | SessionError::Abandoned => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boundary(transient: bool) -> [SessionError; 3] {
        [
            SessionError::Begin {
                error: "x".into(),
                transient,
            },
            SessionError::Commit {
                error: "x".into(),
                transient,
            },
            SessionError::Rollback {
                error: "x".into(),
                transient,
            },
        ]
    }

    #[test]
    fn the_moment_is_the_adapter_s_verdict_and_an_absorbed_failure_is_never_one() {
        assert!(SessionError::Acquire("pool timed out".into()).is_transient());
        assert!(boundary(true).iter().all(SessionError::is_transient));
        assert!(!boundary(false).iter().any(SessionError::is_transient));
        let deadlock = AbortCause {
            sqlstate: "40P01".to_owned(),
            message: "deadlock detected".to_owned(),
            constraint: None,
        };
        for error in [
            SessionError::Aborted {
                cause: Some(deadlock),
            },
            SessionError::Aborted { cause: None },
            SessionError::ScopeAlreadyOpen,
            SessionError::Abandoned,
        ] {
            assert!(!error.is_transient(), "{error}");
        }
    }

    #[test]
    fn an_aborted_scope_names_the_failure_it_absorbed() {
        let error = SessionError::Aborted {
            cause: Some(AbortCause {
                sqlstate: "23505".to_owned(),
                message: "duplicate key value violates unique constraint \"held_once\"".to_owned(),
                constraint: Some("held_once".to_owned()),
            }),
        };
        let text = error.to_string();
        assert!(text.contains("nothing is durable"), "{text}");
        assert!(text.ends_with("(23505) on `held_once`"), "{text}");
        assert!(
            SessionError::Aborted { cause: None }
                .to_string()
                .ends_with("nothing is durable")
        );
    }
}
