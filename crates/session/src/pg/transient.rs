//! Which errors of the database are of the moment, and which are defects.
//!
//! A loop that meets an error of the moment — a lock cycle the server broke,
//! a serialization failure, a connection lost, a server shutting down or out
//! of resources — waits and goes on: its transaction was rolled back, nothing
//! was recorded, and the work comes back next time. Any other error is a
//! defect — of the schema, of a statement, of a value — and repeating it
//! would repeat the defect; a loop stops on it. The line is drawn by SQLSTATE
//! class (PostgreSQL manual, appendix A). An error without a SQLSTATE is of
//! the moment when it is the connection's, and a defect when it is the
//! driver's reading of a value, such as a column of an unexpected type.

use std::error::Error as _;

use tokio_postgres::error::SqlState;

use crate::SessionError;

/// Whether an error of the driver is of the moment.
pub fn transient(error: &tokio_postgres::Error) -> bool {
    if error.is_closed() {
        return true;
    }
    if let Some(code) = error.code() {
        return transient_state(code);
    }
    // No SQLSTATE: the connection, the network, or the driver reading a value.
    // The driver keeps the kind of its errors to itself; the connection's
    // carry an I/O error, or say so in their text.
    let text = error.to_string();
    error
        .source()
        .is_some_and(|source| source.is::<std::io::Error>())
        || text.starts_with("timeout waiting for server")
        || text.starts_with("error connecting to server")
}

/// The SQLSTATE classes and codes of the moment: connection exceptions
/// (`08`), insufficient resources (`53`), serialization failure and deadlock
/// detected (`40001`, `40P01`), statement completion unknown (`40003`), and
/// the server going down or not yet up (`57P01`, `57P02`, `57P03`).
pub fn transient_state(code: &SqlState) -> bool {
    let code = code.code();
    matches!(
        code,
        "40001" | "40P01" | "40003" | "57P01" | "57P02" | "57P03"
    ) || code.starts_with("08")
        || code.starts_with("53")
}

/// Whether an error of a session is of the moment: a connection that could
/// not be taken from the pool is, and so is a scope boundary the driver
/// failed on for a reason of the moment.
pub fn transient_session(error: &SessionError) -> bool {
    match error {
        SessionError::Acquire(_) => true,
        SessionError::Begin(error)
        | SessionError::Commit(error)
        | SessionError::Rollback(error) => error
            .downcast_ref::<tokio_postgres::Error>()
            .is_some_and(transient),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_moment_is_told_from_the_defect_by_sqlstate() {
        for (code, moment) in [
            ("40P01", true),  // deadlock detected
            ("40001", true),  // serialization failure
            ("08006", true),  // connection failure
            ("53300", true),  // too many connections
            ("57P01", true),  // admin shutdown
            ("42P01", false), // undefined table
            ("23505", false), // unique violation
            ("22P02", false), // invalid text representation
            ("0A000", false), // feature not supported
        ] {
            assert_eq!(
                transient_state(&SqlState::from_code(code)),
                moment,
                "{code}"
            );
        }
    }

    #[test]
    fn a_connection_that_cannot_be_taken_is_of_the_moment() {
        assert!(transient_session(&SessionError::Acquire(
            "pool timed out".into()
        )));
        assert!(!transient_session(&SessionError::ScopeAlreadyOpen));
        assert!(!transient_session(&SessionError::Begin(
            "not the driver's".into()
        )));
    }
}
