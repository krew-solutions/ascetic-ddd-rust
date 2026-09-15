//! What a statement could see.

use std::fmt;
use std::str::FromStr;

/// A PostgreSQL snapshot, `pg_current_snapshot()`, as the statement that
/// read a row ran under it: which transactions it could see.
///
/// A transaction is visible when its id is below `xmin`, or below `xmax` and
/// not among those in progress. The observer reports the snapshot of every
/// statement that walks the table, so that a recorded run says which stored
/// rows each walk could see, whatever order the events were logged in.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    /// Every transaction below this id had ended.
    pub xmin: u64,
    /// No transaction at or above this id had started.
    pub xmax: u64,
    /// Transactions between the two that were still in progress.
    pub in_progress: Vec<u64>,
}

impl Snapshot {
    /// Whether the statement could see what transaction `xid` committed.
    pub fn sees(&self, xid: u64) -> bool {
        xid < self.xmin || (xid < self.xmax && !self.in_progress.contains(&xid))
    }
}

/// `xmin:xmax:xip1,xip2`, the text of `pg_snapshot`.
impl FromStr for Snapshot {
    type Err = MalformedSnapshot;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let malformed = || MalformedSnapshot(text.to_owned());
        let mut parts = text.split(':');
        let xmin = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(malformed)?;
        let xmax = parts
            .next()
            .and_then(|p| p.parse().ok())
            .ok_or_else(malformed)?;
        let in_progress = parts
            .next()
            .ok_or_else(malformed)?
            .split(',')
            .filter(|p| !p.is_empty())
            .map(|p| p.parse().map_err(|_| malformed()))
            .collect::<Result<_, _>>()?;
        if parts.next().is_some() {
            return Err(malformed());
        }
        Ok(Snapshot {
            xmin,
            xmax,
            in_progress,
        })
    }
}

/// The text was not a `pg_snapshot`.
#[derive(Debug)]
pub struct MalformedSnapshot(String);

impl fmt::Display for MalformedSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "not a snapshot: `{}`", self.0)
    }
}

impl std::error::Error for MalformedSnapshot {}

#[cfg(test)]
mod tests {
    use super::Snapshot;

    #[test]
    fn a_snapshot_is_parsed_from_its_text() {
        let snapshot: Snapshot = "10:20:12,15".parse().unwrap();
        assert_eq!(
            snapshot,
            Snapshot {
                xmin: 10,
                xmax: 20,
                in_progress: vec![12, 15]
            }
        );
        assert_eq!(
            "10:10:".parse::<Snapshot>().unwrap().in_progress,
            Vec::<u64>::new()
        );
        assert!("10:20".parse::<Snapshot>().is_err());
        assert!("a:b:".parse::<Snapshot>().is_err());
    }

    #[test]
    fn it_sees_what_had_ended_and_not_what_was_in_progress_or_not_yet_started() {
        let snapshot: Snapshot = "10:20:12,15".parse().unwrap();
        assert!(snapshot.sees(9), "ended before xmin");
        assert!(snapshot.sees(11), "between, not in progress");
        assert!(!snapshot.sees(12), "in progress");
        assert!(snapshot.sees(19));
        assert!(!snapshot.sees(20), "not yet started");
        assert!(!snapshot.sees(21));
    }
}
