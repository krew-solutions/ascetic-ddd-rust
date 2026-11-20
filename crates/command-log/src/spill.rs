//! A pass's spill directory, and the reading of a run's records (ADR-0046
//! decision 15).
//!
//! Each pass — the engine's, the validator's — spills into a directory of its
//! own under the caller's, claimed under a lock and removed on every exit,
//! unwinding included. A pass killed outright (by oomd, a reboot) leaves its
//! directory behind, but not its lock: the next pass to start under the same
//! parent sweeps it, and says how much it freed.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

/// The parent's lock, held while a pass sweeps and claims: two passes starting
/// at once never sweep each other.
const SWEEP: &str = ".sweep.lock";

/// A pass's own spill directory.
#[derive(Debug)]
pub(crate) struct SpillDir {
    path: PathBuf,
    lock: PathBuf,
    /// Held for the pass's life: the sweep of another pass leaves it alone.
    _held: File,
}

impl SpillDir {
    /// Claims `parent/pass-N` for this pass, after removing every pass
    /// directory under `parent` whose lock no live pass holds.
    pub(crate) fn new(parent: &Path) -> io::Result<SpillDir> {
        fs::create_dir_all(parent)?;
        let sweep = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(parent.join(SWEEP))?;
        sweep.lock()?;
        sweep_orphans(parent)?;
        let mut n = 0u64;
        loop {
            let lock = parent.join(format!("pass-{n}.lock"));
            match OpenOptions::new().write(true).create_new(true).open(&lock) {
                Ok(held) => {
                    held.lock()?;
                    let path = parent.join(format!("pass-{n}"));
                    fs::create_dir(&path)?;
                    return Ok(SpillDir {
                        path,
                        lock,
                        _held: held,
                    });
                }
                Err(e) if e.kind() == io::ErrorKind::AlreadyExists => n = n.saturating_add(1),
                Err(e) => return Err(e),
            }
        }
    }

    /// Where the pass writes its runs.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for SpillDir {
    fn drop(&mut self) {
        // The directory first: a lock file whose directory is gone is a
        // finished pass, a directory whose lock file is gone never is.
        let _ = fs::remove_dir_all(&self.path);
        let _ = fs::remove_file(&self.lock);
    }
}

/// Removes the pass directories under `parent` no live pass holds — a lock
/// file nobody has locked, or a directory with no lock file — and returns the
/// bytes they held.
fn sweep_orphans(parent: &Path) -> io::Result<u64> {
    let mut freed = 0u64;
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else {
            continue;
        };
        if let Some(pass) = name
            .strip_suffix(".lock")
            .filter(|p| p.starts_with("pass-"))
        {
            let orphan = File::open(entry.path()).is_ok_and(|f| f.try_lock().is_ok());
            if orphan {
                let dir = parent.join(pass);
                freed = freed.saturating_add(size(&dir));
                let _ = fs::remove_dir_all(&dir);
                let _ = fs::remove_file(entry.path());
            }
        } else if name.starts_with("pass-")
            && entry.file_type()?.is_dir()
            && !parent.join(format!("{name}.lock")).exists()
        {
            freed = freed.saturating_add(size(&entry.path()));
            let _ = fs::remove_dir_all(entry.path());
        }
    }
    Ok(freed)
}

fn size(dir: &Path) -> u64 {
    fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(Result::ok)
                .filter_map(|e| e.metadata().ok())
                .map(|m| m.len())
                .fold(0u64, u64::saturating_add)
        })
        .unwrap_or(0)
}

/// The first byte of a record, or `None` at the clean end of a run. Only a
/// zero-length read is an end: a cut record, or a cut compressed frame, is an
/// error, never fewer records.
pub(crate) fn first_byte(r: &mut impl Read) -> io::Result<Option<u8>> {
    let mut b = [0u8; 1];
    loop {
        match r.read(&mut b) {
            Ok(0) => return Ok(None),
            Ok(_) => return Ok(b.first().copied()),
            Err(e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
}

/// A run's reader that holds it to its count: a run that ends before its last
/// record, or goes on past it, is an error.
pub(crate) struct Counted<R> {
    pub(crate) reader: R,
    pub(crate) left: u64,
    pub(crate) path: PathBuf,
}

impl<R: Read> Counted<R> {
    /// Whether another record is due; at the last, the run must end there.
    pub(crate) fn due(&mut self) -> io::Result<bool> {
        if self.left > 0 {
            return Ok(true);
        }
        match first_byte(&mut self.reader)? {
            None => Ok(false),
            Some(_) => Err(io::Error::other(format!(
                "{}: a run longer than written",
                self.path.display()
            ))),
        }
    }

    /// Reads a record's bytes, its first byte read as a record's, not an end.
    pub(crate) fn exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.reader.read_exact(buf).map_err(|e| self.cut(e))
    }

    /// The error for a run cut short.
    pub(crate) fn cut(&self, e: io::Error) -> io::Error {
        io::Error::new(
            e.kind(),
            format!("{}: a run cut short: {e}", self.path.display()),
        )
    }
}
