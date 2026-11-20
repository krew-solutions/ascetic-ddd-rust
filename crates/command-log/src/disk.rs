//! What a generation asks of the machine before it commits hours to a pass
//! (ADR-0046 decision 15): free space, which filesystem a path is on, whether
//! it is memory, how many files a merge may hold open.
//!
//! A port, so a test can be short of one byte without filling a disk.

use std::io;
use std::path::Path;

/// The machine's disks, as a generation sees them.
pub trait Disk: Send + Sync {
    /// Bytes an unprivileged writer may still write on `path`'s filesystem.
    fn free(&self, path: &Path) -> io::Result<u64>;
    /// An identity of `path`'s filesystem: two paths on one share it.
    fn device(&self, path: &Path) -> io::Result<u64>;
    /// Whether `path`'s filesystem is memory (tmpfs, ramfs).
    fn in_memory(&self, path: &Path) -> io::Result<bool>;
    /// The files this process may hold open: its soft limit.
    fn open_files(&self) -> io::Result<u64>;
}

/// The local machine, through `rustix`.
#[derive(Clone, Copy, Debug, Default)]
pub struct LocalDisk;

/// `TMPFS_MAGIC` and `RAMFS_MAGIC` of Linux's `statfs`.
const MEMORY: [i64; 2] = [0x0102_1994, 0x8584_58f6];

impl Disk for LocalDisk {
    fn free(&self, path: &Path) -> io::Result<u64> {
        let s = rustix::fs::statvfs(path)?;
        Ok(s.f_bavail.saturating_mul(s.f_frsize))
    }

    fn device(&self, path: &Path) -> io::Result<u64> {
        Ok(rustix::fs::stat(path)?.st_dev)
    }

    #[allow(
        clippy::useless_conversion,
        reason = "f_type is an i64 on x86_64 Linux and narrower on other targets"
    )]
    fn in_memory(&self, path: &Path) -> io::Result<bool> {
        let s = rustix::fs::statfs(path)?;
        Ok(MEMORY.contains(&i64::from(s.f_type)))
    }

    fn open_files(&self) -> io::Result<u64> {
        let limit = rustix::process::getrlimit(rustix::process::Resource::Nofile);
        Ok(limit.current.unwrap_or(u64::MAX))
    }
}

/// The widest merge the open-file limit allows: a quarter of it, the rest left
/// to the host and the other pass, between 2 and 256.
pub fn fan_in(open_files: u64) -> usize {
    usize::try_from((open_files / 4).clamp(2, 256)).unwrap_or(256)
}
