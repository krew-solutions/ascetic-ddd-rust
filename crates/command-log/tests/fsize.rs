//! The partial-write executor of ADR-0046 (decision 15): a log whose write
//! fails partway — past the process's file-size limit — leaves no file, no
//! partial file and no spill behind, and says so. A binary of its own: the
//! limit is the whole process's.

mod fixture;

use ascetic_ddd_command_log::Compression;
use ascetic_ddd_command_log::engine::{Config, EngineError, Unit, generate_file};
use ascetic_ddd_command_log::record::Record;
use fixture::*;

struct Many;

impl Unit for Many {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for i in 0..2_000 {
            out(step(id("device", i, "seen", 0), i))?;
        }
        Ok(())
    }
}

#[test]
fn a_write_failing_partway_leaves_nothing() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    // SAFETY: the test's process alone; the signal is ignored before any file
    // grows past the limit, so a write fails with EFBIG instead.
    unsafe {
        libc::signal(libc::SIGXFSZ, libc::SIG_IGN);
    }
    let hard = getrlimit(Resource::Fsize).maximum;
    setrlimit(
        Resource::Fsize,
        Rlimit {
            current: Some(64 << 10),
            maximum: hard,
        },
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let (spill, log) = (dir.path().join("spill"), dir.path().join("plan.jsonl"));
    let config = Config {
        threads: 2,
        budget: 8 << 10,
        cell: 500,
        cells: 4,
        spill: spill.clone(),
        fan_in: 0,
        memory_spill: true,
        progress: None,
        pipe_blocks: 64,
    };
    let units: [&dyn Unit; 1] = [&Many];
    let error = generate_file(&log, header(), &units, &config, Compression::None).unwrap_err();
    let efbig = |e: &std::io::Error| e.raw_os_error() == Some(libc::EFBIG);
    let named = match &error {
        EngineError::Log(ascetic_ddd_command_log::LogError::Io(e)) => efbig(e),
        EngineError::Io { error, .. } => efbig(error),
        _ => false,
    };
    assert!(named, "{error:?}");
    let mut left: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    left.sort();
    assert_eq!(left, vec!["spill"], "no log, no partial log");
    assert!(leftover(&spill).is_empty(), "{:?}", leftover(&spill));
}
