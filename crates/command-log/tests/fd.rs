//! The FD executor of ADR-0046 (decision 15): under a soft limit of 64 open
//! files, a generation and a validation of thousands of runs still finish —
//! their merges are as wide as the limit allows. A binary of its own: the limit
//! is the whole process's.

mod fixture;

use std::io::Cursor;

use ascetic_ddd_command_log::Reader;
use ascetic_ddd_command_log::engine::{Config, EngineError, Unit, generate};
use ascetic_ddd_command_log::record::Record;
use ascetic_ddd_command_log::validate::{NoHost, validate};
use fixture::*;

struct Many;

impl Unit for Many {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for i in 0..3_000 {
            out(step(id("device", i, "seen", 0), i))?;
        }
        Ok(())
    }
}

#[test]
fn thousands_of_runs_merge_under_sixty_four_open_files() {
    use rustix::process::{Resource, Rlimit, getrlimit, setrlimit};
    let hard = getrlimit(Resource::Nofile).maximum;
    setrlimit(
        Resource::Nofile,
        Rlimit {
            current: Some(64),
            maximum: hard,
        },
    )
    .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let config = Config {
        threads: 1,
        budget: 1,
        cell: u64::MAX,
        cells: 1,
        spill: dir.path().to_owned(),
        fan_in: 0,
        memory_spill: true,
        progress: None,
        pipe_blocks: 64,
    };
    let units: [&dyn Unit; 1] = [&Many];
    let (log, trailer) = generate(header(), &units, &config, Vec::new()).unwrap();
    assert_eq!(trailer.records, 3_000);
    let limits = ascetic_ddd_command_log::validate::Limits {
        ids_in_memory: 1,
        ..limits_in(dir.path())
    };
    let report = validate(Reader::new(Cursor::new(&log)).unwrap(), &NoHost, &limits).unwrap();
    assert!(report.violations.is_empty());
}
