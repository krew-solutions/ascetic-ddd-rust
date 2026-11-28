//! M7 of ADR-0046: a log of 10⁷ steps generated on four threads through
//! spills, in release, under `/usr/bin/time -v`:
//!
//! ```text
//! cargo test --release -p ascetic-ddd-command-log --test scale -- --ignored --nocapture
//! ```
//!
//! It prints the log's bytes; wall time and peak memory are `time`'s, peak disk
//! a sampler's beside it, the bytes a step by kind a reading of the file after.

mod fixture;

use std::fs::File;
use std::io::BufWriter;

use ascetic_ddd_command_log::engine::{Config, EngineError, Unit, generate};
use ascetic_ddd_command_log::record::{On, Record};
use ascetic_ddd_faker::{Key, Seed};
use fixture::*;

const DAY: u64 = 86_400_000_000;

/// Devices `first..first + count`: managed at a time of the day drawn from the
/// key, then three facts an hour apart on a gapless stream.
struct Devices {
    first: u64,
    count: u64,
    /// The days the managed steps are spread over.
    days: u64,
}

impl Unit for Devices {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for d in self.first..self.first + self.count {
            let key = Key::root(Seed(7)).child("device").at(d);
            let at = key.child("managed").unit().raw() % (DAY * self.days);
            let managed = step(id("device", d, "managed", 0), at);
            let mut before = managed.step.clone();
            out(managed)?;
            let mut t = at;
            for n in 0..3u32 {
                t += 3_600_000_000 + key.child("fact").at(u64::from(n)).unit().raw() % 60_000_000;
                let fact = after(
                    message(step(id("device", d, "fact", n), t), "lifecycle", n + 1),
                    &before,
                    On::Expected,
                );
                before = fact.step.clone();
                out(fact)?;
            }
        }
        Ok(())
    }
}

#[test]
#[ignore = "M7: ten million steps, in release, under /usr/bin/time -v"]
fn m7_ten_million_steps() {
    let base = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/m7"));
    let _ = std::fs::remove_dir_all(base);
    let spill = base.join("spill");
    std::fs::create_dir_all(&spill).unwrap();
    let devices = 2_500_000u64;
    let units: Vec<Devices> = (0..1_000)
        .map(|i| Devices {
            first: i * 2_500,
            count: 2_500,
            days: 1,
        })
        .collect();
    let units: Vec<&dyn Unit> = units.iter().map(|u| u as &dyn Unit).collect();
    let config = Config {
        threads: 4,
        budget: 64 << 20,
        cell: DAY / 64,
        cells: 128,
        spill,
        fan_in: 0,
        memory_spill: true,
        progress: None,
        pipe_blocks: 64,
    };
    let path = base.join("log.jsonl");
    let out = BufWriter::with_capacity(1 << 20, File::create(&path).unwrap());
    let (_, trailer) = generate(header(), &units, &config, out).unwrap();
    assert_eq!(trailer.records, devices * 4);
    println!(
        "M7: {} steps, {} bytes",
        trailer.records,
        std::fs::metadata(&path).unwrap().len()
    );
}

// M9 of ADR-0046 (decision 15): the M7 scenario at `M9_STEPS` steps (10⁷ by
// default), four a device, on both paths, in release:
//
//     M9_STEPS=100000000 cargo test --release -p ascetic-ddd-command-log \
//         --test scale -- --ignored --nocapture m9_
//
// Each prints its phases' wall times; peak memory, open files and disk are a
// sampler's beside it.
mod m9 {
    #![allow(
        clippy::disallowed_types,
        clippy::disallowed_methods,
        reason = "a measurement's harness: its wall clock and parameters are the measurement's, not a generation's"
    )]

    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Instant;

    use ascetic_ddd_command_log::engine::{Config, Observer, Progress, Unit, generate_file};
    use ascetic_ddd_command_log::live::{Commitment, dry, live};
    use ascetic_ddd_command_log::validate::{Limits, NoHost, validate_file};
    use ascetic_ddd_command_log::{Compression, open_file};

    use super::{DAY, Devices};

    /// The fixture's header with a window of two hours: a fact waits on the
    /// step an hour or so before it.
    fn header() -> ascetic_ddd_command_log::record::Header {
        ascetic_ddd_command_log::record::Header {
            window: 2 * 3_600_000_000,
            ..super::header()
        }
    }

    fn m9_base(name: &str) -> std::path::PathBuf {
        let base = std::path::Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/../../target/m9"))
            .join(name);
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(base.join("spill")).unwrap();
        base
    }

    /// The days the plan spans (`M9_DAYS`, 1 by default): the planned rate
    /// is the steps over them.
    fn m9_days() -> u64 {
        std::env::var("M9_DAYS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(1)
    }

    fn m9_steps() -> u64 {
        std::env::var("M9_STEPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10_000_000)
    }

    /// Devices in units of 2,500.
    fn m9_units() -> Vec<Devices> {
        let devices = m9_steps() / 4;
        (0..devices.div_ceil(2_500))
            .map(|i| Devices {
                first: i * 2_500,
                count: 2_500.min(devices - i * 2_500),
                days: m9_days(),
            })
            .collect()
    }

    /// Says each phase as it goes, with the wall time since the start.
    struct Clock {
        start: Instant,
        units: u64,
        said: AtomicU64,
        runs_said: [AtomicU64; 2],
    }

    impl Clock {
        fn say(&self, what: std::fmt::Arguments<'_>) {
            eprintln!("M9 {:>8.1} s  {what}", self.start.elapsed().as_secs_f64());
        }
    }

    impl Progress for Clock {
        fn began(&self, swept: u64) {
            self.say(format_args!("began, {swept} bytes swept"));
        }
        fn emitted(&self, units: u64, records: u64, spilled: u64) {
            let tenth = (units * 10) / self.units.max(1);
            if self.said.fetch_max(tenth, Ordering::Relaxed) < tenth {
                self.say(format_args!(
                    "emitted {units} units, {records} steps, {spilled} bytes spilled"
                ));
            }
        }
        fn merging(&self, cells: u64) {
            self.say(format_args!("merging {cells} cells"));
        }
        fn merged(&self, cells: u64, lines: u64) {
            if cells.is_multiple_of(16) {
                self.say(format_args!("merged {cells} cells, {lines} lines"));
            }
        }
        fn runs(&self, kind: &'static str, on_disk: u64) {
            // Every gigabyte a kind's runs pass, and when they are gone.
            let gb = on_disk >> 30;
            let key = if kind == "ids" { 0 } else { 1 };
            let last = self.runs_said[key].swap(gb, Ordering::Relaxed);
            if gb != last || on_disk == 0 {
                self.say(format_args!("{kind} runs: {on_disk} bytes on disk"));
            }
        }
    }

    fn m9_config(spill: std::path::PathBuf, units: u64) -> (Config, Arc<Clock>) {
        let clock = Arc::new(Clock {
            start: Instant::now(),
            units,
            said: AtomicU64::new(0),
            runs_said: [AtomicU64::new(0), AtomicU64::new(0)],
        });
        let config = Config {
            threads: 4,
            budget: 64 << 20,
            cell: DAY * m9_days() / 64,
            cells: 128,
            spill,
            fan_in: 0,
            memory_spill: false,
            progress: Some(Observer(clock.clone())),
            pipe_blocks: 64,
        };
        (config, clock)
    }

    /// The persisted path: the log written compressed (`M9_LEVEL`, 1 by default,
    /// `none` for plain), read back, validated unless `M9_VALIDATE=0`.
    #[test]
    #[ignore = "M9: the persisted path, in release, beside a sampler"]
    fn m9_persisted() {
        let base = m9_base("persisted");
        let units = m9_units();
        let units: Vec<&dyn Unit> = units.iter().map(|u| u as &dyn Unit).collect();
        let (config, clock) = m9_config(base.join("spill"), units.len() as u64);
        let compression = match std::env::var("M9_LEVEL").as_deref() {
            Ok("none") => Compression::None,
            Ok(level) => Compression::Zstd {
                level: level.parse().unwrap(),
            },
            Err(_) => Compression::Zstd { level: 1 },
        };
        let path = base.join("log.jsonl.zst");
        let trailer = generate_file(&path, header(), &units, &config, compression).unwrap();
        let bytes = std::fs::metadata(&path).unwrap().len();
        clock.say(format_args!(
            "written: {} steps, {bytes} bytes, {compression:?}",
            trailer.records
        ));
        let limits = Limits {
            progress: Some(Observer(clock.clone())),
            ..Limits::new(base.join("ids"))
        };
        if std::env::var("M9_READ").as_deref() == Ok("0") {
            return validated(&path, &limits, &clock);
        }
        let started = Instant::now();
        let read = open_file(&path)
            .unwrap()
            .try_fold(0u64, |n, r| r.map(|_| n + 1))
            .unwrap();
        let secs = started.elapsed().as_secs_f64();
        clock.say(format_args!(
            "read: {read} steps, {:.0} steps/s",
            read as f64 / secs
        ));
        if std::env::var("M9_VALIDATE").as_deref() == Ok("0") {
            return;
        }
        validated(&path, &limits, &clock);
    }

    /// The log validated, with what it found and must hold.
    fn validated(path: &std::path::Path, limits: &Limits, clock: &Clock) {
        let started = Instant::now();
        let report = validate_file(path, &NoHost, limits).unwrap();
        let secs = started.elapsed().as_secs_f64();
        assert!(
            report.violations.is_empty(),
            "{:?}",
            report.violations.first()
        );
        clock.say(format_args!(
            "validated: {} steps, {:.0} steps/s, peaks {:?}",
            report.records,
            report.records as f64 / secs,
            report.peaks
        ));
    }

    /// The live path: a dry run validated into a commitment, then a live run
    /// released into a sink through it.
    #[test]
    #[ignore = "M9: the live path, in release, beside a sampler"]
    fn m9_live() {
        let base = m9_base("live");
        let units = m9_units();
        let units: Vec<&dyn Unit> = units.iter().map(|u| u as &dyn Unit).collect();
        let (config, clock) = m9_config(base.join("spill"), units.len() as u64);
        let path = base.join("plan.commitment");
        let limits = Limits {
            progress: Some(Observer(clock.clone())),
            ..Limits::new(base.join("ids"))
        };
        let report = dry(
            header(),
            &units,
            &config,
            &NoHost,
            &limits,
            "m9",
            1 << 16,
            &path,
        )
        .unwrap();
        assert!(
            report.violations.is_empty(),
            "{:?}",
            report.violations.first()
        );
        clock.say(format_args!(
            "dry: {} steps validated, peaks {:?}",
            report.records, report.peaks
        ));
        let commitment = Commitment::read(&path).unwrap();
        clock.said.store(0, Ordering::Relaxed);
        let (_, trailer) = live(
            header(),
            &units,
            &config,
            &commitment,
            "m9",
            std::io::sink(),
        )
        .unwrap();
        clock.say(format_args!("live: {} steps released", trailer.records));
    }

    /// SHA-256 in process over 1 GiB of log lines: `sha2` soft, or with
    /// `--features sha2/asm`.
    #[test]
    #[ignore = "M9: SHA-256's MB/s, in release"]
    fn m9_sha256() {
        use sha2::{Digest, Sha256};
        let line =
            br#"{"step":{"id":["device",1234567,"fact",2],"at":123456789012,"call":"lifecycle"}}"#;
        let block: Vec<u8> = line.iter().copied().cycle().take(64 << 20).collect();
        let started = Instant::now();
        let mut digest = Sha256::new();
        for _ in 0..16 {
            digest.update(&block);
        }
        let _ = digest.finalize();
        let secs = started.elapsed().as_secs_f64();
        eprintln!("M9 sha256: {:.0} MB/s", 1024.0 * 1.048_576 / secs);
    }
}

// K1e-2 of ADR-0047: what the plan's own bound costs the validator, on a plan
// whose every hold outlasts the window — eight subjects live at a time through
// cycles of an update capturing its ETag, a read sending it, and a call
// awaiting the read settled, a line every 125 µs, W 20 ms and timeouts of
// 10 s: some 8·10⁴ steps held past the window and as many not ended at once,
// a pin on every update, a barrier key a label and a cycle. In release, the
// validation timed inside, beside /usr/bin/time -v:
//
//     KE2_STEPS=2000000 cargo test --release -p ascetic-ddd-command-log \
//         --test scale -- --ignored --nocapture k1e2_
mod k1e2 {
    #![allow(
        clippy::disallowed_methods,
        clippy::disallowed_types,
        reason = "a measurement: it reads its size from the environment and times the pass"
    )]
    use super::*;
    use ascetic_ddd_command_log::log::Writer;
    use ascetic_ddd_command_log::record::{Header, Value};
    use ascetic_ddd_command_log::validate::{Limits, NoHost, validate_file};

    const EVERY: u64 = 125;
    const LIVE: u64 = 8;

    fn record(i: u64, cycles: u64) -> Record {
        let at = i * EVERY;
        let (round, within) = (i / (3 * LIVE), i % (3 * LIVE));
        let (phase, slot) = (within / LIVE, within % LIVE);
        let cycle = (round % cycles) as u32;
        let s = (round / cycles) * LIVE + slot;
        let update = id("group", s, "update", cycle);
        let read = id("group", s, "read", cycle);
        match phase {
            0 => {
                let mut r = captures_etag(
                    Record {
                        action: rest("update", vec![Value::Literal(s.to_string())]),
                        ..step(update, at)
                    },
                    s,
                );
                if cycle > 0 {
                    r = after(r, &id("group", s, "update", cycle - 1), On::Settled);
                    r = after(r, &id("group", s, "read", cycle - 1), On::Settled);
                }
                r
            }
            1 => after(
                Record {
                    action: read_etag("read", s, &update),
                    ..step(read, at)
                },
                &update,
                On::Expected,
            ),
            _ => after(step(id("group", s, "call", cycle), at), &read, On::Settled),
        }
    }

    #[test]
    #[ignore = "K1e-2: the bound's cost, in release, under /usr/bin/time -v"]
    fn k1e2_bound() {
        let steps: u64 = std::env::var("KE2_STEPS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(2_000_000);
        // On disk: the validator refuses to spill into memory.
        let base = std::path::PathBuf::from(
            std::env::var("KE2_DIR")
                .unwrap_or_else(|_| concat!(env!("CARGO_MANIFEST_DIR"), "/../../target").into()),
        );
        std::fs::create_dir_all(&base).unwrap();
        let dir = tempfile::tempdir_in(base).unwrap();
        let path = dir.path().join("plan.jsonl");
        let header = Header {
            window: 20_000,
            ..header()
        };
        let started = std::time::Instant::now();
        {
            let mut w = Writer::new(BufWriter::new(File::create(&path).unwrap()), header).unwrap();
            for i in 0..steps {
                w.step(record(i, 16)).unwrap();
            }
            w.finish().unwrap();
        }
        let written = started.elapsed().as_secs_f64();
        let spill = dir.path().join("spill");
        let started = std::time::Instant::now();
        let report = validate_file(&path, &NoHost, &Limits::new(spill)).unwrap();
        let secs = started.elapsed().as_secs_f64();
        assert!(
            report.violations.is_empty(),
            "{:?}",
            &report.violations[..1]
        );
        eprintln!(
            "K1e-2: {steps} steps written in {written:.1} s, validated in {secs:.1} s, {:.1}·10³ steps/s; {:?}",
            steps as f64 / secs / 1e3,
            report.peaks
        );
    }
}
