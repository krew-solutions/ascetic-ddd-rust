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
}

impl Unit for Devices {
    fn emit(
        &self,
        out: &mut dyn FnMut(Record) -> Result<(), EngineError>,
    ) -> Result<(), EngineError> {
        for d in self.first..self.first + self.count {
            let key = Key::root(Seed(7)).child("device").at(d);
            let at = key.child("managed").unit().raw() % DAY;
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
        })
        .collect();
    let units: Vec<&dyn Unit> = units.iter().map(|u| u as &dyn Unit).collect();
    let config = Config {
        threads: 4,
        budget: 64 << 20,
        cell: DAY / 64,
        cells: 128,
        spill,
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
