//! The shell's gates that its lints cannot hold alone (ADR-0047 decision 16):
//! its dependencies an allowlist; the clocks `clippy.toml` refuses allowed by
//! the `clock` module alone; every `select!` biased, so that the order a loop
//! takes its branches in is the order written; every task spawned at the one
//! place that times it (decision 18).

const SOURCES: [(&str, &str); 15] = [
    ("carrier.rs", include_str!("../src/carrier.rs")),
    ("clock.rs", include_str!("../src/clock.rs")),
    ("decorate.rs", include_str!("../src/decorate.rs")),
    ("io.rs", include_str!("../src/io.rs")),
    ("lib.rs", include_str!("../src/lib.rs")),
    ("live.rs", include_str!("../src/live.rs")),
    ("rest.rs", include_str!("../src/rest.rs")),
    ("run.rs", include_str!("../src/run.rs")),
    ("sink.rs", include_str!("../src/sink.rs")),
    ("source.rs", include_str!("../src/source.rs")),
    ("stages.rs", include_str!("../src/stages.rs")),
    ("stamps.rs", include_str!("../src/stamps.rs")),
    ("stand.rs", include_str!("../src/stand.rs")),
    ("tcp.rs", include_str!("../src/tcp.rs")),
    ("turn.rs", include_str!("../src/turn.rs")),
];

#[test]
fn the_shell_depends_on_its_allowlist_alone() {
    let manifest = include_str!("../Cargo.toml");
    let mut section = "";
    let mut deps = Vec::new();
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            section = line;
            continue;
        }
        let listed = section == "[dependencies]" || section.ends_with(".dependencies]");
        if listed && !line.is_empty() && !line.starts_with('#') {
            deps.push(line.split('=').next().unwrap_or("").trim().to_owned());
        }
    }
    assert_eq!(
        deps,
        vec![
            "ascetic-ddd-command-log",
            "ascetic-ddd-scheduler",
            "bytes",
            "http",
            "http-body",
            "hyper",
            "tokio",
            // Linux's voluntary context switches: whether a park blocked
            // (decision 18).
            "nix"
        ]
    );
}

/// Decision 18: a task's polls are timed for its send path's lag only if it
/// is spawned where they are — one place, `Stamps::spawn_on`.
#[test]
fn every_task_is_spawned_where_it_is_timed() {
    for (name, text) in SOURCES {
        let code: String = text
            .lines()
            .filter(|l| !l.trim_start().starts_with("//"))
            .collect::<Vec<_>>()
            .join("\n");
        let spawns = [
            "tokio::spawn",
            "task::spawn",
            "spawn_local",
            ".spawn(async",
            "spawn_blocking",
        ]
        .iter()
        .chain(std::iter::once(&"on.spawn("))
        .filter(|p| code.contains(**p))
        .count();
        let one = code.matches("on.spawn(").count();
        // Any method spawn of a task, however its receiver is named — a
        // thread's builder takes a closure, a task a future.
        let any = code
            .match_indices(".spawn(")
            .filter(|(i, m)| {
                let after = code[i + m.len()..].trim_start();
                !after.starts_with("move ||") && !after.starts_with("||")
            })
            .count();
        match name {
            // The run's timed spawn, the measurement's and the probe's
            // before T0 (an `async` block: two patterns).
            "stamps.rs" => assert_eq!((spawns, one, any), (2, 3, 3), "{name}"),
            // The virtual clock's own task, its timer: no attempt's.
            "clock.rs" => assert_eq!(code.matches("tokio::spawn").count(), 1, "{name}"),
            _ => assert_eq!((spawns, any), (0, 0), "{name}"),
        }
    }
}

#[test]
fn only_the_clock_reads_a_clock() {
    let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/src");
    let mut listed: Vec<&str> = SOURCES.iter().map(|(n, _)| *n).collect();
    listed.sort_unstable();
    let mut found: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().file_name().into_string().unwrap())
        .collect();
    found.sort_unstable();
    assert_eq!(found, listed, "a source the gate does not read");
    for (name, text) in SOURCES {
        // Every `allow(…)` and `expect(…)`, whitespace dropped: one naming a
        // refused clock lets its module read one.
        let flat: String = text.split_whitespace().collect();
        let allows = ["allow(", "expect("].iter().any(|attr| {
            flat.match_indices(attr).any(|(i, _)| {
                let inner = &flat[i..];
                let end = inner.find(')').unwrap_or(inner.len());
                inner[..end].contains("clippy::disallowed")
            })
        });
        assert_eq!(allows, name == "clock.rs", "{name}");
    }
}

#[test]
fn every_select_is_biased() {
    for (name, text) in SOURCES {
        let mut rest = text;
        while let Some(i) = rest.find("select! {") {
            let after = &rest[i + "select! {".len()..];
            assert!(
                after.trim_start().starts_with("biased;"),
                "{name}: a select! not biased"
            );
            rest = after;
        }
    }
}

/// Decision 20: a constructor that assumes what the stages prove — a plan, a
/// runtime's events, the descriptors, a run ready — is a test's alone, held
/// to the `testing` feature; none slips into a host's build.
#[test]
fn every_assumption_is_a_test_s() {
    let mut held = 0;
    for (name, text) in SOURCES {
        let lines: Vec<&str> = text.lines().collect();
        for (i, line) in lines.iter().enumerate() {
            let t = line.trim_start();
            if !(t.starts_with("pub fn assumed(") || t.starts_with("pub fn with_descriptors(")) {
                continue;
            }
            // The attributes right above it, its doc comment aside.
            let above = lines[..i]
                .iter()
                .rev()
                .map(|l| l.trim_start())
                .take_while(|l| l.starts_with("#[") || l.starts_with("///"))
                .any(|l| l == "#[cfg(feature = \"testing\")]");
            assert!(above, "{name}:{}: {t}", i + 1);
            held += 1;
        }
    }
    // A plan's, the runtimes' two, the descriptors', a ready run's.
    assert_eq!(held, 5);
}
