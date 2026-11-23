//! The shell's gates that its lints cannot hold alone (ADR-0047 decision 16):
//! its dependencies an allowlist; the clocks `clippy.toml` refuses allowed by
//! the `clock` module alone; every `select!` biased, so that the order a loop
//! takes its branches in is the order written.

const SOURCES: [(&str, &str); 11] = [
    ("carrier.rs", include_str!("../src/carrier.rs")),
    ("clock.rs", include_str!("../src/clock.rs")),
    ("io.rs", include_str!("../src/io.rs")),
    ("lib.rs", include_str!("../src/lib.rs")),
    ("rest.rs", include_str!("../src/rest.rs")),
    ("run.rs", include_str!("../src/run.rs")),
    ("sink.rs", include_str!("../src/sink.rs")),
    ("source.rs", include_str!("../src/source.rs")),
    ("stamps.rs", include_str!("../src/stamps.rs")),
    ("stand.rs", include_str!("../src/stand.rs")),
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
        if section == "[dependencies]" && !line.is_empty() && !line.starts_with('#') {
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
            "tokio"
        ]
    );
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
