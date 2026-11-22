//! ADR-0047 decision 1: the machine's crate depends on the command log alone,
//! and on serde for its journal's vocabulary (K1e-3).
//! The lint gate names std paths only, so a crate with a clock of its own would
//! pass it; this list is the gate's other half.

#[test]
fn the_machine_depends_on_the_command_log_and_serde_alone() {
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
    assert_eq!(deps, vec!["ascetic-ddd-command-log", "serde"]);
}
