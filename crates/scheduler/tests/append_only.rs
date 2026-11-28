//! K2b-7e-2b-3d-3b-4a of ADR-0047: the pins' file is appended to, never
//! edited — `vectors/append_only.py`, which CI's `append-only` runs, in a
//! repository of its own: a line appended passes, a line edited, removed or
//! moved fails, a file new since the base passes.

use std::path::Path;
use std::process::Command;

fn git(dir: &Path, args: &[&str]) {
    let out = Command::new("git")
        .args([
            "-c",
            "user.name=t",
            "-c",
            "user.email=t@t",
            "-c",
            "commit.gpgsign=false",
        ])
        .args(args)
        .current_dir(dir)
        .output()
        .expect("git, which the library's gate requires");
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[expect(
    clippy::disallowed_methods,
    reason = "a scratch repository's file (ADR-0047 K1e-3 refuses files to the machine, not its tests)"
)]
fn passes(dir: &Path, now: &str) -> bool {
    std::fs::write(dir.join("pins.jsonl"), now).unwrap();
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/vectors/append_only.py");
    Command::new("python3")
        .args(["-I", script, "HEAD", "pins.jsonl"])
        .current_dir(dir)
        .output()
        .expect("python3, which the library's gate requires")
        .status
        .success()
}

#[test]
#[expect(
    clippy::disallowed_methods,
    reason = "a scratch repository's file (ADR-0047 K1e-3 refuses files to the machine, not its tests)"
)]
fn a_pinned_line_is_appended_to_never_edited() {
    let dir = tempfile::tempdir().unwrap();
    let d = dir.path();
    git(d, &["init", "-q"]);
    std::fs::write(d.join("other"), "x\n").unwrap();
    git(d, &["add", "other"]);
    git(d, &["commit", "-q", "-m", "base"]);
    assert!(passes(d, "a\n"), "a file new since the base");
    git(d, &["add", "pins.jsonl"]);
    git(d, &["commit", "-q", "-m", "pins"]);
    assert!(passes(d, "a\nb\n"), "a line appended");
    std::fs::write(d.join("pins.jsonl"), "a\nb\n").unwrap();
    git(d, &["commit", "-q", "-am", "appended"]);
    assert!(passes(d, "a\nb\n"), "nothing changed");
    assert!(!passes(d, "A\nb\n"), "a line edited");
    assert!(!passes(d, "a\n"), "a line removed");
    assert!(!passes(d, "b\na\n"), "lines moved");
    assert!(!passes(d, "a\nb"), "the last line's end removed");
}
