//! `OBSERVERS.md` quotes code from `tests/observer_channel.rs` and says the
//! quote is exact. This test is what makes that sentence true: every block the
//! guide marks as verbatim must appear in the test source, character for
//! character. A quote that drifts fails here rather than misleading a reader.

const GUIDE: &str = include_str!("../OBSERVERS.md");
const SOURCE: &str = include_str!("observer_channel.rs");
const MARKER: &str = "<!-- verbatim: tests/observer_channel.rs -->";

/// The code blocks that follow a verbatim marker.
fn quoted_blocks(guide: &str) -> Vec<&str> {
    guide
        .split(MARKER)
        .skip(1)
        .map(|after| {
            let fence = "```rust\n";
            let start = after
                .find(fence)
                .expect("a marker is followed by a rust block")
                + fence.len();
            let end = after[start..].find("```").expect("the block is closed") + start;
            &after[start..end]
        })
        .collect()
}

#[test]
fn every_verbatim_block_in_the_guide_is_in_the_test() {
    let blocks = quoted_blocks(GUIDE);
    assert!(
        !blocks.is_empty(),
        "the guide marks at least one block as verbatim"
    );

    for block in blocks {
        assert!(
            SOURCE.contains(block),
            "a block marked verbatim in OBSERVERS.md is not in tests/observer_channel.rs:\n{block}",
        );
    }
}
