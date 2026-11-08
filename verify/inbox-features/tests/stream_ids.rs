//! A stream id read and written in the host's build of serde_json
//! (ADR-0037 decision 1): the golden table the workspace runs, the same.

#[path = "../../../crates/bus/tests/golden/stream_ids.rs"]
mod golden;

use serde_json::Value;

/// The build is the host's: under `arbitrary_precision` serde_json keeps
/// `1e400` as written and reads the private key as a number, which the
/// default build cannot. Without this the table would run in the default
/// build twice.
#[test]
fn the_build_is_the_host_s() {
    assert!(serde_json::from_str::<Value>("1e400").is_ok());
    assert!(
        serde_json::from_str::<Value>("{\"$serde_json::private::Number\":\"5\"}")
            .is_ok_and(|value| value.is_number())
    );
    #[cfg(feature = "raw_value")]
    assert!(serde_json::value::RawValue::from_string("1".to_owned()).is_ok());
}

#[test]
fn the_golden_table_reads_as_it_should() {
    golden::check();
}
