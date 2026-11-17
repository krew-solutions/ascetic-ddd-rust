//! The statements that move a row, read from the library's own source
//! (ADR-0042 § Executors, X1 and R1). A statement that takes a row out of
//! the queue — marks it, parks it, sets it aside — clears every hold on it,
//! unconditionally or exactly when it leaves, and the baseline's CHECK
//! refuses one that forgets, but only once a held row meets it; a statement
//! no test runs on a held row would wait for production to fail. The gate
//! reads every one. And every interval a row waits is bound to the
//! database's clock as the statement runs: no process's clock, which may
//! run an hour fast or slow, and not the transaction's start, which a slow
//! subscriber left behind.

/// The sources whose SQL writes the inbox table.
const SOURCES: [(&str, &str); 6] = [
    ("store.rs", include_str!("store.rs")),
    ("surface.rs", include_str!("surface.rs")),
    ("dispatch.rs", include_str!("dispatch.rs")),
    ("sweep.rs", include_str!("sweep.rs")),
    ("verbs.rs", include_str!("verbs.rs")),
    ("stalls.rs", include_str!("stalls.rs")),
];

/// The `SET` clause of every `UPDATE` of the inbox table in `source`: the
/// text from `SET` to its `WHERE`, Rust's line continuations kept.
fn set_clauses(source: &str) -> Vec<String> {
    source
        .match_indices("UPDATE {")
        .filter_map(|(at, _)| {
            let statement = &source[at..];
            // `{table}`, `{}`, never `{table}_slots` nor `{table}_rejects`
            let target = statement.split_whitespace().nth(1)?;
            if !(target == "{table}" || target == "{}") {
                return None;
            }
            let set = statement.find(" SET ")?;
            let end = statement[set..].find("WHERE").map(|end| set + end)?;
            Some(statement[set..end].to_owned())
        })
        .collect()
}

/// Whether `clause` takes a row out of the queue: it marks the row
/// processed, parks it, or sets it aside to wait.
fn leaves(clause: &str) -> bool {
    clause.contains("processed_position = nextval")
        || clause.contains("parked_at = CURRENT_TIMESTAMP")
        || clause.contains("parked_at = CASE")
        || clause.contains("waiting_for = $")
}

#[test]
fn every_statement_that_takes_a_row_out_of_the_queue_clears_its_holds() {
    let mut exits = 0;
    for (file, source) in SOURCES {
        for clause in set_clauses(source).iter().filter(|clause| leaves(clause)) {
            exits += 1;
            // the fragment, or each of its fields cleared exactly when the
            // statement parks: the spend that keeps a row queued keeps its
            // back-off
            let conditional = [
                "held_since = CASE WHEN {parks} THEN NULL",
                "hold_kind = CASE WHEN {parks} THEN NULL",
                "next_attempt_at = CASE WHEN {parks} THEN NULL",
                "holds = NULL",
            ]
            .iter()
            .all(|field| clause.contains(field));
            assert!(
                clause.contains("{LEAVE_QUEUE}") || conditional,
                "{file}: a statement takes a row out of the queue and leaves its holds: {clause}"
            );
            // a cause lives as long as the state it explains: every exit but
            // a park, which writes its own, clears it
            if !clause.contains("parked_at = CURRENT_TIMESTAMP")
                && !clause.contains("parked_at = CASE")
            {
                assert!(
                    clause.contains("cause = NULL"),
                    "{file}: an exit keeps the row's cause: {clause}"
                );
            }
        }
    }
    // mark, resolve, set aside, the expiry, the park of a row's own values,
    // the spend, the operator's park of held heads or of a queue (one
    // statement for both): a gate that matched fewer reads nothing
    assert!(exits >= 7, "{exits} exits found");
}

#[test]
fn every_wait_a_row_is_given_runs_from_the_database_s_clock_as_the_statement_runs() {
    let mut waits = 0;
    for (file, source) in SOURCES {
        for (at, _) in source.match_indices("next_attempt_at = ") {
            let value = &source[at + "next_attempt_at = ".len()..];
            let value = value
                .split([',', '\\', '"'])
                .next()
                .unwrap_or_default()
                .trim();
            waits += 1;
            assert!(
                value == "NULL"
                    || value.starts_with("clock_timestamp() + make_interval(")
                    || value.starts_with("CASE WHEN {parks} THEN NULL"),
                "{file}: a wait bound to another clock: next_attempt_at = {value}"
            );
        }
        for (at, _) in source.match_indices("ELSE clock_timestamp()") {
            assert!(source[at..].starts_with("ELSE clock_timestamp() + make_interval("));
        }
    }
    assert!(waits >= 4, "{waits} waits found");
}
