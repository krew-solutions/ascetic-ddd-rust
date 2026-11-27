//! Today's readers, frozen (ADR-0047 decision 23, K2b-7e-4b-2): the tree's
//! JSON and the readers that parsed an answer's body whole when it was read,
//! as they stood at `2679bb9` — before K2b-7e-4a read it lazily and
//! K2b-7e-4b read it into a tape —, their imports alone rewritten. The
//! oracle of "every judgement today's"; `tests/readers.rs` pins both files
//! by digest, so a copy edited to agree with a regression turns red.

#[allow(dead_code, clippy::all)]
pub mod json;

#[allow(dead_code, clippy::all)]
pub mod answer;
