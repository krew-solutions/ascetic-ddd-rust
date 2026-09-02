//! Runnable counterpart of `python -m ascetic_ddd.saga.examples.serialization_example`.
//!
//! ```text
//! cargo run --example serialization_example
//! ```

use ascetic_ddd_saga::Result;
use ascetic_ddd_saga::examples::serialization_example::{
    run_compensation_with_serialization, run_travel_booking_with_serialization,
};

fn main() -> Result<()> {
    futures::executor::block_on(async {
        run_travel_booking_with_serialization().await?;
        run_compensation_with_serialization().await?;
        Ok(())
    })
}
