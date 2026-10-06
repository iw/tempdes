//! Temporal metric ingestion (observed values) and emission (simulated values).

pub mod cmd;
#[cfg(feature = "fetch")]
pub mod fetch;
pub mod observed;
pub mod prom;
