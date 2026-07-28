//! Alopex Skulk wide-column time-series storage engine.
//!
//! Version 0.3 separates the wide data model, durable Arrow/Parquet storage,
//! and protocol-independent ingestion into explicit responsibility modules.
//! The v0.2 single-value TSM/Gorilla format is intentionally unsupported.

#![deny(missing_docs)]
#![warn(rustdoc::missing_crate_level_docs)]

pub mod error;
pub mod ingest;
pub mod model;
pub mod store;

pub use error::{Result, TsmError};

#[cfg(test)]
mod module_surface_tests {
    #[test]
    fn exposes_v0_3_responsibility_modules() {
        #[allow(unused_imports)]
        use crate::{ingest as _, model as _, store as _};
    }
}
