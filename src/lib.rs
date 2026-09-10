//! Stream `pg_dump` plain-text output directly into Delta Lake tables.
//!
//! The crate is organised as a one-way pipeline. Bytes enter as chunks, are recognised
//! by [`scan`], decoded by [`copy`], turned into Arrow batches by [`builders`], and
//! written to Delta by [`sink`]. [`pipeline`] wires the stages together. See
//! `docs/architecture.md` for the full design, including the concurrency model and the
//! failure model.
//!
//! Every stage is synchronous and allocation-free in steady state. Asynchrony appears
//! only at the storage edge, in [`sink`], which [`pipeline`] drives on a private runtime.
//!
//! # Status
//!
//! The pipeline runs end to end, single threaded. The decode pool described in the
//! architecture document is not built yet, and neither are the Python bindings.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

pub mod chan;
pub mod copy;
pub mod dump;
pub mod error;
pub mod pipeline;
pub mod scan;
pub mod sink;
pub mod types;
pub mod builders;
pub mod values;

pub use error::{Error, Result};

mod python;

use pyo3::prelude::*;

/// The compiled half of the `pgdelta` package. `python/pgdelta/__init__.py` re-exports
/// its contents, so callers import from `pgdelta`, not `pgdelta._pgdelta`.
#[pymodule]
fn _pgdelta(module: &Bound<'_, PyModule>) -> PyResult<()> {
    python::register(module)
}

