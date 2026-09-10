//! Stream `pg_dump` plain-text output directly into Delta Lake tables.
//!
//! The crate is organised as a one-way pipeline. Bytes enter as chunks, are recognised
//! by [`scan`], decoded by [`copy`], turned into Arrow batches by [`builders`], and
//! written to Delta by [`sink`]. [`pipeline`] wires the stages together. See
//! `docs/architecture.md` for the full design, including the concurrency model and the
//! failure model.
//!
//! Every decode stage is synchronous and allocation-free in steady state. Asynchrony
//! appears only at the storage edge, in [`sink`], which [`pipeline`] drives on a private
//! runtime.
//!
//! # Entry points
//!
//! [`pipeline::run`] and [`pipeline::run_file`] are the Rust surface, and the compiled
//! `pgdelta` Python module wraps the latter. Both are blocking, and both must be called
//! from outside a Tokio runtime.
//!
//! The reader and scanner run on the calling thread, because DDL must be read in order.
//! Row decoding and Parquet encoding run on a pool of workers sized by
//! [`pipeline::LoadConfig::threads`]; see the [`pipeline`] module documentation for the
//! concurrency model and for what bounds peak memory.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(rustdoc::broken_intra_doc_links)]

pub mod builders;
pub mod chan;
pub mod copy;
pub mod dump;
pub mod error;
pub mod pipeline;
pub mod scan;
pub mod sink;
pub mod source;
pub mod types;
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
