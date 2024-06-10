use std::{any::Any, sync::Arc};

use popgetter::Popgetter;
use pyo3::prelude::*;
use pyo3_polars::PyDataFrame;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn config() -> PyResult<String> {
    let popgetter = Popgetter::new();
    Ok("test".into())
}
/// A Python module implemented in Rust.
#[pymodule]
fn popgetter_py(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(config, m)?)?;
    Ok(())
}
