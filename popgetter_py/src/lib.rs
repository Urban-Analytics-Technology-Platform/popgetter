use std::default::Default;

use ::popgetter::{
    data_request_spec::DataRequestSpec,
    search::{SearchParams, SearchText},
    COL,
};
use polars::prelude::DataFrame;
use pyo3::{
    exceptions::PyValueError,
    prelude::*,
    types::{PyDict, PyString},
};
use pyo3_polars::PyDataFrame;
use serde::de::DeserializeOwned;

/// Converts Python dict to a generic `T` that can be deserialized without borrows.
fn convert_py_dict<T: DeserializeOwned>(obj: &Bound<'_, PyAny>) -> PyResult<T> {
    // Use Python JSON module to convert Python dict to JSON String
    if let Ok(dict) = obj.downcast::<PyDict>() {
        let json = PyModule::import_bound(dict.py(), "json")?.getattr("dumps")?;
        let json_str: String = json.call1((dict,))?.extract()?;

        // Deserialize from `str` to `T`
        serde_json::from_str::<T>(&json_str)
            // TODO: refine error type
            .map_err(|err| PyErr::new::<PyValueError, _>(err.to_string()))
    } else {
        Err(PyErr::new::<PyValueError, _>("Argument must be a 'dict'"))
    }
}

/// Returns search results as a `DataFrame` from given `SearchParams`.
async fn _search(search_params: SearchParams) -> DataFrame {
    let popgetter = ::popgetter::Popgetter::new().await.unwrap();
    let search_results = popgetter.search(&search_params);
    search_results
        .0
        .select([
            COL::METRIC_ID,
            COL::METRIC_HUMAN_READABLE_NAME,
            COL::METRIC_DESCRIPTION,
            COL::METRIC_HXL_TAG,
            COL::GEOMETRY_LEVEL,
        ])
        .unwrap()
}

/// Gets `SearchParams` from a given PyAny argument. `PyString` arguments are treated as text,
/// `PyDict` arguments are treated as deserializable `SearchParams`. Any other types will return an
/// error.
fn get_search_params(obj: &Bound<'_, PyAny>) -> PyResult<SearchParams> {
    if let Ok(text) = obj.downcast::<PyString>() {
        println!("Argument is 'str', searching as text: {}", text);
        let search_text = SearchText {
            text: text.to_string(),
            ..SearchText::default()
        };
        return Ok(SearchParams {
            text: vec![search_text],
            ..SearchParams::default()
        });
    }
    if let Ok(dict) = obj.downcast::<PyDict>() {
        println!(
            "Argument is 'dict', searching as search parameters: {}",
            dict
        );
        return convert_py_dict(dict);
    };
    Err(PyErr::new::<PyValueError, _>(
        "Argument must be either 'str' (text) or 'dict' (search parameters)",
    ))
}

/// Downloads data as a `DataFrame` for a given `DataRequestSpec`.
async fn _download_data_request_spec(data_request: DataRequestSpec) -> DataFrame {
    let popgetter = ::popgetter::Popgetter::new().await.unwrap();
    popgetter
        .download_data_request_spec(&data_request)
        .await
        .unwrap()
}

/// Gets `DataRequestSpec` from a given Python object.
fn get_data_request_spec(obj: &Bound<'_, PyAny>) -> PyResult<DataRequestSpec> {
    if let Ok(dict) = obj.downcast::<PyDict>() {
        return convert_py_dict(dict);
    }
    Err(PyErr::new::<PyValueError, _>(
        "Argument must be 'dict' (data request spec)",
    ))
}

/// Downloads data using Popgetter from a given `DataRequestSpec` dict with data returned as a
/// polars `DataFrame`.
#[pyfunction]
fn download_data_request(
    #[pyo3(from_py_with = "get_data_request_spec")] data_request: DataRequestSpec,
) -> PyResult<PyDataFrame> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = rt.block_on(_download_data_request_spec(data_request));
    Ok(PyDataFrame(result))
}

/// Searches using Popgetter from a given `SearchParams` dict or text `String` with search results
/// returned as a polars `DataFrame`.
#[pyfunction]
fn search(
    #[pyo3(from_py_with = "get_search_params")] search_query: SearchParams,
) -> PyResult<PyDataFrame> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let result = rt.block_on(_search(search_query));
    Ok(PyDataFrame(result))
}

/// Popgetter Python module.
#[pymodule]
fn popgetter(_py: Python, m: &Bound<PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(search, m)?)?;
    m.add_function(wrap_pyfunction!(download_data_request, m)?)?;
    Ok(())
}
