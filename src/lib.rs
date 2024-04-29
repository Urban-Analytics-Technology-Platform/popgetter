use anyhow::Result;
use data_request_spec::DataRequestSpec;
use metadata::{load_metadata, SourceDataRelease};
use parquet::{get_metrics, MetricRequest};
use polars::frame::DataFrame;
pub mod data_request_spec;
pub mod geo;
pub mod metadata;
pub mod parquet;

pub struct Popgetter {
    pub metadata: SourceDataRelease,
}

impl Popgetter {
    pub fn new() -> Result<Self> {
        let metadata = load_metadata("us_metadata_test2.json")?;
        Ok(Self { metadata })
    }

    pub fn get_data_request(&self, data_request: &DataRequestSpec) -> Result<DataFrame> {
        let metric_requests = data_request.metric_requests(&self.metadata)?;
        println!("{metric_requests:#?}");
        let metrics = get_metrics(&metric_requests, None)?;
        Ok(metrics)
    }
}
