use anyhow::Result;
use data_request_spec::DataRequestSpec;
use metadata::{load_metadata, SourceDataRelease};
use parquet::{get_metrics, MetricRequest};
use polars::frame::DataFrame;

use crate::geo::get_geometries;
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

    pub async fn get_data_request(&self, data_request: &DataRequestSpec) -> Result<DataFrame> {
        let metric_requests = data_request.metric_requests(&self.metadata)?;
        let geom_file = data_request.geom_details(&self.metadata)?;
        println!("{metric_requests:#?}");
        let metrics = tokio::task::spawn_blocking(move || {
            get_metrics(&metric_requests,None)
        }).await??;
        // let metrics = get_metrics(&metric_requests, None)?;
        let geoms = get_geometries(&geom_file, None).await?;
        Ok(metrics)
    }
}
