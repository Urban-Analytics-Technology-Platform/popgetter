use anyhow::Result;
use data_request_spec::DataRequestSpec;
use metadata::{load_metadata, SourceDataRelease};
use parquet::{get_metrics, MetricRequest};
use polars::frame::DataFrame;
use tokio::try_join;

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

        // Required because polars is blocking
        let metrics = tokio::task::spawn_blocking(move || {
            get_metrics(&metric_requests,None)
        });

        let geoms = get_geometries(&geom_file, None);

        // try_from requires us to have the errors from all futures be the same. 
        // We use anyhow to get it back properly
        let (metrics,geoms) = try_join!(async move { metrics.await.map_err(anyhow::Error::from)}, geoms)?;
        metrics
    }
}
