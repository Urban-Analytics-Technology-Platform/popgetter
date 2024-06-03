use anyhow::Result;
use data_request_spec::DataRequestSpec;
use metadata::Metadata;
use parquet::get_metrics;
use polars::{frame::DataFrame, prelude::DataFrameJoinOps};
use tokio::try_join;

use crate::{data_request_spec::MetricSpec, geo::get_geometries};
pub mod data_request_spec;
pub mod error;
pub mod geo;
pub mod metadata;
pub mod parquet;

#[cfg(feature="formatters")]
pub mod formatters;

pub struct Popgetter {
    pub metadata: Metadata,
}

impl Popgetter {
    /// Setup the Popgetter object 
    pub fn new() -> Result<Self> {
        let metadata = metadata::load_all(&["be"])?;
        Ok(Self { metadata })
    }

    // Given a Data Request Spec 
    // Return a DataFrame of the selected dataset 
    pub async fn get_data_request(&self, data_request: &DataRequestSpec) -> Result<DataFrame> {
        let metric_requests = data_request.metric_requests(&self.metadata)?;

        // Required because polars is blocking
        let metrics = tokio::task::spawn_blocking(move || {
            get_metrics(&metric_requests.metrics,None)
        });
        
        let geom_file  = self.metadata.get_geom_details(&metric_requests.selected_geometry)?;
        let geoms = get_geometries(&geom_file, None, None);

        // try_join requires us to have the errors from all futures be the same. 
        // We use anyhow to get it back properly
        let (metrics,geoms) = try_join!(async move { metrics.await.map_err(anyhow::Error::from)}, geoms)?;
        println!("geoms {geoms:#?}");
        println!("metrics {metrics:#?}");
        
        let result =geoms.inner_join(&metrics?,["GEOID"],["GEO_ID"])?; 
        Ok(result)
    }
}

