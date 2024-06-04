use anyhow::Result;
use data_request_spec::DataRequestSpec;
use log::debug;
use metadata::Metadata;
use parquet::get_metrics;
use polars::{frame::DataFrame, prelude::DataFrameJoinOps};
use tokio::try_join;

use crate::config::Config;
use crate::geo::get_geometries;
pub mod config;
pub mod data_request_spec;
pub mod geo;
pub mod metadata;
pub mod parquet;

#[cfg(feature = "formatters")]
pub mod formatters;

pub struct Popgetter {
    pub metadata: Metadata,
    pub config: Config,
}

impl Popgetter {
    /// Setup the Popgetter object
    pub async fn new() -> Result<Self> {
        let config = Config::from_toml();
        debug!("config: {config:?}");
        let metadata = metadata::load_all(&config).await?;
        Ok(Self { metadata, config })
    }

    // Given a Data Request Spec
    // Return a DataFrame of the selected dataset
    pub async fn get_data_request(&self, data_request: &DataRequestSpec) -> Result<DataFrame> {
        let metric_requests = data_request.metric_requests(&self.metadata)?;
        let geom_file = data_request.geom_details(&self.metadata)?;

        // Required because polars is blocking
        let metrics = tokio::task::spawn_blocking(move || get_metrics(&metric_requests, None));

        // TODO The custom geoid here is because of the legacy US code
        // This should be standardized on future pipeline outputs
        let geoms = get_geometries(&geom_file, None, Some("AFFGEOID".into()));

        // try_join requires us to have the errors from all futures be the same.
        // We use anyhow to get it back properly
        let (metrics, geoms) = try_join!(
            async move { metrics.await.map_err(anyhow::Error::from) },
            geoms
        )?;
        debug!("geoms: {geoms:#?}");
        debug!("metrics: {metrics:#?}");

        let result = geoms.inner_join(&metrics?, ["GEOID"], ["GEO_ID"])?;
        Ok(result)
    }
}
