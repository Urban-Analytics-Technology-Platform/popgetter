use anyhow::Result;
use data_request_spec::DataRequestSpec;
use log::debug;
use metadata::Metadata;
use parquet::get_metrics;
use polars::{frame::DataFrame, prelude::DataFrameJoinOps};
use tokio::try_join;

use crate::{config::Config, geo::get_geometries};
pub mod config;
pub mod data_request_spec;
pub mod error;
pub mod geo;
pub mod metadata;
pub mod parquet;
pub mod search;

pub(crate) const GEO_ID_COL_NAME: &str = "GEO_ID";

#[cfg(feature = "formatters")]
pub mod formatters;

pub struct Popgetter {
    pub metadata: Metadata,
    pub config: Config,
}

impl Popgetter {
    /// Setup the Popgetter object with default configuration
    pub async fn new() -> Result<Self> {
        Self::new_with_config(Config::default()).await
    }

    /// Setup the Popgetter object with custom configuration
    pub async fn new_with_config(config: Config) -> Result<Self> {
        debug!("config: {config:?}");
        let metadata = metadata::load_all(&config).await?;
        Ok(Self { metadata, config })
    }

    // Given a Data Request Spec
    // Return a DataFrame of the selected dataset
    pub async fn get_data_request(&self, data_request: &DataRequestSpec) -> Result<DataFrame> {
        let metric_requests = data_request.metric_requests(&self.metadata)?;
        debug!("{:#?}", metric_requests);
        // Required because polars is blocking
        let metrics =
            tokio::task::spawn_blocking(move || get_metrics(&metric_requests.metrics, None));

        let geom_file = self
            .metadata
            .get_geom_details(&metric_requests.selected_geometry)?;
        let geom_file_full_path = format!("{}/{}.fgb", self.config.base_path, geom_file);
        let geoms = get_geometries(&geom_file_full_path, None, None);

        // try_join requires us to have the errors from all futures be the same.
        // We use anyhow to get it back properly
        let (metrics, geoms) = try_join!(
            async move { metrics.await.map_err(anyhow::Error::from) },
            geoms
        )?;
        debug!("geoms: {geoms:#?}");
        debug!("metrics: {metrics:#?}");

        let result = geoms.inner_join(&metrics?, [GEO_ID_COL_NAME], ["GEO_ID"])?;
        Ok(result)
    }
}
