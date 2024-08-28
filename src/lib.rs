use anyhow::Result;
use data_request_spec::DataRequestSpec;
use log::debug;
use metadata::Metadata;
use polars::frame::DataFrame;
use search::{DownloadParams, Params, SearchParams, SearchResults};

use crate::config::Config;

// Re-exports
pub use column_names as COL;

// Modules
pub mod column_names;
pub mod config;
pub mod data_request_spec;
pub mod error;
#[cfg(feature = "formatters")]
pub mod formatters;
pub mod geo;
pub mod metadata;
pub mod parquet;
pub mod search;

/// Type for popgetter data and API
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

    /// Generates `SearchResults` using popgetter given `SearchParams`
    pub fn search(&self, search_params: &SearchParams) -> SearchResults {
        search_params
            .clone()
            .search(&self.metadata.combined_metric_source_geometry())
    }

    /// Downloads data using popgetter given a `DataRequestSpec`
    pub async fn get_data_request(&self, data_request_spec: DataRequestSpec) -> Result<DataFrame> {
        let params: Params = data_request_spec.try_into()?;
        let search_results = self.search(&params.search);
        search_results
            .download(&self.config, &params.search, &params.download)
            .await
    }

    /// Downloads data using popgetter given `SearchParams`
    pub async fn get_search_params(
        &self,
        search_params: SearchParams,
        download_params: DownloadParams,
    ) -> Result<DataFrame> {
        self.search(&search_params)
            .download(&self.config, &search_params, &download_params)
            .await
    }
}
