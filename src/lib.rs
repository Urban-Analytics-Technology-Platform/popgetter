use std::path::Path;

use anyhow::Result;
use data_request_spec::DataRequestSpec;
use log::debug;
use metadata::Metadata;
use polars::frame::DataFrame;
use search::{Params, SearchParams, SearchResults};

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

    // TODO: add condition with feature "not_wasm" feature
    /// Setup the Popgetter object with custom configuration
    pub fn new_with_config_and_cache<P: AsRef<Path>>(config: Config, cache: P) -> Result<Self> {
        let metadata = Metadata::from_cache(cache)?;
        Ok(Self { metadata, config })
    }

    /// Generates `SearchResults` using popgetter given `SearchParams`
    pub fn search(&self, search_params: &SearchParams) -> SearchResults {
        search_params
            .clone()
            .search(&self.metadata.combined_metric_source_geometry())
    }

    /// Downloads data using popgetter given a `DataRequestSpec`
    pub async fn download_data_request_spec(
        &self,
        data_request_spec: &DataRequestSpec,
    ) -> Result<DataFrame> {
        let params: Params = data_request_spec.clone().try_into()?;
        let search_results = self.search(&params.search);
        search_results
            .download(&self.config, &params.download)
            .await
    }

    /// Downloads data using popgetter given `Params`
    pub async fn download_params(&self, params: &Params) -> Result<DataFrame> {
        self.search(&params.search)
            .download(&self.config, &params.download)
            .await
    }
}
