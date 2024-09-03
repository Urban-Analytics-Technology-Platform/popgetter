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

/// Type for popgetter metadata, config and API
#[derive(Debug, PartialEq)]
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

    // Only include method with "cache" feature since it requires a filesystem
    #[cfg(feature = "cache")]
    /// Setup the Popgetter object with custom configuration from cache
    pub fn new_with_config_and_cache<P: AsRef<Path>>(config: Config, cache: P) -> Result<Self> {
        let metadata = Metadata::from_cache(cache)?;
        Ok(Self { metadata, config })
    }

    /// Generates `SearchResults` using popgetter given `SearchParams`
    // TODO: consider reverting to an API where `SearchParams` are moved, add benches
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

#[cfg(test)]
#[cfg(feature = "cache")]
mod tests {

    use tempfile::TempDir;

    use super::*;

    #[tokio::test]
    async fn test_popgetter_cache() -> anyhow::Result<()> {
        let tempdir = TempDir::new()?;
        let config = Config::default();
        let popgetter = Popgetter::new_with_config(config.clone()).await?;
        popgetter.metadata.write_cache(&tempdir)?;
        let popgetter_from_cache = Popgetter::new_with_config_and_cache(config, &tempdir)?;
        assert_eq!(popgetter, popgetter_from_cache);
        Ok(())
    }
}
