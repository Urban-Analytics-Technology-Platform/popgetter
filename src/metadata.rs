use anyhow::{anyhow, Result};
use polars::{
    frame::DataFrame,
    lazy::{
        dsl::col,
        frame::{IntoLazy, LazyFrame, ScanArgsParquet},
    },
    prelude::{lit, JoinArgs, JoinType, UnionArgs},
};
use std::default::Default;
use log::info;

use crate::parquet::MetricRequest;

/// This struct contains the base url and names of
/// the files that contain the metadata. It has a
/// default impl which give the version that we will
/// normally use but this allows us to customise it
/// if we need to.
pub struct CountryMetadataPaths {
    base_url: String,
    geometry: String,
    metrics: String,
    country: String,
    source_data: String,
    data_publishers: String,
}

impl Default for CountryMetadataPaths {
    fn default() -> Self {
        Self {
            // TODO move this to enviroment variable or config or something
            base_url: "https://popgetter.blob.core.windows.net/popgetter-dagster-test/test_2"
                .into(),
            geometry: "geometry_metadata.parquet".into(),
            metrics: "metric_metadata.parquet".into(),
            country: "country_metadata.parquet".into(),
            source_data: "source_data_releases.parquet".into(),
            data_publishers: "data_publishers.parquet".into(),
        }
    }
}

/// `CountryMetadataLoader` takes a country iso string
/// along with a CountryMetadataPaths and provides methods
/// for fetching and construting a `Metadata` catalouge.
pub struct CountryMetadataLoader {
    country: String,
    paths: CountryMetadataPaths,
}

/// The metadata struct contains the polars `DataFrames` for
/// the various different metadata tables. Can be constructed
/// from a single `CountryMetadataLoader` or for all countries.
/// It also provides the various functions for searching and
/// getting `MetricRequests` from the catalouge.
#[derive(Debug)]
pub struct Metadata {
    pub metrics: DataFrame,
    pub geometries: DataFrame,
    pub source_data_releases: DataFrame,
    pub data_publishers: DataFrame,
    pub countries: DataFrame,
}

impl Metadata {
    fn combined_metric_source_geometry(&self) -> LazyFrame {
        // Join with source_data_release and geometry
        self.metrics
            .clone()
            .lazy()
            .join(
                self.source_data_releases.clone().lazy(),
                [col("source_data_release_id")],
                [col("id")],
                JoinArgs::new(JoinType::Inner),
            )
            .join(
                self.geometries.clone().lazy(),
                [col("geometry_metadata_id")],
                [col("id")],
                JoinArgs::new(JoinType::Inner),
            )
    }
    /// Given a `metric_id`, geometry_level and year, return the
    /// `MetricRequest` object that can be used to fetch that metric
    pub fn get_metric_details(
        &self,
        metric_id: &str,
        geometry_level: &str,
        year: &str,
    ) -> Result<MetricRequest> {
        let matches = self
            .combined_metric_source_geometry()
            .filter(
                col("hxl_tag").eq(lit(metric_id)
                    .and(col("geometry_level").eq(lit(geometry_level)))
                    .and(col("year").eq(lit(year)))),
            )
            .collect()?;

        if matches.height() == 0 {
            Err(anyhow!("Failed to find metric"))
        } else if matches.height() > 1 {
            Err(anyhow!("Multiple metrics match this id"))
        } else {
            let column: String = matches
                .column("metric_parquet_column")?
                .str()?
                .get(0)
                .unwrap()
                .into();
            let file: String = matches
                .column("metric_parquet_file_url")?
                .str()?
                .get(0)
                .unwrap()
                .into();
            Ok(MetricRequest { column, file })
        }
    }

    /// Given a geometry level return the path to the
    /// geometry file that it corrisponds to
    pub fn get_geom_details(&self, geom_level: &str) -> Result<String> {
        let matches = self
            .geometries
            .clone()
            .lazy()
            .filter(col("level").eq(lit(geom_level)))
            .collect()?;

        let file = matches
            .column("filename_stem")?
            .str()?
            .get(0)
            .unwrap()
            .into();

        Ok(file)
    }
}

impl CountryMetadataLoader {
    /// Create a metadata loader for a specific Country
    pub fn new(country: &str) -> Self {
        let paths = CountryMetadataPaths::default();
        Self {
            country: country.into(),
            paths,
        }
    }
    /// Overwrite the Paths object to specifiy custom
    /// metadata filenames and `base_url`.
    pub fn with_paths(&mut self, paths: CountryMetadataPaths) -> &mut Self {
        self.paths = paths;
        self
    }

    /// Load the Metadata catalouge for this country with
    /// the specified metadata paths
    pub fn load(&self) -> Result<Metadata> {
        Ok(Metadata {
            metrics: self.load_metadata(&self.paths.metrics)?,
            geometries: self.load_metadata(&self.paths.geometry)?,
            source_data_releases: self.load_metadata(&self.paths.source_data)?,
            data_publishers: self.load_metadata(&self.paths.data_publishers)?,
            countries: self.load_metadata(&self.paths.country)?,
        })
    }

    /// Performs a load of a given metadata parquet file
    fn load_metadata(&self, path: &str) -> Result<DataFrame> {
        let url = format!("{}/{}/{path}", self.paths.base_url, self.country);
        let args = ScanArgsParquet::default();
        info!("Attempting to load {url}");
        let df: DataFrame = LazyFrame::scan_parquet(url, args)?.collect()?;
        Ok(df)
    }
}

/// Load the metadata for a list of countries and merge them into
/// a single `Metadata` catalouge.
pub fn load_all(countries: &[&str]) -> Result<Metadata> {
    let metadata: Result<Vec<Metadata>> = countries
        .iter()
        .map(|c| CountryMetadataLoader::new(c).load())
        .collect();
    let metadata = metadata?;

    // Merge metrics
    let metric_dfs: Vec<LazyFrame> = metadata.iter().map(|m| m.metrics.clone().lazy()).collect();
    let metrics = polars::prelude::concat(metric_dfs, UnionArgs::default())?.collect()?;

    // Merge geometries
    let geometries_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.geometries.clone().lazy())
        .collect();
    let geometries = polars::prelude::concat(geometries_dfs, UnionArgs::default())?.collect()?;

    // Merge source data relaeses
    let source_data_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.source_data_releases.clone().lazy())
        .collect();

    let source_data_releases =
        polars::prelude::concat(source_data_dfs, UnionArgs::default())?.collect()?;

    // Merge source data publishers
    let data_publisher_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.data_publishers.clone().lazy())
        .collect();

    let data_publishers =
        polars::prelude::concat(data_publisher_dfs, UnionArgs::default())?.collect()?;

    // Merge coutnries

    let countries_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.countries.clone().lazy())
        .collect();

    let countries = polars::prelude::concat(countries_dfs, UnionArgs::default())?.collect()?;

    Ok(Metadata {
        metrics,
        geometries,
        source_data_releases,
        data_publishers,
        countries,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn country_metadata_should_load() {
        let metadata = CountryMetadataLoader::new("be").load();
        println!("{metadata:#?}");
        assert!(metadata.is_ok(), "Data should have loaded ok");
    }

    #[test]
    fn all_metadata_should_load() {
        let metadata = load_all(&["be"]);
        println!("{metadata:#?}");
        assert!(metadata.is_ok(), "Data should have loaded ok");
    }

    #[test]
    fn we_should_be_able_to_find_metadata_by_id() {
        let metadata = load_all(&["be"]).unwrap();
        let metrics =
            metadata.get_metric_details("#population+children+age0_17", "municipality", "2022");
        println!("{metrics:#?}");
    }
}
