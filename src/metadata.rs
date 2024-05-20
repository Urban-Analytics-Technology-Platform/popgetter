use anyhow::{anyhow, Result};
use polars::{
    frame::DataFrame,
    lazy::frame::{IntoLazy, LazyFrame, ScanArgsParquet},
    prelude::{col, lit, UnionArgs},
};
use std::default::Default;

use crate::parquet::MetricRequest;

pub struct CountryMetadataPaths {
    base_url: String,
    geometry: String,
    metrics: String,
    country: String,
    sourceData: String,
    dataPublishers: String,
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
            sourceData: "source_data_releases.parquet".into(),
            dataPublishers: "data_publishers.parquet".into(),
        }
    }
}

pub struct CountryMetadataLoader {
    country: String,
    paths: CountryMetadataPaths,
}

#[derive(Debug)]
pub struct Metadata {
    pub metrics: DataFrame,
    pub geometries: DataFrame,
    pub source_data_releases: DataFrame,
    pub data_publishers: DataFrame,
    pub countries: DataFrame,
}

impl Metadata {
    pub fn get_metric_details(&self, metric_id: &str) -> Result<MetricRequest> {
        let matches = self
            .metrics
            .clone()
            .lazy()
            .filter(col("hxl_tag").eq(lit(metric_id)))
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
    pub fn new(country: &str) -> Self {
        let paths: CountryMetadataPaths = Default::default();
        Self {
            country: country.into(),
            paths,
        }
    }

    pub fn with_paths(&mut self, paths: CountryMetadataPaths) -> &mut Self {
        self.paths = paths;
        self
    }

    pub fn load(&self) -> Result<Metadata> {
        Ok(Metadata {
            metrics: self.load_metadata(&self.paths.metrics)?,
            geometries: self.load_metadata(&self.paths.geometry)?,
            source_data_releases: self.load_metadata(&self.paths.sourceData)?,
            data_publishers: self.load_metadata(&self.paths.dataPublishers)?,
            countries: self.load_metadata(&self.paths.country)?,
        })
    }

    fn load_metadata(&self, path: &str) -> Result<DataFrame> {
        let url = format!("{}/{}/{path}", self.paths.base_url, self.country);
        let args = ScanArgsParquet::default();
        println!("Attempting to load {url}");
        let df: DataFrame = LazyFrame::scan_parquet(url, args)?.collect()?;
        Ok(df)
    }
}

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
        let metrics = metadata.get_metric_details("#population+children+age0_17");
        println!("{metrics:#?}");
    }
}
