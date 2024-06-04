use anyhow::{anyhow, Result};
use futures::future::join_all;
use log::info;
use polars::{
    frame::{explode, DataFrame},
    lazy::{
        dsl::{col, Expr},
        frame::{IntoLazy, LazyFrame, ScanArgsParquet},
    },
    prelude::{lit, JoinArgs, JoinType, NamedFrom, UnionArgs},
    series::Series,
};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, default::Default};

use crate::config::Config;
use crate::parquet::MetricRequest;

/// This struct contains the base url and names of
/// the files that contain the metadata. It has a
/// default impl which give the version that we will
/// normally use but this allows us to customise it
/// if we need to.
pub struct CountryMetadataPaths {
    geometry: String,
    metrics: String,
    country: String,
    source_data: String,
    data_publishers: String,
}

/// Represents a way of refering to a metric id
/// can be converted into a polars expression for
/// selection
#[derive(Serialize, Deserialize, PartialEq, Eq, Debug)]
pub enum MetricId {
    /// Hxl (Humanitarian Exchange Language) tag
    Hxl(String),
    /// Internal UUID
    Id(String),
    /// Human Readable name
    CommonName(String),
}

impl MetricId {
    /// Returns the column in the metadata that this id type corrispondes to
    pub fn to_col_name(&self) -> String {
        match self {
            MetricId::Hxl(_) => "hxl_tag".into(),
            MetricId::Id(_) => "id".into(),
            MetricId::CommonName(_) => "human_readable_name".into(),
        }
    }
    /// Return a string representing the textual content of the ID
    pub fn to_query_string(&self) -> &str {
        match self {
            MetricId::Hxl(s) => s,
            MetricId::Id(s) => s,
            MetricId::CommonName(s) => s,
        }
    }

    /// Generate a polars Expr that will do
    /// an exact match on the MetricId
    pub fn to_polars_expr(&self) -> Expr {
        col(&self.to_col_name()).eq(self.to_query_string())
    }

    /// Generate a polars Expr that will generate
    /// a fuzzy search for the content of the Id
    pub fn to_fuzzy_polars_expr(&self) -> Expr {
        col(&self.to_col_name())
            .str()
            .contains(lit(self.to_query_string()), false)
    }
}

impl Into<Expr> for MetricId {
    fn into(self) -> Expr {
        self.to_polars_expr()
    }
}

impl Default for CountryMetadataPaths {
    fn default() -> Self {
        Self {
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
    /// If our metric_id is a regex, expand it in to a list of explicit `MetricIds`
    pub fn expand_wildcard_metric(&self, metric_id: &MetricId) -> Result<Vec<MetricId>> {
        let col_name = metric_id.to_col_name();
        let query = metric_id.to_query_string();
        let catalouge = self.combined_metric_source_geometry();

        catalouge
            .filter(metric_id.to_fuzzy_polars_expr())
            .collect()?
            .column(&col_name)?
            .str()?
            .iter()
            .map(|expanded_id| {
                if let Some(id) = expanded_id {
                    Ok(match metric_id {
                        MetricId::Hxl(_) => MetricId::Hxl(id.into()),
                        MetricId::Id(_) => MetricId::Id(id.into()),
                        MetricId::CommonName(_) => MetricId::CommonName(id.into()),
                    })
                } else {
                    Err(anyhow!("Failed to expand id"))
                }
            })
            .collect()
    }

    /// Generate a Lazy DataFrame which joins the metrics, source and geometry metadata
    pub fn combined_metric_source_geometry(&self) -> LazyFrame {
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

    /// Given a list of metric ids return a Data Frame with the
    /// list of possible matches across years and geometries
    pub fn get_possible_metric_details(&self, metrics: &[MetricId]) -> Result<DataFrame> {
        let mut id_collections: HashMap<String, Vec<String>> = HashMap::new();

        for metric in metrics {
            id_collections
                .entry(metric.to_col_name())
                .and_modify(|e| e.push(metric.to_query_string().into()))
                .or_insert(vec![metric.to_query_string().into()]);
        }

        let mut filter_expression: Option<Expr> = None;
        for (col_name, ids) in &id_collections {
            let filter_series = Series::new("filter", ids.clone());
            filter_expression = if let Some(expression) = filter_expression {
                Some(expression.or(col(col_name).is_in(lit(filter_series))))
            } else {
                Some(col(col_name).is_in(lit(filter_series)))
            };
        }

        let metrics = self
            .combined_metric_source_geometry()
            .filter(filter_expression.unwrap())
            .collect()?;
        Ok(metrics)
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
    pub async fn load(self, config: &Config) -> Result<Metadata> {
        Ok(Metadata {
            metrics: self.load_metadata(&self.paths.metrics, config).await?,
            geometries: self.load_metadata(&self.paths.geometry, config).await?,
            source_data_releases: self.load_metadata(&self.paths.source_data, config).await?,
            data_publishers: self
                .load_metadata(&self.paths.data_publishers, config)
                .await?,
            countries: self.load_metadata(&self.paths.country, config).await?,
        })
    }

    /// Performs a load of a given metadata parquet file
    async fn load_metadata(&self, path: &str, config: &Config) -> Result<DataFrame> {
        let full_path = format!("{}/{}/{path}", config.base_path, self.country);
        let args = ScanArgsParquet::default();
        info!("Attempting to load {full_path}");
        let df: DataFrame = tokio::task::spawn_blocking(move || {
            LazyFrame::scan_parquet(&full_path, args)?
                .collect()
                .map_err(|e| anyhow!("Failed to load '{full_path}': {e}"))
        })
        .await??;
        Ok(df)
    }
}

/// Load the metadata for a list of countries and merge them into
/// a single `Metadata` catalouge.
pub async fn load_all(config: &Config) -> Result<Metadata> {
    let country_text_file = format!("{}/countries.txt", config.base_path);
    let country_names: Vec<String> = reqwest::Client::new()
        .get(&country_text_file)
        .send()
        .await?
        .text()
        .await?
        .lines()
        .map(|s| s.to_string())
        .collect();
    info!("Detected country names: {:?}", country_names);

    let metadata: Result<Vec<Metadata>> = join_all(
        country_names
            .iter()
            .map(|c| CountryMetadataLoader::new(c).load(config)),
    )
    .await
    .into_iter()
    .collect();
    let metadata = metadata?;

    // Merge metrics
    // to_supertypes is set to true here because for BE, source_archive_file_path is always a
    // string, but for NI source_archive_file_path is always null and polars infers the column type
    // to be null. If merged directly polars will error as the types are incompatible. Setting
    // to_supertypes to true lets polars concatenate to a single string-type column.
    let metric_dfs: Vec<LazyFrame> = metadata.iter().map(|m| m.metrics.clone().lazy()).collect();
    let metrics = polars::prelude::concat(
        metric_dfs,
        UnionArgs {
            to_supertypes: true,
            ..Default::default()
        },
    )?
    .collect()?;
    info!("Merged metrics with shape: {:?}", metrics.shape());

    // Merge geometries
    let geometries_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.geometries.clone().lazy())
        .collect();
    let geometries = polars::prelude::concat(geometries_dfs, UnionArgs::default())?.collect()?;
    info!("Merged geometries with shape: {:?}", geometries.shape());

    // Merge source data relaeses
    let source_data_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.source_data_releases.clone().lazy())
        .collect();

    let source_data_releases =
        polars::prelude::concat(source_data_dfs, UnionArgs::default())?.collect()?;
    info!(
        "Merged source data releases with shape: {:?}",
        source_data_releases.shape()
    );

    // Merge source data publishers
    let data_publisher_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.data_publishers.clone().lazy())
        .collect();

    let data_publishers =
        polars::prelude::concat(data_publisher_dfs, UnionArgs::default())?.collect()?;
    info!(
        "Merged data publishers with shape: {:?}",
        data_publishers.shape()
    );

    // Merge countries
    let countries_dfs: Vec<LazyFrame> = metadata
        .iter()
        .map(|m| m.countries.clone().lazy())
        .collect();
    let countries = polars::prelude::concat(countries_dfs, UnionArgs::default())?.collect()?;
    info!("Merged countries with shape: {:?}", countries.shape());

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
    use geo::Extremes;

    use super::*;
    /// TODO stub out a mock here that we can use to test with.

    #[tokio::test]
    async fn country_metadata_should_load() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("be").load(&config).await;
        println!("{metadata:#?}");
        assert!(metadata.is_ok(), "Data should have loaded ok");
    }

    #[tokio::test]
    async fn all_metadata_should_load() {
        let config = Config::default();
        let metadata = load_all(&config).await;
        println!("{metadata:#?}");
        assert!(metadata.is_ok(), "Data should have loaded ok");
    }

    #[tokio::test]
    async fn metric_ids_should_expand_properly() {
        let config = Config::default();
        let metadata = load_all(&config).await.unwrap();
        let expanded_metrics =
            metadata.expand_wildcard_metric(&MetricId::Hxl("population-*".into()));
        assert!(
            expanded_metrics.is_ok(),
            "Should successfully expand metrics"
        );
        let expanded_metrics = expanded_metrics.unwrap();

        assert_eq!(
            expanded_metrics.len(),
            7,
            "should return the correct number of metrics"
        );

        let metric_names: Vec<&str> = expanded_metrics
            .iter()
            .map(|m| m.to_query_string())
            .collect();

        assert_eq!(
            metric_names,
            vec![
                "#population+children+age5_17",
                "#population+infants+age0_4",
                "#population+children+age0_17",
                "#population+adults+f",
                "#population+adults+m",
                "#population+adults",
                "#population+ind"
            ],
            "should get the correct metrics"
        );
    }

    #[tokio::test]
    async fn human_readable_metric_ids_should_expand_properly() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("be").load(&config).await.unwrap();
        let expanded_metrics =
            metadata.expand_wildcard_metric(&MetricId::CommonName("Children*".into()));

        println!("{:#?}", expanded_metrics);

        assert!(
            expanded_metrics.is_ok(),
            "Should successfully expand metrics"
        );

        let expanded_metrics = expanded_metrics.unwrap();

        assert_eq!(
            expanded_metrics.len(),
            2,
            "should return the correct number of metrics"
        );

        let metric_names: Vec<&str> = expanded_metrics
            .iter()
            .map(|m| m.to_query_string())
            .collect();

        assert_eq!(
            metric_names,
            vec!["Children aged 5 to 17", "Children aged 0 to 17"],
            "should get the correct metrics"
        );
    }

    #[tokio::test]
    async fn fully_defined_metric_ids_should_expand_to_itself() {
        let config = Config::default();
        let metadata = CountryMetadataLoader::new("be").load(&config).await.unwrap();
        let expanded_metrics = metadata
            .expand_wildcard_metric(&MetricId::Hxl(r"#population\+infants\+age0\_4".into()));
        assert!(
            expanded_metrics.is_ok(),
            "Should successfully expand metrics"
        );
        let expanded_metrics = expanded_metrics.unwrap();

        assert_eq!(
            expanded_metrics.len(),
            1,
            "should return the correct number of metrics"
        );

        let metric_names: Vec<&str> = expanded_metrics
            .iter()
            .map(|m| m.to_query_string())
            .collect();

        assert_eq!(
            metric_names,
            vec!["#population+infants+age0_4",],
            "should get the correct metrics"
        );

        println!("{:#?}", expanded_metrics);
    }
}
