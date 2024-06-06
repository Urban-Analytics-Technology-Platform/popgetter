use anyhow::{anyhow, Result};

use polars::{
    chunked_array::ops::SortMultipleOptions,
    frame::DataFrame,
    lazy::{
        dsl::{col, Expr},
        frame::{IntoLazy, LazyFrame, ScanArgsParquet},
    },
    prelude::{lit, JoinArgs, JoinType, NamedFrom, UnionArgs},
    series::Series,
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, default::Default, fmt::Display};

use crate::{data_request_spec::GeometrySpec, parquet::MetricRequest};

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
            MetricId::CommonName(s) | MetricId::Id(s) | MetricId::Hxl(s) => s,
        }
    }

    /// Generate a polars Expr that will do
    /// an exact match on the MetricId
    pub fn to_polars_expr(&self) -> Expr {
        col(&self.to_col_name()).eq(self.to_query_string())
    }

    /// Generate a polars Expr that will generate
    /// a regex search for the content of the Id
    pub fn to_fuzzy_polars_expr(&self) -> Expr {
        col(&self.to_col_name())
            .str()
            .contains(lit(self.to_query_string()), false)
    }
}

impl From<MetricId> for Expr {
    fn from(value: MetricId) -> Self {
        value.to_polars_expr()
    }
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

pub struct ExpandedMetdataTable(pub LazyFrame);

impl ExpandedMetdataTable {
    pub fn as_df(&self) -> LazyFrame {
        self.0.clone()
    }

    pub fn select_metrics(&self, metrics: &[MetricId]) -> Self {
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

        ExpandedMetdataTable(self.as_df().filter(filter_expression.unwrap()))
    }

    pub fn to_metric_requests(&self) -> Result<Vec<MetricRequest>> {
        let df = self
            .as_df()
            .select([col("parquet_metric_file"), col("parquet_metric_id")])
            .collect()?;

        let metric_requests: Vec<MetricRequest> = df
            .column("parquet_metric_id")?
            .str()?
            .into_iter()
            .zip(df.column("parquet_metric_file")?.str()?)
            .filter_map(|(column, file)| {
                if let (Some(column), Some(file)) = (column, file) {
                    Some(MetricRequest {
                        column: column.to_owned(),
                        file: file.to_owned(),
                    })
                } else {
                    None
                }
            })
            .collect();
        Ok(metric_requests)
    }

    pub fn select_geometry(&self, geometry: &str) -> Self {
        ExpandedMetdataTable(self.as_df().filter(col("geometry_level").eq(geometry)))
    }
    pub fn select_years<T>(&self, years: &[T]) -> Self
    where
        T: AsRef<str>,
    {
        let years: Vec<&str> = years.iter().map(std::convert::AsRef::as_ref).collect();
        let years_series = Series::new("years", years);
        ExpandedMetdataTable(self.as_df().filter(col("year").is_in(lit(years_series))))
    }

    /// Return a ranked list of avaliable geometries
    pub fn avaliable_geometries(&self) -> Result<Vec<String>> {
        let df = self.as_df();
        let counts: DataFrame = df
            .group_by([col("geometry_level")])
            .agg([col("goemetry_level").count().alias("count")])
            .sort(
                ["count"],
                SortMultipleOptions::new().with_order_descending(true),
            )
            .collect()?;

        Ok(counts
            .column("geometry_level")?
            .str()?
            .iter()
            .filter_map(|geom| geom.map(std::borrow::ToOwned::to_owned))
            .collect())
    }

    /// Return a ranked list of avaliable years
    pub fn avaliable_years(&self) -> Result<Vec<String>> {
        let df = self.as_df();
        let counts: DataFrame = df
            .group_by([col("year")])
            .agg([col("year").count().alias("count")])
            .sort(
                ["count"],
                SortMultipleOptions::new().with_order_descending(true),
            )
            .collect()?;

        Ok(counts
            .column("year")?
            .str()?
            .iter()
            .filter_map(|geom| geom.map(std::borrow::ToOwned::to_owned))
            .collect())
    }

    /// Get fully speced metric ids
    pub fn get_explicit_metric_ids(&self) -> Result<Vec<MetricId>> {
        let reamining: DataFrame = self.as_df().select([col("metric_id")]).collect()?;
        Ok(reamining
            .column("id")?
            .str()?
            .into_iter()
            .filter_map(|pos_id| pos_id.map(|id| MetricId::Id(id.to_owned())))
            .collect())
    }
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

pub struct FullSelectionPlan {
    pub explicit_metric_ids: Vec<MetricId>,
    pub geometry: String,
    pub year: Vec<String>,
    pub advice: String,
}

impl Display for FullSelectionPlan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Getting {} metrics \n, on {} geometries \n , for the years {}",
            self.explicit_metric_ids.len(),
            self.geometry,
            self.year.join(",")
        )
    }
}

impl Metadata {
    /// If our metric_id is a regex, expand it in to a list of explicit `MetricIds`
    pub fn expand_regex_metric(&self, metric_id: &MetricId) -> Result<Vec<MetricId>> {
        let col_name = metric_id.to_col_name();
        let query = metric_id.to_query_string();
        let catalouge = self.combined_metric_source_geometry();

        catalouge
            .as_df()
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
    fn combined_metric_source_geometry(&self) -> ExpandedMetdataTable {
        // Join with source_data_release and geometry
        ExpandedMetdataTable(
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
                ),
        )
    }

    pub fn get_metric_requests(&self, metric_ids: Vec<MetricId>) -> Result<Vec<MetricRequest>> {
        self.combined_metric_source_geometry()
            .select_metrics(&metric_ids);
        Ok(vec![])
    }

    pub fn generate_selection_plan(
        &self,
        metrics: &[MetricId],
        geometry: &GeometrySpec,
        years: &Option<Vec<String>>,
    ) -> Result<FullSelectionPlan> {
        let mut advice: Vec<String> = vec![];
        // Find metadata for all specified metrics over all geoemtries and years
        let possible_metrics = self
            .combined_metric_source_geometry()
            .select_metrics(metrics);

        // If the user has selected a geometry, we will use it explicitly
        let selected_geometry = if let Some(geom) = &geometry.geometry_level {
            geom.clone()
        }
        // Otherwise we will get the geometry with the most matches to our
        // metrics
        else {
            // Get a ranked list of geometriesthat are avaliable for these
            // metrics
            let avaliable_geometries = possible_metrics.avaliable_geometries()?;
            if avaliable_geometries.is_empty() {
                return Err(anyhow!(
                    "No geometry specifed and non found for these metrics"
                ));
            }

            let geom = avaliable_geometries[0].to_owned();
            if avaliable_geometries.len() > 1 {
                let rest = avaliable_geometries[1..].join(",");
                advice.push(format!("We are selecting the geometry level {geom}. The requested metrics are also avaliable at the following levels: {rest}"));
            }
            geom
        };

        // If the user has selected a set of years, we will use them explicity
        let selected_years = if let Some(years) = years {
            years.clone()
        } else {
            let mut avaliable_years = possible_metrics
                .select_geometry(&selected_geometry)
                .avaliable_years()?;

            if avaliable_years.is_empty() {
                return Err(anyhow!(
                    "No year specified and no year matches found given the geometry level {selected_geometry}"
                ));
            }
            let year = avaliable_years[0].to_owned();
            if avaliable_years.len() > 1 {
                let rest = avaliable_years[1..].join(",");
                advice.push(format!("We automatically selected the year {year}. The requested metrics are also avaiable in the follow time spans {rest}"));
            }
            vec![year]
        };

        let metrics = possible_metrics
            .select_geometry(&selected_geometry)
            .select_years(&selected_years)
            .get_explicit_metric_ids()?;

        Ok(FullSelectionPlan {
            explicit_metric_ids: metrics,
            geometry: selected_geometry,
            year: selected_years,
            advice: advice.join("\n"),
        })
    }

    /// Given a geometry level return the path to the
    /// geometry file that it corresponds to
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
            .ok_or(anyhow!("Matches does not contain 'filename_stem' column"))?
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
        println!("Attempting to load {url}");
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

    // Merge countries
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
    /// TODO stub out a mock here that we can use to test with.

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
    fn metric_ids_should_expand_properly() {
        let metadata = load_all(&["be"]).unwrap();
        let expanded_metrics = metadata.expand_regex_metric(&MetricId::Hxl("population-*".into()));
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
            .map(metadata::MetricId::to_query_string)
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

    #[test]
    fn human_readable_metric_ids_should_expand_properly() {
        let metadata = load_all(&["be"]).unwrap();
        let expanded_metrics =
            metadata.expand_regex_metric(&MetricId::CommonName("Children*".into()));

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
            .map(MetricId::to_query_string)
            .collect();

        assert_eq!(
            metric_names,
            vec!["Children aged 5 to 17", "Children aged 0 to 17"],
            "should get the correct metrics"
        );
    }
    #[test]
    fn fully_defined_metric_ids_should_expand_to_itself() {
        let metadata = load_all(&["be"]).unwrap();
        let expanded_metrics =
            metadata.expand_regex_metric(&MetricId::Hxl(r"#population\+infants\+age0\_4".into()));
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
            .map(MetricId::to_query_string)
            .collect();

        assert_eq!(
            metric_names,
            vec!["#population+infants+age0_4",],
            "should get the correct metrics"
        );

        println!("{:#?}", expanded_metrics);
    }
}
