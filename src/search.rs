//! Search

use crate::metadata::Metadata;
use polars::lazy::dsl::{col, lit, Expr};
use polars::prelude::{DataFrame, LazyFrame};
use serde::{Deserialize, Serialize};

/// Combine multiple queries with OR. If there are no queries in the input list, returns None.
fn combine_exprs_with_or(exprs: Vec<Expr>) -> Option<Expr> {
    let mut query: Option<Expr> = None;
    for expr in exprs {
        query = if let Some(partial_query) = query {
            Some(partial_query.or(expr))
        } else {
            Some(expr)
        };
    }
    query
}

/// Combine multiple queries with AND. If there are no queries in the input list, returns None.
fn combine_exprs_with_and(exprs: Vec<Expr>) -> Option<Expr> {
    let mut query: Option<Expr> = None;
    for expr in exprs {
        query = if let Some(partial_query) = query {
            Some(partial_query.and(expr))
        } else {
            Some(expr)
        };
    }
    query
}

#[derive(Clone, Debug, Deserialize, Serialize)]
enum SearchContext {
    Hxl,
    HumanReadableName,
    Description,
}

impl SearchContext {
    fn all() -> Vec<Self> {
        vec![Self::Hxl, Self::HumanReadableName, Self::Description]
    }
}

/// Implementing conversion from `SearchText` to a polars expression enables a
/// `SearchText` to be passed to polars dataframe for filtering results.
impl From<SearchText> for Option<Expr> {
    fn from(val: SearchText) -> Self {
        let queries = val
            .context
            .into_iter()
            .map(|field| {
                match field {
                    SearchContext::Hxl => col("hxl_tag"),
                    SearchContext::HumanReadableName => col("human_readable_name"),
                    SearchContext::Description => col("description"),
                }
                .eq(lit(val.text.clone()))
            })
            .collect();
        combine_exprs_with_or(queries)
    }
}

impl From<Year> for Option<Expr> {
    fn from(value: Year) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("year").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<DataPublisher> for Option<Expr> {
    fn from(value: DataPublisher) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("data_publisher").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<SourceDataRelease> for Option<Expr> {
    fn from(value: SourceDataRelease) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("source_data_release").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<GeometryLevel> for Option<Expr> {
    fn from(value: GeometryLevel) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("geometry_level").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<Country> for Option<Expr> {
    fn from(value: Country) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("country").eq(lit(val)))
                .collect(),
        )
    }
}

impl From<SourceMetricId> for Option<Expr> {
    fn from(value: SourceMetricId) -> Self {
        combine_exprs_with_or(
            value
                .0
                .into_iter()
                .map(|val| col("source_metric_id").eq(lit(val)))
                .collect(),
        )
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
struct SearchText {
    pub text: String,
    pub context: Vec<SearchContext>,
}

impl Default for SearchText {
    fn default() -> Self {
        Self {
            text: "".to_string(),
            context: SearchContext::all(),
        }
    }
}

// Whether year is string or int has implications with how it's encoded in the dfs
// TODO: open ticket to capture how to progress this
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Year(pub Vec<String>);

/// To allow search over multiple years
#[derive(Clone, Debug, Deserialize, Serialize)]
struct GeometryLevel(pub Vec<String>);

/// Source data release: set of strings that will search over this
#[derive(Clone, Debug, Deserialize, Serialize)]
struct SourceDataRelease(pub Vec<String>);

/// Data publisher: set of strings that will search over this
#[derive(Clone, Debug, Deserialize, Serialize)]
struct DataPublisher(pub Vec<String>);

/// Countries: set of countries to be included in the search
#[derive(Clone, Debug, Deserialize, Serialize)]
struct Country(pub Vec<String>);

/// Census tables: set of census tables to be included in the search
#[derive(Clone, Debug, Deserialize, Serialize)]
struct SourceMetricId(pub Vec<String>);

/// Ways of searching metadata: the approach is that you can provide
/// a set of optional filters that can be composed to produce the
/// overall search query:
///  - Some sort of fuzzy string match on hxltags, human readable
///    name or descriptions (may want to subset place to search
///    for the text, e.g. only search hxltags)
///  - Country
///  - Census table name (e.g. identifier on source website)
///  - Information about source release
///  - Information about data publisher
///  - Time (year of census)
///  - Geo level
#[derive(Clone, Debug, Deserialize, Serialize)]
struct SearchRequest {
    pub text: Option<SearchText>,
    pub year: Option<Year>,
    pub geometry_level: Option<GeometryLevel>,
    pub source_data_release: Option<SourceDataRelease>,
    pub data_publisher: Option<DataPublisher>,
    pub country: Option<Country>,
    pub census_table: Option<SourceMetricId>,
    // TODO (possible enhancement): refactor field to `Expr`
    // - search_fields: Vec<Box<dyn SearchRequestField>>>
    // - Also enum dispatch pattren in the commands for CLI
}

impl SearchRequest {
    pub fn new() -> Self {
        Self {
            text: None,
            year: None,
            geometry_level: None,
            source_data_release: None,
            data_publisher: None,
            country: None,
            census_table: None,
        }
    }

    pub fn with_country(mut self, country: &str) -> Self {
        self.country = Some(Country(vec![country.to_string()]));
        self
    }

    pub fn with_data_publisher(mut self, data_publisher: &str) -> Self {
        self.data_publisher = Some(DataPublisher(vec![data_publisher.to_string()]));
        self
    }

    pub fn with_source_data_release(mut self, source_data_release: &str) -> Self {
        self.source_data_release = Some(SourceDataRelease(vec![source_data_release.to_string()]));
        self
    }

    pub fn with_year(mut self, year: &str) -> Self {
        self.year = Some(Year(vec![year.to_string()]));
        self
    }

    pub fn with_geometry_level(mut self, geometry_level: &str) -> Self {
        self.geometry_level = Some(GeometryLevel(vec![geometry_level.to_string()]));
        self
    }

    pub fn with_census_table(mut self, census_table: &str) -> Self {
        self.census_table = Some(SourceMetricId(vec![census_table.to_string()]));
        self
    }

    pub fn search_results(self, metadata: &Metadata) -> anyhow::Result<SearchResults> {
        let expr: Option<Expr> = self.into();
        let full_results: LazyFrame = metadata.combined_metric_source_geometry();
        let result: DataFrame = match expr {
            Some(expr) => full_results.filter(expr),
            None => full_results,
        }
        .collect()?;
        Ok(SearchResults(result))
    }
}

struct SearchResults(pub DataFrame);

// impl std::fmt::Display for SearchResults {
//     // TODO: display method

//     todo!()
// }

impl From<SearchRequest> for Option<Expr> {
    fn from(value: SearchRequest) -> Self {
        let subexprs: Vec<Option<Expr>> = vec![
            value.text?.into(),
            value.year?.into(),
            value.geometry_level?.into(),
            value.source_data_release?.into(),
            value.data_publisher?.into(),
            value.country?.into(),
            value.census_table?.into(),
        ];
        // Remove the Nones and unwrap the Somes
        let valid_subexprs: Vec<Expr> = subexprs.into_iter().flatten().collect();
        combine_exprs_with_and(valid_subexprs)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn test_search_request() {
    //     let mut sr = SearchRequest{search_string: None}.with_country("a").with_country("b");
    // }
}
