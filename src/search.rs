//! Search

use polars::lazy::dsl::{col, lit, Expr};

/// Ways of searching metadata: the approach is that you can provide
/// a set of optional filters that can be composed to produce the
/// overall search query:
///  - Some sort of fuzzy string match on hxltags, human readable
///    name and descriptions (may want to subset place to search
///    for the text, e.g. only search hxltags)
///  - Country
///  - Census table name (e.g. identifier on source website)
///  - Information about source release
///  - Information about data publisher
///  - Time (year of census)
///  - Geo level
use crate::metadata::MetricId;

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
// TODO: consider try from to fix unwrap for empty query
impl From<SearchText> for Expr {
    fn from(search_text: SearchText) -> Self {
        let mut query: Option<Expr> = None;
        for field in search_text.context {
            let sub_query = match field {
                SearchContext::Hxl => col("hxl_tag"),
                SearchContext::HumanReadableName => col("human_readable_name"),
                SearchContext::Description => col("h"),
            }
            .eq(lit(search_text.text.clone()));

            query = if let Some(partial_query) = query {
                Some(partial_query.or(sub_query))
            } else {
                Some(sub_query)
            };
        }
        query.unwrap()
    }
}
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
struct Year(pub Vec<String>);

/// To allow search over multiple years
struct GeometryLevel(pub Vec<String>);

/// Source data release: set of strings that will search over this
struct SourceDataRelease(pub Vec<String>);

/// Data publisher: set of strings that will search over this
struct DataPublisher(pub Vec<String>);

/// Countries: set of countries to be included in the search
struct Country(pub Vec<String>);

/// Census tables: set of census tables to be included in the search
struct CensusTable(pub Vec<String>);

struct SearchRequest {
    pub text: Option<SearchText>,
    pub year: Option<Year>,
    pub geometry_level: Option<GeometryLevel>,
    pub source_data_release: Option<SourceDataRelease>,
    pub data_publisher: Option<DataPublisher>,
    pub country: Option<Country>,
    pub census_table: Option<CensusTable>,
}

impl SearchRequest {
    fn with_country(self, country: &str) -> Self {
        todo!()
    }
    fn with_(&mut self, country: &str) -> Self {
        todo!()
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
