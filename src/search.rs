//! Search

use polars::lazy::dsl::{col, lit, Expr};
use polars::frame::DataFrame;
use serde::{Deserialize, Serialize};
use crate::metadata::{MetricId, Metadata};

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

// TODO: wrapping trait for From conversions


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
                SearchContext::Description => col("description"),
            }
            .eq(lit(search_text.text.clone()));
            // TODO (enhancement): fuzzy matching crate (e.g. https://github.com/bnm3k/polars-fuzzy-match/blob/master/Cargo.toml)
            // Fuzzy string version
            // .str()?.contains(lit(search_text.text.clone()));

            query = if let Some(partial_query) = query {
                Some(partial_query.or(sub_query))
            } else {
                Some(sub_query)
            };
        }
        query.unwrap()
    }
}

impl From<Year> for Expr {
    fn from(value: Year) -> Self {
        todo!()
    }
}

impl From<DataPublisher> for Expr {
    fn from(value: DataPublisher) -> Self {
        todo!()
    }
}

impl From<SourceDataRelease> for Expr {
    fn from(value: SourceDataRelease) -> Self {
        todo!()
    }
}

impl From<GeometryLevel> for Expr {
    fn from(value: GeometryLevel) -> Self {
        todo!()
    }
}

impl From<Country> for Expr {
    fn from(value: Country) -> Self {
        todo!()
    }
}

impl From<CensusTable> for Expr {
    fn from(value: CensusTable) -> Self {
        todo!()
    }
}


/// Search text is th
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

/// Trait to enable collections of search request fields (not currently used)
trait SearchRequestField {}
impl SearchRequestField for Year {}
impl SearchRequestField for GeometryLevel {}
impl SearchRequestField for SearchText {}
impl SearchRequestField for SourceDataRelease {}
impl SearchRequestField for DataPublisher {}
impl SearchRequestField for Country {}
impl SearchRequestField for CensusTable {}

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
struct CensusTable(pub Vec<String>);


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
#[derive(Clone, Debug, Deserialize, Serialize)]
struct SearchRequest {
    pub text: Option<SearchText>,
    pub year: Option<Year>,
    pub geometry_level: Option<GeometryLevel>,
    pub source_data_release: Option<SourceDataRelease>,
    pub data_publisher: Option<DataPublisher>,
    pub country: Option<Country>,
    pub census_table: Option<CensusTable>,

    // TODO (possible enhancement): refactor field to `Expr`
    // - search_fields: Vec<Box<dyn SearchRequestField>>>
    // - Also enum dispatch pattren in the commands for CLI
}

impl SearchRequest {
    fn with_country(self, country: &str) -> Self {
        todo!()
    }
    fn with_data_publisher(&mut self, country: &str) -> Self {
        todo!()
    }
    // TODO: add other methods for builder 


    // TODO: Perform filtering on full metadata catalog
    fn search_results(self, metadata: &Metadata) -> anyhow::Result<SearchResults>{
        let result: DataFrame = metadata.combined_metric_source_geometry().filter(self.into()).collect()?;
        Ok(SearchResults(result))
    }

}

struct SearchResults(pub DataFrame);



// impl std::fmt::Display for SearchResults {
//     // TODO: display method
    
//     todo!()
// }


fn combine_queries<T>(q1_opt: Option<Expr>, q2_opt: Option<T>) -> Option<Expr>
where T : Into<Expr> {
    match (q1_opt, q2_opt) {
        (Some(q1), Some(q2)) => Some(q1.and(q2.into())),
        (Some(q1), None) => Some(q1),
        (None, Some(q2)) => Some(q2.into()),
        (None, None) => None,
    }
}


// impl TryFrom<SearchRequest> for Expr {
impl From<SearchRequest> for Expr {
    fn from(value: SearchRequest) -> Self {
        let mut query: Option<Expr> = None;
        
        query = combine_queries(query, value.text);
        query = combine_queries(query, value.year);
        query = combine_queries(query, value.geometry_level);
        query = combine_queries(query, value.source_data_release);
        query = combine_queries(query, value.data_publisher);
        query = combine_queries(query, value.country);
        query = combine_queries(query, value.census_table);
        query.unwrap()
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
