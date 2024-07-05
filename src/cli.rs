// FromStr is required by EnumString. The compiler seems to not be able to
// see that and so is giving a warning. Dont remove it
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use enum_dispatch::enum_dispatch;
use log::{debug, info};
use nonempty::nonempty;
use popgetter::{
    config::Config,
    formatters::{
        CSVFormatter, GeoJSONFormatter, GeoJSONSeqFormatter, OutputFormatter, OutputGenerator,
    },
    geo::BBox,
    search::{
        Country, DataPublisher, GeometryLevel, MetricId, SearchContext, SearchParams, SearchText,
        SourceDataRelease, SourceMetricId, YearRange,
    },
    Popgetter,
};
use serde::{Deserialize, Serialize};
use std::fs::File;
use strum_macros::EnumString;

use crate::display::display_search_results;

/// Defines the output formats we are able to produce data in.
#[derive(Clone, Debug, Deserialize, Serialize, EnumString, PartialEq, Eq)]
#[strum(ascii_case_insensitive)]
pub enum OutputFormat {
    GeoJSON,
    GeoJSONSeq,
    Csv,
    GeoParquet,
    FlatGeobuf,
    Stdout,
}

/// Trait that defines what to run when a given subcommand is invoked.
#[enum_dispatch]
pub trait RunCommand {
    async fn run(&self, config: Config) -> Result<()>;
}

/// The `data` command downloads and outputs metrics for a given region in a given format.
#[derive(Args, Debug)]
pub struct DataCommand {
    #[arg(
        short,
        long,
        value_name = "MIN_LAT,MIN_LNG,MAX_LAT,MAX_LNG",
        help = "Bounding box in which to get the results"
    )]
    bbox: Option<BBox>,
    #[arg(
        short = 'f',
        long,
        value_name = "geojson|geojsonseq|csv",
        help = "Output format for the results"
    )]
    output_format: OutputFormat,
    #[arg(short = 'o', long, help = "Output file to place the results")]
    output_file: Option<String>,
    #[command(flatten)]
    search_params_args: SearchParamsArgs,
}

impl From<&OutputFormat> for OutputFormatter {
    fn from(value: &OutputFormat) -> Self {
        match value {
            OutputFormat::GeoJSON => OutputFormatter::GeoJSON(GeoJSONFormatter),
            OutputFormat::Csv => OutputFormatter::Csv(CSVFormatter::default()),
            OutputFormat::GeoJSONSeq => OutputFormatter::GeoJSONSeq(GeoJSONSeqFormatter),
            OutputFormat::Stdout => OutputFormatter::Csv(CSVFormatter::default()),
            _ => todo!("output format not implemented"),
        }
    }
}

impl From<OutputFormat> for OutputFormatter {
    fn from(value: OutputFormat) -> Self {
        Self::from(&value)
    }
}

impl RunCommand for DataCommand {
    async fn run(&self, config: Config) -> Result<()> {
        info!("Running `data` subcommand");

        let popgetter = Popgetter::new_with_config(config).await?;

        let mut data = popgetter
            .search(self.search_params_args.clone().into())
            .download(&popgetter.config)
            .await?;

        debug!("{data:#?}");

        let formatter: OutputFormatter = (&self.output_format).into();
        if let Some(output_file) = &self.output_file {
            let mut f = File::open(output_file)?;
            formatter.save(&mut f, &mut data)?;
        } else {
            let stdout = std::io::stdout();
            let mut stdout_lock = stdout.lock();
            formatter.save(&mut stdout_lock, &mut data)?;
        };
        Ok(())
    }
}

/// The Metrics command allows a user to search for a set of metrics by bounding box and filter.
/// The set of ways to search will likley increase over time
#[derive(Args, Debug)]
pub struct MetricsCommand {
    #[arg(
        short,
        long,
        value_name = "MIN_LAT,MIN_LNG,MAX_LAT,MAX_LNG",
        help = "Bounding box in which to get the results"
    )]
    bbox: Option<BBox>,
    #[arg(
        short,
        long,
        help = "Show all metrics even if there are a large number"
    )]
    full: bool,
    #[command(flatten)]
    search_params_args: SearchParamsArgs,
}

/// These are the command-line arguments that can be parsed into a SearchParams. The type is
/// slightly different because of the way we allow people to search in text fields.
#[derive(Args, Debug, Clone)]
struct SearchParamsArgs {
    // Note: using `std::vec::Vec` rather than just `Vec`, to enforce that multiple year ranges are
    // passed in a single argument e.g. `-y 2014...2016,2018...2019` rather than multiple arguments
    // e.g. `-y 2014...2016 -y 2018...2019`. See https://github.com/clap-rs/clap/issues/4626 and
    // https://docs.rs/clap/latest/clap/_derive/index.html#arg-types
    #[arg(
        short,
        long,
        help = "Filter by year ranges. All ranges are inclusive; multiple ranges can be comma-separated.",
        value_name = "YEAR|START...|...END|START...END",
        value_parser = parse_year_range,
    )]
    year_range: Option<std::vec::Vec<YearRange>>,
    #[arg(short, long, help = "Filter by geometry level")]
    geometry_level: Option<String>,
    #[arg(short, long, help = "Filter by source data release name")]
    source_data_release: Option<String>,
    #[arg(short, long, help = "Filter by data publisher name")]
    publisher: Option<String>,
    #[arg(short, long, help = "Filter by country")]
    country: Option<String>,
    #[arg(
        long,
        help = "Filter by source metric ID (i.e. the name of the table in the original data release)"
    )]
    source_metric_id: Option<String>,
    #[arg(
        short = 'i',
        long,
        help = "Specify a metric by its popgetter ID (or a prefix thereof)"
    )]
    id: Vec<String>,
    // Filters for text
    #[arg(long, help="Filter by HXL tag", num_args=0..)]
    hxl: Vec<String>,
    #[arg(long, help="Filter by metric name", num_args=0..)]
    name: Vec<String>,
    #[arg(long, help="Filter by metric description", num_args=0..)]
    description: Vec<String>,
    #[arg(short, long, help="Filter by HXL tag, name, or description", num_args=0..)]
    text: Vec<String>,
}

/// Expected behaviour:
/// N.. -> After(N); ..N -> Before(N); M..N -> Between(M, N); N -> Between(N, N)
/// Year ranges can be comma-separated
fn parse_year_range(value: &str) -> Result<Vec<YearRange>, &'static str> {
    fn parse_single_year_range(value: &str) -> Result<YearRange, &'static str> {
        fn str_to_option_u16(value: &str) -> Result<Option<u16>, &'static str> {
            if value.is_empty() {
                return Ok(None);
            }
            match value.parse::<u16>() {
                Ok(value) => Ok(Some(value)),
                Err(_) => Err("Invalid year range"),
            }
        }
        let parts: Vec<Option<u16>> = value
            .split("...")
            .map(str_to_option_u16)
            .collect::<Result<Vec<Option<u16>>, &'static str>>()?;
        match parts.as_slice() {
            [Some(a)] => Ok(YearRange::Between(*a, *a)),
            [None, Some(a)] => Ok(YearRange::Before(*a)),
            [Some(a), None] => Ok(YearRange::After(*a)),
            [Some(a), Some(b)] => {
                if a > b {
                    Err("Invalid year range")
                } else {
                    Ok(YearRange::Between(*a, *b))
                }
            }
            _ => Err("Invalid year range"),
        }
    }
    value
        .split(',')
        .map(parse_single_year_range)
        .collect::<Result<Vec<YearRange>, &'static str>>()
}

fn text_searches_from_args(
    hxl: Vec<String>,
    name: Vec<String>,
    description: Vec<String>,
    text: Vec<String>,
) -> Vec<SearchText> {
    let mut all_text_searches: Vec<SearchText> = vec![];
    all_text_searches.extend(hxl.iter().map(|t| SearchText {
        text: t.clone(),
        context: nonempty![SearchContext::Hxl],
    }));
    all_text_searches.extend(name.iter().map(|t| SearchText {
        text: t.clone(),
        context: nonempty![SearchContext::HumanReadableName],
    }));
    all_text_searches.extend(description.iter().map(|t| SearchText {
        text: t.clone(),
        context: nonempty![SearchContext::Description],
    }));
    all_text_searches.extend(text.iter().map(|t| SearchText {
        text: t.clone(),
        context: SearchContext::all(),
    }));
    all_text_searches
}

impl From<SearchParamsArgs> for SearchParams {
    fn from(args: SearchParamsArgs) -> Self {
        SearchParams {
            text: text_searches_from_args(args.hxl, args.name, args.description, args.text),
            year_range: args.year_range.clone(),
            geometry_level: args.geometry_level.clone().map(GeometryLevel),
            source_data_release: args.source_data_release.clone().map(SourceDataRelease),
            data_publisher: args.publisher.clone().map(DataPublisher),
            country: args.country.clone().map(Country),
            source_metric_id: args.source_metric_id.clone().map(SourceMetricId),
            metric_id: args.id.clone().into_iter().map(MetricId).collect(),
        }
    }
}

impl RunCommand for MetricsCommand {
    async fn run(&self, config: Config) -> Result<()> {
        info!("Running `metrics` subcommand");
        debug!("{:#?}", self);

        let popgetter = Popgetter::new_with_config(config).await?;
        let search_results = popgetter.search(self.search_params_args.clone().into());

        let len_requests = search_results.0.shape().0;
        println!("Found {} metrics.", len_requests);

        if len_requests > 50 && !self.full {
            display_search_results(search_results, Some(50));
            println!(
                "{} more results not shown. Use --full to show all results.",
                len_requests - 50
            );
        } else {
            display_search_results(search_results, None);
        }
        Ok(())
    }
}

/// The Countries command should return information about the various countries we have data for.
/// This could include metrics like the number of surveys / metrics / geographies available for
/// each country.
#[derive(Args, Debug)]
pub struct CountriesCommand;

impl RunCommand for CountriesCommand {
    async fn run(&self, config: Config) -> Result<()> {
        let _popgetter = Popgetter::new_with_config(config).await?;
        Ok(())
    }
}

/// The Surveys command should list the various surveys that popgetter has access to and related
/// statistics.
#[derive(Args, Debug)]
pub struct SurveysCommand;

impl RunCommand for SurveysCommand {
    async fn run(&self, _config: Config) -> Result<()> {
        info!("Running `surveys` subcommand");
        Ok(())
    }
}

/// The Recipe command loads a recipe file and generates the output data requested
/// TODO: Reimplement this
// #[derive(Args, Debug)]
// pub struct RecipeCommand {
//     #[arg(index = 1)]
//     recipe_file: String,

//     #[arg(short = 'f', long)]
//     output_format: OutputFormat,

//     #[arg(short = 'o', long)]
//     output_file: String,
// }

// impl RunCommand for RecipeCommand {
//     async fn run(&self, config: Config) -> Result<()> {
//         let popgetter = Popgetter::new_with_config(config).await?;
//         let recipe = fs::read_to_string(&self.recipe_file)?;
//         let data_request: DataRequestSpec = serde_json::from_str(&recipe)?;
//         let mut results = popgetter.get_data_request(&data_request).await?;
//         println!("{results}");
//         let formatter: OutputFormatter = (&self.output_format).into();
//         let mut f = File::create(&self.output_file)?;
//         formatter.save(&mut f, &mut results)?;
//         Ok(())
//     }
// }

/// The entrypoint for the CLI.
#[derive(Parser, Debug)]
#[command(version, about, long_about = None, name="popgetter", long_about="Popgetter is a tool to quickly get the data you need!")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

/// Commands contains the list of subcommands avaliable for use in the CLI.
/// Each command should implmement the RunCommand trait and specify the list
/// of required args for that command.
#[derive(Subcommand, Debug)]
#[enum_dispatch(RunCommand)]
pub enum Commands {
    /// List countries for which data are available
    Countries(CountriesCommand),
    /// Output data for a given region and set of metrics
    Data(DataCommand),
    /// List and filter available metrics. Multiple filters are applied conjunctively, i.e. this
    /// command only returns metrics that match all filters.
    Metrics(MetricsCommand),
    /// Surveys
    Surveys(SurveysCommand),
    // /// From recipe
    // Recipe(RecipeCommand),
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

    #[test]
    fn test_parse_year_range() {
        assert_eq!(
            parse_year_range("2000"),
            Ok(vec![YearRange::Between(2000, 2000)])
        );
        assert_eq!(
            parse_year_range("2000..."),
            Ok(vec![YearRange::After(2000)])
        );
        assert_eq!(
            parse_year_range("...2000"),
            Ok(vec![YearRange::Before(2000)])
        );
        assert_eq!(
            parse_year_range("2000...2001"),
            Ok(vec![YearRange::Between(2000, 2001)])
        );
        assert_eq!(
            parse_year_range("2000...2001,2005..."),
            Ok(vec![YearRange::Between(2000, 2001), YearRange::After(2005)])
        );
        assert_eq!(
            parse_year_range("...2001,2005,2009"),
            Ok(vec![
                YearRange::Before(2001),
                YearRange::Between(2005, 2005),
                YearRange::Between(2009, 2009)
            ])
        );
    }

    #[test]
    fn output_type_should_deserialize_properly() {
        let output_format = OutputFormat::from_str("GeoJSON");
        assert_eq!(
            output_format.unwrap(),
            OutputFormat::GeoJSON,
            "geojson format should be parsed correctly"
        );
        let output_format = OutputFormat::from_str("GeoJson");
        assert_eq!(
            output_format.unwrap(),
            OutputFormat::GeoJSON,
            "parsing should be case insensitive"
        );
        let output_format = OutputFormat::from_str("geoparquet");
        assert_eq!(
            output_format.unwrap(),
            OutputFormat::GeoParquet,
            "correct variants should parse correctly"
        );
        let output_format = OutputFormat::from_str("awesome_tiny_model");
        assert!(output_format.is_err(), "non listed formats should fail");
    }
}
