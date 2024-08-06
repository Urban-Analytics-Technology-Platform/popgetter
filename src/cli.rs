// FromStr is required by EnumString. The compiler seems to not be able to
// see that and so is giving a warning. Dont remove it
use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use enum_dispatch::enum_dispatch;
use log::{debug, error, info};
use nonempty::nonempty;
use polars::frame::DataFrame;
use popgetter::{
    config::Config,
    data_request_spec::{DataRequestSpec, RegionSpec},
    formatters::{
        CSVFormatter, GeoJSONFormatter, GeoJSONSeqFormatter, OutputFormatter, OutputGenerator,
    },
    geo::BBox,
    search::{
        Country, DataPublisher, DownloadParams, GeometryLevel, MetricId, Params, SearchContext,
        SearchParams, SearchResults, SearchText, SourceDataRelease, SourceMetricId, YearRange,
    },
    Popgetter,
};
use serde::{Deserialize, Serialize};
use spinners::{Spinner, Spinners};
use std::{fs::File, path::Path};
use std::{io, process};
use strum_macros::EnumString;

use crate::display::{display_countries, display_search_results};

const DEFAULT_PROGRESS_SPINNER: Spinners = Spinners::Dots;
const COMPLETE_PROGRESS_STRING: &str = "✔";
const RUNNING_TAIL_STRING: &str = "...";
const DOWNLOADING_SEARCHING_STRING: &str = "Downloading and searching metadata";

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

fn write_output<T, U>(
    output_generator: T,
    mut data: DataFrame,
    output_file: Option<U>,
) -> anyhow::Result<()>
where
    T: OutputGenerator,
    U: AsRef<Path>,
{
    if let Some(output_file) = output_file {
        let mut f = File::create(output_file)?;
        output_generator.save(&mut f, &mut data)?;
    } else {
        let mut stdout_lock = std::io::stdout().lock();
        output_generator.save(&mut stdout_lock, &mut data)?;
    };
    Ok(())
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
    #[command(flatten)]
    download_params_args: DownloadParamsArgs,
    #[arg(
        short = 'r',
        long,
        default_value_t = false,
        help = "Force run without prompt"
    )]
    force_run: bool,
    #[arg(from_global)]
    quiet: bool,
}

#[derive(Args, Debug, Clone)]
struct DownloadParamsArgs {
    #[arg(
        long = "no-geometry",
        help = "When set, no geometry data is included in the results"
    )]
    no_geometry: bool,
}

/// A type combining both the `SearchParamsArgs` and `DownloadParamsArgs` to enable `DownloadParams`
/// to be constructed since fields of `SearchParamsArgs` may also be needed for `DownloadParams`.
#[derive(Clone, Debug)]
struct CombinedParamsArgs {
    search_params_args: SearchParamsArgs,
    download_params_args: DownloadParamsArgs,
}

impl From<CombinedParamsArgs> for DownloadParams {
    fn from(combined_params_args: CombinedParamsArgs) -> Self {
        Self {
            region_spec: combined_params_args
                .search_params_args
                .bbox
                .map(|bbox| vec![RegionSpec::BoundingBox(bbox)])
                .unwrap_or_default(),
            include_geoms: !combined_params_args.download_params_args.no_geometry,
        }
    }
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

async fn read_popgetter(config: Config) -> anyhow::Result<Popgetter> {
    let xdg_dirs = xdg::BaseDirectories::with_prefix("popgetter")?;
    // TODO: enable cache to be optional
    let path = xdg_dirs.get_cache_home();
    // Try to read metadata from cache
    if path.exists() {
        match Popgetter::new_with_config_and_cache(config.clone(), &path) {
            Ok(popgetter) => return Ok(popgetter),
            Err(err) => {
                error!("Failed to read metadata from cache with error: {err}");
            }
        }
    }
    // If no metadata cache, get metadata and try to cache
    std::fs::create_dir_all(&path)?;
    let popgetter = Popgetter::new_with_config(config).await?;

    // If error creating cache, remove cache path
    if let Err(err) = popgetter.metadata.write_cache(&path) {
        std::fs::remove_dir_all(&path)
            .with_context(|| "Failed to remove cache dir following error writing cache: {err}")?;
        Err(err)?
    }
    Ok(popgetter)
}

impl RunCommand for DataCommand {
    async fn run(&self, config: Config) -> Result<()> {
        info!("Running `data` subcommand");
        let sp = (!self.quiet).then(|| {
            Spinner::with_timer(
                DEFAULT_PROGRESS_SPINNER,
                DOWNLOADING_SEARCHING_STRING.to_string() + RUNNING_TAIL_STRING,
            )
        });
        let popgetter = read_popgetter(config).await?;
        let search_params: SearchParams = self.search_params_args.clone().into();
        let search_results = popgetter.search(&search_params);

        // sp.stop_and_persist is potentially a better method, but not obvious how to
        // store the timing. Leaving below until that option is ruled out.
        // sp.stop_and_persist(&COMPLETE_PROGRESS_STRING, spinner_message.into());
        if let Some(mut s) = sp {
            s.stop_with_symbol(COMPLETE_PROGRESS_STRING)
        }

        print_metrics_count(search_results.clone());
        let download_params: DownloadParams = CombinedParamsArgs {
            search_params_args: self.search_params_args.clone(),
            download_params_args: self.download_params_args.clone(),
        }
        .into();

        if !self.force_run {
            println!("Input 'r' to run query, any other character will cancel");
            let mut input = String::new();
            io::stdin().read_line(&mut input).unwrap();
            let input = input.trim().to_lowercase();
            match input.as_str() {
                "r" | "run" | "y" | "yes" => {}
                _ => {
                    println!("Cancelling query.");
                    process::exit(0);
                }
            }
        }
        let sp = (!self.quiet).then(|| {
            Spinner::with_timer(
                DEFAULT_PROGRESS_SPINNER,
                "Downloading metrics".to_string() + RUNNING_TAIL_STRING,
            )
        });
        let data = search_results
            .download(&popgetter.config, &download_params)
            .await?;
        if let Some(mut s) = sp {
            s.stop_with_symbol(COMPLETE_PROGRESS_STRING);
        }
        debug!("{data:#?}");

        let formatter: OutputFormatter = (&self.output_format).into();
        write_output(formatter, data, self.output_file.as_deref())?;
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
        help = "Show all metrics even if there are a large number"
    )]
    full: bool,
    #[command(flatten)]
    search_params_args: SearchParamsArgs,
    #[arg(from_global)]
    quiet: bool,
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
        help = "\
            Filter by year ranges. All ranges are inclusive; multiple ranges can be\n\
            comma-separated.",
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
        help = "\
            Filter by source metric ID (i.e. the name of the table in the original data\n\
            release)."
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
    #[arg(
        short,
        long,
        value_name = "LEFT,BOTTOM,RIGHT,TOP",
        allow_hyphen_values = true,
        help = "\
            Bounding box in which to get the results. The bounding box provided must be in\n\
            the same coordinate system as used in the requested geometry file. For\n\
            example, United States has geometries with latitude and longitude (EPSG:4326),\n\
            Great Britain has geometries with the British National Grid (EPSG:27700),\n\
            Northern Ireland has geometries with the Irish Grid (EPSG:29902), and\n\
            Beligum has geometries with the Belgian Lambert 2008 reference system\n\
            (EPSG:3812)."
    )]
    bbox: Option<BBox>,
}

/// Expected behaviour:
/// N.. -> After(N); ..N -> Before(N); M..N -> Between(M, N); N -> Between(N, N)
/// Year ranges can be comma-separated
fn parse_year_range(value: &str) -> Result<Vec<YearRange>, anyhow::Error> {
    value
        .split(',')
        .map(|range| range.parse())
        .collect::<Result<Vec<YearRange>, anyhow::Error>>()
}

// A simple function to manage similaries across multiple cases.
// May ultimately be generalised to a function to manage all progress UX
// that can be switched on and off.
fn print_metrics_count(search_results: SearchResults) -> usize {
    let len_requests = search_results.0.shape().0;
    println!("Found {len_requests} metric(s).");
    len_requests
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
            region_spec: args
                .bbox
                .map(|bbox| vec![RegionSpec::BoundingBox(bbox)])
                .unwrap_or_default(),
        }
    }
}

impl RunCommand for MetricsCommand {
    async fn run(&self, config: Config) -> Result<()> {
        info!("Running `metrics` subcommand");
        debug!("{:#?}", self);

        let sp = (!self.quiet).then(|| {
            Spinner::with_timer(
                DEFAULT_PROGRESS_SPINNER,
                DOWNLOADING_SEARCHING_STRING.into(),
            )
        });
        let popgetter = read_popgetter(config).await?;
        let search_results = popgetter.search(&self.search_params_args.to_owned().into());
        if let Some(mut s) = sp {
            s.stop_with_symbol(COMPLETE_PROGRESS_STRING);
        }

        let len_requests = print_metrics_count(search_results.clone());

        if len_requests > 50 && !self.full {
            display_search_results(search_results, Some(50))?;
            println!(
                "{} more results not shown. Use --full to show all results.",
                len_requests - 50
            );
        } else {
            display_search_results(search_results, None)?;
        }
        Ok(())
    }
}

/// The Countries command should return information about the various countries we have data for.
/// This could include metrics like the number of surveys / metrics / geographies available for
/// each country.
#[derive(Args, Debug)]
pub struct CountriesCommand {
    #[arg(from_global)]
    quiet: bool,
}

impl RunCommand for CountriesCommand {
    async fn run(&self, config: Config) -> Result<()> {
        info!("Running `countries` subcommand");
        let sp = (!self.quiet).then(|| {
            let spinner_message = "Downloading countries";
            Spinner::with_timer(
                DEFAULT_PROGRESS_SPINNER,
                spinner_message.to_string() + RUNNING_TAIL_STRING,
            )
        });
        let popgetter = read_popgetter(config).await?;
        if let Some(mut s) = sp {
            s.stop_with_symbol(COMPLETE_PROGRESS_STRING);
        }
        println!("\nThe following countries are available:");
        display_countries(popgetter.metadata.countries, None)?;
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
        unimplemented!("The `Surveys` subcommand is not implemented for the current release");
    }
}

/// The Recipe command loads a recipe file and generates the output data requested
#[derive(Args, Debug)]
pub struct RecipeCommand {
    #[arg(index = 1)]
    recipe_file: String,

    #[arg(short = 'f', long)]
    output_format: OutputFormat,

    #[arg(short = 'o', long)]
    output_file: Option<String>,
}

impl RunCommand for RecipeCommand {
    async fn run(&self, config: Config) -> Result<()> {
        let popgetter = Popgetter::new_with_config(config).await?;
        let recipe = std::fs::read_to_string(&self.recipe_file)?;
        let data_request: DataRequestSpec = serde_json::from_str(&recipe)?;
        let params: Params = data_request.try_into()?;
        let search_results = popgetter.search(&params.search);
        let data = search_results
            .download(&popgetter.config, &params.download)
            .await?;
        debug!("{data:#?}");
        let formatter: OutputFormatter = (&self.output_format).into();
        write_output(formatter, data, self.output_file.as_deref())?;
        Ok(())
    }
}

/// The entrypoint for the CLI.
#[derive(Parser, Debug)]
#[command(version, about="Popgetter is a tool to quickly get the data you need!", long_about = None, name="popgetter")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
    #[arg(
        short = 'q',
        long = "quiet",
        help = "\
            Do not print progress bar to stdout. Prompt, results and logs (when `RUST_LOG`\n\
            is set) will still be printed.",
        global = true
    )]
    quiet: bool,
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
    /// From recipe
    Recipe(RecipeCommand),
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use tempfile::NamedTempFile;

    use super::*;

    #[tokio::test]
    async fn test_recipe_command() {
        let recipe_command = RecipeCommand {
            recipe_file: format!("{}/test_recipe.json", env!("CARGO_MANIFEST_DIR")),
            output_format: OutputFormat::GeoJSON,
            output_file: Some(
                NamedTempFile::new()
                    .unwrap()
                    .path()
                    .to_string_lossy()
                    .to_string(),
            ),
        };
        let result = recipe_command.run(Config::default()).await;
        assert!(result.is_ok())
    }

    #[test]
    fn test_parse_year_range() {
        assert_eq!(
            parse_year_range("2000").unwrap(),
            vec![YearRange::Between(2000, 2000)]
        );
        assert_eq!(
            parse_year_range("2000...").unwrap(),
            vec![YearRange::After(2000)]
        );
        assert_eq!(
            parse_year_range("...2000").unwrap(),
            vec![YearRange::Before(2000)]
        );
        assert_eq!(
            parse_year_range("2000...2001").unwrap(),
            vec![YearRange::Between(2000, 2001)]
        );
        assert_eq!(
            parse_year_range("2000...2001,2005...").unwrap(),
            vec![YearRange::Between(2000, 2001), YearRange::After(2005)]
        );
        assert_eq!(
            parse_year_range("...2001,2005,2009").unwrap(),
            vec![
                YearRange::Before(2001),
                YearRange::Between(2005, 2005),
                YearRange::Between(2009, 2009)
            ]
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

    #[test]
    fn cli() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }
}
