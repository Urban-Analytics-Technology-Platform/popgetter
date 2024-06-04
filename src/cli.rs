// FromStr is required by EnumString. The compiler seems to not be able to
// see that and so is giving a warning. Dont remove it
use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use enum_dispatch::enum_dispatch;
use log::{debug, info};
use popgetter::{
    data_request_spec::{BBox, DataRequestSpec, GeometrySpec, MetricSpec, RegionSpec}, formatters::{CSVFormatter, GeoJSONFormatter, GeoJSONSeqFormatter, OutputFormatter, OutputGenerator}, metadata::MetricId, Popgetter
};
use serde::{Deserialize, Serialize};
use std::fs::File;
use strum_macros::EnumString;

/// Defines the output formats we are able to produce data in.
#[derive(Clone, Debug, Deserialize, Serialize, EnumString, PartialEq, Eq)]
#[strum(ascii_case_insensitive)]
pub enum OutputFormat {
    GeoJSON,
    GeoJSONSeq,
    Csv,
    GeoParquet,
    FlatGeobuf,
}

/// Trait that defines what to run when a given subcommand is invoked.
#[enum_dispatch]
pub trait RunCommand {
    async fn run(&self) -> Result<()>;
}

/// The Data command is the one we invoke to get a set of metrics and geometry
/// for some given region and set of metrics. Currently it takes two arguments
/// - bbox: A Bounding box
/// - metrics: A comma seperated list of metrics to retrive.
///
/// The Data command can be converted into a `DataRequestSpec` which is the processed
/// by the core library.
#[derive(Args, Debug)]
pub struct DataCommand {
    /// Only get data in  bounding box ([min_lat,min_lng,max_lat,max_lng])
    #[arg(short, long, allow_hyphen_values(true), help="Bounding box in which to get the results. Format is: min_lon, min_lat, max_lon, max_lat ")]
    bbox: Option<BBox>,
    /// Specify a metric by hxl
    #[arg(short='h', long, help="Specify a metric by Humanitarian Exchange Language tag")]
    hxl: Option<Vec<String>>,

    // Specify a metric by id
    #[arg(short='i', long, help="Specify a metric by uuid, can be a partial uuid")]
    id: Option<Vec<String>>,

    // Specify a metric by name 
    #[arg(short='n', long, help="Specify a metric by Human readable name")]
    name: Option<Vec<String>>,

    /// Specify output format
    #[arg(short='f', long, help="One of GeoJSON, CSV, GeoJSONSeq")]
    output_format: OutputFormat,

    /// Specify where the result should be saved
    #[arg(short='o',long, help="Output file to place the results")]
    output_file: String,

    /// Specify the years we should get the result for
    #[arg(short='y', long, help="Specify the year ranges for which you are interested in the metrics")]
    years: Option<Vec<String>>
}


impl DataCommand{
    pub fn gather_metric_requests(&self)->Vec<MetricId>{
        let mut metric_ids: Vec<MetricId> = vec![];

        if let Some(ids) = &self.id{
            for id in ids{
                metric_ids.push(MetricId::Id(id.clone()));
            }
        } 

        if let Some(hxls) = &self.hxl{
            for hxl in hxls{
                metric_ids.push(MetricId::Hxl(hxl.clone()));
            }
        } 

        if let Some(names) = &self.name{
            for name in names{
                metric_ids.push(MetricId::CommonName(name.clone()));
            }
        } 

        metric_ids

    
    }
}

impl RunCommand for DataCommand {
    async fn run(&self) -> Result<()> {
        info!("Running `data` subcommand");

        let popgetter = Popgetter::new().await?;
        let data_request = DataRequestSpec::from(self);
        let mut results = popgetter.get_data_request(&data_request).await?;

        let formatter = match &self.output_format {
            OutputFormat::GeoJSON => OutputFormatter::GeoJSON(GeoJSONFormatter),
            OutputFormat::Csv => OutputFormatter::Csv(CSVFormatter::default()),
            OutputFormat::GeoJSONSeq => OutputFormatter::GeoJSONSeq(GeoJSONSeqFormatter),
            _ => todo!("output format not implemented"),
        };

        debug!("{results:#?}");
        let mut f = File::create(&self.output_file)?;
        formatter.save(&mut f, &mut results)?;

        Ok(())
    }
}

impl From<&DataCommand> for DataRequestSpec {
    fn from(value: &DataCommand) -> Self {
        let region = if let Some(bbox) = value.bbox.clone() {
            vec![RegionSpec::BoundingBox(bbox)]
        } else {
            vec![]
        };

        let metrics = value.gather_metric_requests()
                           .into_iter()
                           .map(|metric_id| MetricSpec::Metric(metric_id))
                           .collect();

        DataRequestSpec {
            geometry: GeometrySpec::default(), 
            region, 
            metrics,
            years: None, 
        }
    }
}

/// The Metrics command allows a user to search for a set of metrics by bounding box and filter.
/// The set of ways to search will likley increase over time
#[derive(Args, Debug)]
pub struct MetricsCommand {
    /// Only get data in  bounding box ([min_lat,min_lng,max_lat,max_lng])
    #[arg(short, long)]
    bbox: Option<BBox>,
    /// Only get the specific metrics
    #[arg(short, long)]
    filter: Option<String>,
}

impl RunCommand for MetricsCommand {
    async fn run(&self) -> Result<()> {
        info!("Running `metrics` subcommand");
        Ok(())
    }
}

/// The Countries command should return information about the various countries we have data for.
/// This could include metrics like the number of surveys / metrics / geographies avaliable for each country.
#[derive(Args, Debug)]
pub struct CountriesCommand;

impl RunCommand for CountriesCommand {
    async fn run(&self) -> Result<()> {
        let _popgetter = Popgetter::new().await?;
        Ok(())
    }
}

/// The Surveys command should list the various surveys that popgetter has access to and releated
/// stastistics.
#[derive(Args, Debug)]
pub struct SurveysCommand;

impl RunCommand for SurveysCommand {
    async fn run(&self) -> Result<()> {
        info!("Running `surveys` subcommand");
        Ok(())
    }
}

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
    Countries(CountriesCommand),
    /// Produce a data file with the required metrics and geometry
    Data(DataCommand),
    /// Search / List avaliable metrics
    Metrics(MetricsCommand),
    /// Surveys
    Surveys(SurveysCommand),
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;

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
