use std::str::FromStr;

use clap::{Args, Parser, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json;
use strum_macros::EnumString;

#[derive(Clone, Debug)]
pub struct BBox(pub [f64; 4]);

impl FromStr for BBox {
    type Err = &'static str;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let parts: Vec<f64> = value
            .split(',')
            .map(|s| s.trim().parse::<f64>().map_err(|_| "Failed to parse bbox"))
            .collect::<Result<Vec<_>, _>>()?;

        if parts.len() != 4 {
            return Err("Bounding boxes need to have 4 coords");
        }
        let mut bbox = [0.0; 4];
        bbox.copy_from_slice(&parts);
        Ok(BBox(bbox))
    }
}

#[derive(Clone,Debug,Deserialize,Serialize,EnumString,PartialEq, Eq)]
#[strum(ascii_case_insensitive)]
pub enum OutputFormat{
    GeoJSON,
    CSV,
    GeoParquet,
    FlatGeobuf,
}

#[derive(Args, Debug)]
pub struct DataArgs {
    /// Only get data in  bounding box ([min_lat,min_lng,max_lat,max_lng])
    #[arg(short, long)]
    bbox: Option<BBox>,
    /// Only get the specific metrics
    #[arg(short, long)]
    metrics: Option<String>,
}

#[derive(Args, Debug)]
pub struct MetricArgs {
    /// Only get data in  bounding box ([min_lat,min_lng,max_lat,max_lng])
    #[arg(short, long)]
    bbox: Option<BBox>,
    /// Only get the specific metrics
    #[arg(short, long)]
    filter: Option<String>,
    /// Specify output format
    #[arg(short, long)]
    output_format: OutputFormat,
}

#[derive(Parser, Debug)]
#[command(version, 
          about, 
          long_about = None, 
          name="popgetter", 
          long_about="Popgetter is a tool to quickly get the data you need!"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Option<Commands>,
}

#[derive(Subcommand, Debug)]
pub enum Commands {
    Countries,
    /// Produce a data file with the required metrics and geometry
    Data(DataArgs),
    /// Search / List avaliable metrics
    Metrics(MetricArgs),
    /// Surveys
    Surveys,
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bbox_should_parse_if_correct() {
        let bbox = BBox::from_str("0.0,1.0,2.0,3.0");
        assert!(bbox.is_ok(), "A four coord bbox should parse");
    }

    #[test]
    fn bbox_should_not_parse_if_incorrect() {
        let bbox = BBox::from_str("0.0,1.0,2.0");
        assert!(bbox.is_err(), "A string with fewer than 4 coords should parse");
        let bbox = BBox::from_str("0.0,1.0,2.0,3.0,4.0");
        assert!(bbox.is_err(), "A string with fewer than 5 coords should parse");
        let bbox = BBox::from_str("0.0sdfsd,1.0,2.0");
        assert!(bbox.is_err(), "A string with letters shouldn't parse");
    }

    #[test]
    fn output_type_should_deserialize_properly(){
        let output_format = OutputFormat::from_str("GeoJSON");
        assert_eq!(output_format.unwrap(), OutputFormat::GeoJSON, "geojson format should be parsed correctly");
        let output_format = OutputFormat::from_str("GeoJson");
        assert_eq!(output_format.unwrap(), OutputFormat::GeoJSON, "parsing should be case insensitive");
        let output_format = OutputFormat::from_str("geoparquet");
        assert_eq!(output_format.unwrap(), OutputFormat::GeoParquet, "correct variants should parse correctly");
        let output_format = OutputFormat::from_str("awesome_tiny_model");
        assert!(output_format.is_err(), "non listed formats should fail");
    }
}
