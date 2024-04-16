use clap::{Args, Parser, Subcommand};

#[derive(Clone, Debug)]
pub struct BBox(pub [f64; 4]);

impl TryFrom<String> for BBox {
    type Error = &'static str;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        let parts: Vec<f64> = value
            .split_whitespace()
            .map(|s| s.parse::<f64>().map_err(|_| "Failed to parse bbox"))
            .collect::<Result<Vec<_>, _>>()?;

        if parts.len() != 4 {
            return Err("Bounding boxes need to have coords");
        }
        let mut bbox = [0.0; 4];
        bbox.copy_from_slice(&parts);
        Ok(BBox(bbox))
    }
}

#[derive(Args, Debug)]
pub struct DataArgs {
    /// Only get data in  bounding box
    #[arg(short, long)]
    bbox: Option<String>,
    /// Only get the specific metrics
    #[arg(short, long)]
    metrics: Option<String>,
}

#[derive(Args, Debug)]
pub struct MetricArgs {
    /// Only get data in  bounding box
    #[arg(short, long)]
    bbox: Option<String>,
    /// Only get the specific metrics
    #[arg(short, long)]
    filter: Option<String>,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None, name="popgetter", long_about="Popgetter is a tool to quickly get the data you need!")]
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
