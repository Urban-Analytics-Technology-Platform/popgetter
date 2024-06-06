use anyhow::{Context, Result};

use polars::{
    frame::DataFrame,
    lazy::{
        dsl::{col, lit},
        frame::IntoLazy,
    },
    prelude::NamedFrom,
    series::Series,
};
use serde::{Deserialize, Serialize};
use std::{
    ops::{Index, IndexMut},
    str::FromStr,
};

use crate::{
    metadata::{Metadata, MetricId},
    parquet::MetricRequest,
};

#[derive(Serialize, Deserialize, Debug)]
pub struct DataRequestSpec {
    pub geometry: GeometrySpec,
    pub region: Vec<RegionSpec>,
    pub metrics: Vec<MetricSpec>,
    pub years: Option<Vec<String>>,
}

/// This is the response to requesting a set of metrics.
/// It contains the matched Metric Requests along with
/// the geometry that was selected we can also potentially
/// use this to give feedback on other options in the future
pub struct MetricRequestResult {
    pub metrics: Vec<MetricRequest>,
    pub selected_geometry: String,
}

impl DataRequestSpec {
    /// Generates a vector of metric requests from a `DataRequestSpec` and a catalog.
    pub fn metric_requests(&self, catalogue: &Metadata) -> Result<MetricRequestResult> {
        let mut possible_metrics: Option<DataFrame> = None;

        // Find all the metrics which match the requested ones, expanding
        // any wildcards as we do so
        for metric_spec in &self.metrics {
            match metric_spec {
                MetricSpec::Metric(metric) => {
                    let metrics = catalogue.expand_wildcard_metric(metric)?;
                    let metric_options = catalogue
                        .get_possible_metric_details(&metrics)
                        .with_context(|| "Failed to find metric")?;
                    possible_metrics = if let Some(prev) = possible_metrics {
                        Some(prev.vstack(&metric_options)?)
                    } else {
                        Some(metric_options)
                    };
                }
                MetricSpec::DataProduct(_) => todo!("unsupported metric spec"),
            }
        }

        // Extract the possible matches for the given metrics
        let possible_metrics = possible_metrics.context("Failed to find matching metrics")?;

        // If a geometry level is specified filter out only those metrics with the level
        let filtered_possible_metrics = if let Some(level) = &self.geometry.geometry_level {
            possible_metrics
                .lazy()
                .filter(col("geometry_level").eq(lit(level.clone())))
        } else {
            possible_metrics.lazy()
        };

        // If a year is specifed, filter out only years that have that level.
        let filtered_possible_metrics = if let Some(years) = &self.years {
            let year_series = Series::new("year_filter", years.clone());
            filtered_possible_metrics.filter(col("year").is_in(lit(year_series)))
        } else {
            filtered_possible_metrics
        };

        let filtered_possible_metrics = filtered_possible_metrics.collect()?;

        // Iterate through the results and generate the metric requests
        let metric_requests: Vec<MetricRequest> = filtered_possible_metrics
            .column("parquet_metric_id")?
            .str()?
            .into_iter()
            .zip(
                filtered_possible_metrics
                    .column("parquet_metric_file")?
                    .str()?,
            )
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

        let selected_geometry = filtered_possible_metrics
            .column("geometry_level")?
            .str()?
            .get(0)
            .expect("Should have geometry")
            .to_owned();

        Ok(MetricRequestResult {
            metrics: metric_requests,
            selected_geometry,
        })
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MetricSpec {
    Metric(MetricId),
    DataProduct(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeometrySpec {
    geometry_level: Option<String>,
    include_geoms: bool,
}

impl Default for GeometrySpec {
    fn default() -> Self {
        Self {
            include_geoms: true,
            geometry_level: None,
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum RegionSpec {
    BoundingBox(BBox),
    Polygon(Polygon),
    NamedArea(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Polygon;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BBox(pub [f64; 4]);

impl Index<usize> for BBox {
    type Output = f64;

    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}

impl IndexMut<usize> for BBox {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

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
        assert!(
            bbox.is_err(),
            "A string with fewer than 4 coords should parse"
        );
        let bbox = BBox::from_str("0.0,1.0,2.0,3.0,4.0");
        assert!(
            bbox.is_err(),
            "A string with fewer than 5 coords should parse"
        );
        let bbox = BBox::from_str("0.0sdfsd,1.0,2.0");
        assert!(bbox.is_err(), "A string with letters shouldn't parse");
    }
}
