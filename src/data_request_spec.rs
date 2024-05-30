use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::{
    ops::{Index, IndexMut},
    str::FromStr,
};
use log::debug;

use crate::{metadata::Metadata, parquet::MetricRequest};

#[derive(Serialize, Deserialize, Debug)]
pub struct DataRequestSpec {
    pub geometry: GeometrySpec,
    pub region: Vec<RegionSpec>,
    pub metrics: Vec<MetricSpec>,
}

impl DataRequestSpec {
    /// Generates a vector of metric requests from a `DataRequestSpec`
    /// and a catalouge.
    pub fn metric_requests(&self, catalogue: &Metadata) -> Result<Vec<MetricRequest>> {
        let mut metric_requests: Vec<MetricRequest> = vec![];
        debug!("Try to get metrics {:#?}", self.metrics);
        for metric_spec in &self.metrics {
            match metric_spec {
                MetricSpec::NamedMetric(name) => {
                    metric_requests.push(
                        catalogue
                            // TODO figure out the best way to pass these
                            .get_metric_details(name, "municipality", "2022")
                            .with_context(|| "Failed to find metric")?,
                    );
                }
                MetricSpec::DataProduct(_) => todo!("unsupported metric spec"),
            }
        }
        Ok(metric_requests)
    }

    /// Get the details of a geometry file from a `DataRequestSpec` and
    /// a catalouge.
    pub fn geom_details(&self, catalogue: &Metadata) -> Result<String> {
        catalogue.get_geom_details("municipalty")
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MetricSpec {
    NamedMetric(String),
    DataProduct(String),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeometrySpec {
    geometry_level: String,
    include_geoms: bool,
}

impl Default for GeometrySpec {
    fn default() -> Self {
        Self {
            include_geoms: true,
            geometry_level: "admin2".into(),
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
            "A string with fewer than 4 coords should not parse"
        );
        let bbox = BBox::from_str("0.0,1.0,2.0,3.0,4.0");
        assert!(
            bbox.is_err(),
            "A string with 5 or more coords should not parse"
        );
        let bbox = BBox::from_str("0.0sdfsd,1.0,2.0");
        assert!(bbox.is_err(), "A string with letters shouldn't parse");
    }
}
