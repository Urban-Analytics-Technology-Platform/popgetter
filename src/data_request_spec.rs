use std::str::FromStr;

use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug)]
pub struct DataRequestSpec {
    region: Vec<RegionSpec>,
    metrics: Vec<MetricSpec>,
}

#[derive(Serialize, Deserialize, Debug)]
pub enum MetricSpec {
    NamedMetric(String),
    DataProduct(String),
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
