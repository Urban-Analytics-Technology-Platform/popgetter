use anyhow::{anyhow, Result};
use enum_dispatch::enum_dispatch;
use geo::geometry::Geometry;
use geojson;
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
use std::io::Cursor;
use std::io::Write;
use wkt::TryFromWkt;

/// Utility function to convert from polars `AnyValue` to `serde_json::Value`
/// Doesn't cover all types but most of them.
fn any_value_to_json(value: &AnyValue) -> Result<Value> {
    match value {
        AnyValue::Null => Ok(Value::Null),
        AnyValue::Boolean(b) => Ok(Value::Bool(*b)),
        AnyValue::String(s) => Ok(Value::String((*s).to_string())),
        AnyValue::Int8(n) => Ok(json!(*n)),
        AnyValue::Int16(n) => Ok(json!(*n)),
        AnyValue::Int32(n) => Ok(json!(*n)),
        AnyValue::Int64(n) => Ok(json!(*n)),
        AnyValue::UInt8(n) => Ok(json!(*n)),
        AnyValue::UInt16(n) => Ok(json!(*n)),
        AnyValue::UInt32(n) => Ok(json!(*n)),
        AnyValue::UInt64(n) => Ok(json!(*n)),
        AnyValue::Float32(n) => Ok(json!(*n)),
        AnyValue::Float64(n) => Ok(json!(*n)),
        AnyValue::Date(d) => Ok(json!(d.to_string())), // You might want to format this
        AnyValue::Datetime(dt, _, _) => Ok(json!(dt.to_string())), // You might want to format this
        AnyValue::Time(t) => Ok(json!(t.to_string())), // You might want to format this
        AnyValue::List(series) => {
            let json_values: Result<Vec<Value>> =
                series.iter().map(|val| any_value_to_json(&val)).collect();
            Ok(Value::Array(json_values?))
        }
        _ => Err(anyhow!("Failed to convert type")),
    }
}

/// Trait to define different output generators. Defines two
/// functions, format which generates a serialized string of the
/// `DataFrame` and save which generates a file with the generated
/// file
#[enum_dispatch]
pub trait OutputGenerator {
    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()>;
    fn format(&self, df: &mut DataFrame) -> Result<String> {
        // Just creating an empty vec to store the buffered output
        let mut data: Vec<u8> = vec![0; 200];
        let mut buff = Cursor::new(&mut data);
        self.save(&mut buff, df)?;

        Ok(String::from_utf8(data)?)
    }
}

/// Enum of OutputFormatters one for each potential
/// output type
#[enum_dispatch(OutputGenerator)]
#[derive(Serialize, Deserialize, Debug)]
pub enum OutputFormatter {
    GeoJSON(GeoJSONFormatter),
    GeoJSONSeq(GeoJSONSeqFormatter),
    Csv(CSVFormatter),
}

/// Format the results as geojson sequence format
/// This is one line per feature serialized as a
/// geojson feature
#[derive(Serialize, Deserialize, Debug)]
pub struct GeoJSONSeqFormatter;

impl OutputGenerator for GeoJSONSeqFormatter {
    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        let geometry_col = df.column("geometry")?;
        let other_cols = df.drop("geometry")?;
        for (idx, geom) in geometry_col.str()?.into_iter().enumerate() {
            if let Some(wkt_str) = geom {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt_str).unwrap();
                let mut properties = serde_json::Map::new();
                for col in other_cols.get_columns() {
                    let val = any_value_to_json(&col.get(idx)?)?;
                    properties.insert(col.name().to_string(), val);
                }
                let feature = geojson::Feature {
                    bbox: None,
                    geometry: Some(geojson::Geometry::from(&geom)),
                    id: None,
                    properties: Some(properties),
                    foreign_members: None,
                };
                writeln!(writer, "{feature}")?;
            }
        }
        Ok(())
    }
}

/// Define what format geometries are represented in
///
/// Wkb: Well known binary
/// Wkt: Well knoen text
#[derive(Serialize, Deserialize, Debug)]
pub enum GeoFormat {
    Wkb,
    Wkt,
}

/// Format the results as a CSV file with the
/// geometry encoded in the specified format
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CSVFormatter {
    geo_format: Option<GeoFormat>,
}

impl OutputGenerator for CSVFormatter {
    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        CsvWriter::new(writer).finish(df)?;
        Ok(())
    }
}

/// Format the results as a geojson file
/// TODO there is probably a better way to do this using
/// geozero to process the dataframe to a file without
/// having to construct the entire thing in memory first
#[derive(Serialize, Deserialize, Debug, Default)]
pub struct GeoJSONFormatter;

impl OutputGenerator for GeoJSONFormatter {
    fn format(&self, df: &mut DataFrame) -> Result<String> {
        let geometry_col = df.column("geometry")?;
        let other_cols = df.drop("geometry")?;
        let mut features: Vec<geojson::Feature> = vec![];

        for (idx, geom) in geometry_col.str()?.into_iter().enumerate() {
            if let Some(wkt_str) = geom {
                let geom: Geometry<f64> = Geometry::try_from_wkt_str(wkt_str).unwrap();
                let mut properties = serde_json::Map::new();

                for col in other_cols.get_columns() {
                    let val = any_value_to_json(&col.get(idx)?)?;
                    properties.insert(col.name().to_string(), val);
                }

                let feature = geojson::Feature {
                    geometry: Some(geojson::Geometry::from(&geom)),
                    properties: Some(properties),
                    bbox: None,
                    id: None,
                    foreign_members: None,
                };
                features.push(feature);
            }
        }

        let feature_collection = geojson::FeatureCollection {
            bbox: None,
            features,
            foreign_members: None,
        };
        Ok(feature_collection.to_string())
    }

    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        let result = self.format(df)?;
        writer.write_all(result.as_bytes())?;

        Ok(())
    }
}
