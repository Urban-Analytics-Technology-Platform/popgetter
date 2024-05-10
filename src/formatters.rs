use anyhow::Result;
use enum_dispatch::enum_dispatch;
use geojson;
use geojson::{Feature, GeoJson, Geometry, Value};
use polars::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Cursor;
use std::{convert::TryFrom, io::Write};

#[enum_dispatch]
pub trait OutputGenerator {
    fn format(&self, df: &mut DataFrame) -> Result<String>;
    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()>;
}

#[enum_dispatch(OutputGenerator)]
#[derive(Serialize, Deserialize, Debug)]
pub enum OutputFormatter {
    GeoJSON(GeoJSONFormatter),
    GeoJSONSeq(GeoJSONSeqFormatter),
    Csv(CSVFormatter),
}

#[derive(Serialize, Deserialize, Debug)]
pub struct GeoJSONSeqFormatter;

impl OutputGenerator for GeoJSONSeqFormatter {
    fn format(&self, df: &mut DataFrame) -> Result<String> {
        Ok("Test".into())
    }

    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        let output = self.format(df)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct GeoJSONFormatter;

#[derive(Serialize, Deserialize, Debug)]
pub enum GeoFormat {
    Wkb,
    Wkt,
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct CSVFormatter {
    geo_format: Option<GeoFormat>,
}

impl OutputGenerator for CSVFormatter {
    fn format(&self, df: &mut DataFrame) -> Result<String> {
        let mut data: Vec<u8> = vec![0; 200];
        let mut buff = Cursor::new(&mut data);
        self.save(&mut buff, df)?;

        Ok(String::from_utf8(data)?)
    }

    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        CsvWriter::new(writer).finish(df)?;
        Ok(())
    }
}

impl OutputGenerator for GeoJSONFormatter {
    fn format(&self, df: &mut DataFrame) -> Result<String> {
        Ok("Test".into())
    }

    fn save(&self, writer: &mut impl Write, df: &mut DataFrame) -> Result<()> {
        let output = self.format(df)?;
        Ok(())
    }
}
