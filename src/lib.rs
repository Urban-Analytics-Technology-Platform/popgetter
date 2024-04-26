use anyhow::Result;
use metadata::{load_metadata, SourceDataRelease};
pub mod data_request_spec;
pub mod geo;
pub mod metadata;
pub mod parquet;

pub struct Popgetter {
    pub metadata: SourceDataRelease,
}

impl Popgetter {
    pub fn new() -> Result<Self> {
        let metadata = load_metadata("us_metadata.json")?;
        Ok(Self { metadata })
    }
}
