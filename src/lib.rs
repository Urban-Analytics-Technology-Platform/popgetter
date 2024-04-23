use anyhow::Result;
use metadata::{load_metadata, MetricMetadata, SourceDataRelease};

pub mod data_request_spec;
pub mod metadata;

pub struct Popgetter {
    metadata: SourceDataRelease,
}

impl Popgetter {
    pub fn new() -> Result<Self> {
        let metadata = load_metadata("us_metadata.json")?;
        Ok(Self { metadata })
    }
}
