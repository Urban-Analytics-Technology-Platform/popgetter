use std::{fs::File, io::BufReader};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use typify::import_types;

import_types!("schema/popgetter_0.1.0.json");

pub fn load_metadata(path: &str) -> Result<SourceDataRelease> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let release: SourceDataRelease = serde_json::from_reader(reader)?;
    Ok(release)
}

pub async fn load_metadata_from_url(
    client: &reqwest::Client,
    url: &str,
) -> Result<SourceDataRelease> {
    let release = client
        .get(url)
        .send()
        .await?
        .json::<SourceDataRelease>()
        .await?;
    Ok(release.clone())
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_loading_metadata() {
        let data = load_metadata("us_metadata.json");
        let data = data.expect("Metadata should load and parse fine");
        assert_eq!(data.name, "ACS_2019_fiveYear");
    }

    #[tokio::test]
    async fn test_loading_metadata_from_url() {
        let data = load_metadata_from_url(
            &reqwest::Client::new(),
            "https://popgetter.blob.core.windows.net/popgetter-cli-test/us_metadata.json",
        )
        .await;
        let data = data.expect("Metadata should load and parse fine");
        assert_eq!(data.name, "ACS_2019_fiveYear");
    }
}
