use std::{fs::File, io::BufReader};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use typify::import_types;

import_types!("schema.json");

pub fn hi() -> String {
    "Hello!".into()
}

pub fn load_metadata(path: &str) -> Result<SourceDataRelease> {
    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let release: SourceDataRelease = serde_json::from_reader(reader)?;
    Ok(release)
}

#[cfg(test)]
mod tests {
    // Note this useful idiom: importing names from outer (for mod tests) scope.
    use super::*;

    #[test]
    fn test_loading_metadata() {
        let data = load_metadata("us_metadata.json");
        assert!(data.is_ok(), "Metadata should load and pase fine");
        let data = data.unwrap();
        assert_eq!(data.name, "ACS_2019_fiveYear");
    }
}
