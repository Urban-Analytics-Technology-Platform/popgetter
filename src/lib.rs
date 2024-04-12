use serde::{Deserialize, Serialize};
use typify::import_types;
import_types!("schema.json");

pub fn hi() -> String {
    "Hello!".into()
}

pub fn random_survey() -> CountryMetadata {
    CountryMetadata {
        iso2: "US".into(),
        iso3: "USA".into(),
        name_official: "United States of America".into(),
        name_short_en: "USA".into(),
    }
}
