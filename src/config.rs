use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(default)]
pub struct Config {
    pub base_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            base_path: "https://popgetter.blob.core.windows.net/releases/v0.2.0".into(),
        }
    }
}
