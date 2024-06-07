use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Serialize, Deserialize, Debug)]
#[serde(default)]
pub struct Config {
    pub base_path: String,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            base_path: "https://popgetter.blob.core.windows.net/popgetter-dagster-test/test_2"
                .into(),
        }
    }
}

impl Config {
    fn xdg_config_path() -> PathBuf {
        let xdg_dirs = xdg::BaseDirectories::with_prefix("popgetter").unwrap();
        xdg_dirs.place_config_file("config.toml").unwrap()
    }

    pub fn from_toml() -> Self {
        let file_path = Self::xdg_config_path();

        match std::fs::read_to_string(file_path) {
            Ok(contents) => toml::from_str(&contents).expect("Invalid TOML in config file"),
            Err(e) => {
                if e.kind() == std::io::ErrorKind::NotFound {
                    Config::default()
                } else {
                    panic!("Error reading config file: {:#?}", e);
                }
            }
        }
    }
}
