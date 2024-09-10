mod cli;
mod display;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, RunCommand};
use log::debug;
use popgetter::config::Config;

const DEFAULT_LOGGING_LEVEL: &str = "warn";

#[tokio::main]
async fn main() -> Result<()> {
    // Set RUST_LOG to `DEFAULT_LOGGING_LEVEL` if not set
    let _ =
        std::env::var("RUST_LOG").map_err(|_| std::env::set_var("RUST_LOG", DEFAULT_LOGGING_LEVEL));
    pretty_env_logger::init_timed();
    let args = Cli::parse();
    debug!("args: {args:?}");
    let config: Config = read_config_from_toml();
    debug!("config: {config:?}");

    if let Some(command) = args.command {
        command.run(config).await?;
    }
    Ok(())
}

fn read_config_from_toml() -> Config {
    // macOS: ~/Library/Application Support/popgetter/config.toml
    let file_path = dirs::config_dir()
        .unwrap()
        .join("popgetter")
        .join("config.toml");
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
