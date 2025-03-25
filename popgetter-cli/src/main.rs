mod cli;
mod display;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, RunCommand};
use log::debug;
use popgetter_core::config::Config;

const DEFAULT_LOGGING_LEVEL: &str = "warn";

#[tokio::main]
async fn main() -> Result<()> {
    // Set RUST_LOG to `DEFAULT_LOGGING_LEVEL` if not set
    let _ =
        std::env::var("RUST_LOG").map_err(|_| std::env::set_var("RUST_LOG", DEFAULT_LOGGING_LEVEL));
    pretty_env_logger::init_timed();
    let args = Cli::parse();
    debug!("args: {args:?}");
    let config: Config = read_config_from_toml(args.dev, args.base_path.as_deref());
    debug!("config: {config:?}");

    if let Some(command) = args.command {
        // Return ok if pipe is closed instead of error, otherwise return error
        // See: https://stackoverflow.com/a/65760807, https://github.com/rust-lang/rust/issues/62569
        if let Err(err) = command.run(config).await {
            if let Some(err) = err.downcast_ref::<std::io::Error>() {
                if err.kind() == std::io::ErrorKind::BrokenPipe {
                    return Ok(());
                }
            }
            Err(err)?;
        }
    }
    Ok(())
}

fn read_config_from_toml(dev: bool, base_path: Option<&str>) -> Config {
    // macOS: ~/Library/Application Support/popgetter/config.toml
    let file_path = dirs::config_dir()
        .unwrap()
        .join("popgetter")
        .join("config.toml");
    match std::fs::read_to_string(file_path) {
        Ok(contents) => toml::from_str(&contents).expect("Invalid TOML in config file"),
        Err(e) => {
            if e.kind() == std::io::ErrorKind::NotFound {
                match (dev, base_path) {
                    (_, Some(base_path_str)) => Config {
                        base_path: base_path_str.to_string(),
                    },
                    (false, None) => Config::default(),
                    (true, None) => Config::dev(),
                }
            } else {
                panic!("Error reading config file: {:#?}", e);
            }
        }
    }
}
