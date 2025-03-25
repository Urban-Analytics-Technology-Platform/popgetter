mod cli;
mod display;
mod error;

use clap::Parser;
use cli::{Cli, RunCommand};
use error::{PopgetterCliError, PopgetterCliResult};
use log::debug;
use popgetter_core::config::Config;

const DEFAULT_LOGGING_LEVEL: &str = "warn";

#[tokio::main]
async fn main() -> PopgetterCliResult<()> {
    // Set RUST_LOG to `DEFAULT_LOGGING_LEVEL` if not set
    let _ =
        std::env::var("RUST_LOG").map_err(|_| std::env::set_var("RUST_LOG", DEFAULT_LOGGING_LEVEL));
    pretty_env_logger::init_timed();
    let args = Cli::parse();
    debug!("args: {args:?}");
    let config: Config = read_config_from_toml();
    debug!("config: {config:?}");

    if let Some(command) = args.command {
        // Return ok if pipe is closed instead of error, otherwise return error
        // See: https://stackoverflow.com/a/65760807, https://github.com/rust-lang/rust/issues/62569
        if let Err(err) = command.run(config).await {
            if let PopgetterCliError::IOError(err) = &err {
                if err.kind() == std::io::ErrorKind::BrokenPipe {
                    return Ok(());
                }
            }
            Err(err)?;
        }
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
