mod cli;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, RunCommand};
use log::debug;
use popgetter::config;

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();

    let config = config::init();
    debug!("config: {config:?}");

    let args = Cli::parse();
    debug!("args: {args:?}");

    if let Some(command) = args.command {
        command.run(&config).await?;
    }
    Ok(())
}
