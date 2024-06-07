mod cli;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, RunCommand};
use log::debug;

#[tokio::main]
async fn main() -> Result<()> {
    pretty_env_logger::init_timed();

    let args = Cli::parse();
    debug!("args: {args:?}");

    if let Some(command) = args.command {
        command.run().await?;
    }
    Ok(())
}
