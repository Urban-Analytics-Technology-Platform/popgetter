mod cli;

use anyhow::Result;
use clap::Parser;
use cli::{Cli, RunCommand};

fn main() -> Result<()> {
    let args = Cli::parse();
    if let Some(command) = args.command {
        command.run()?;
    }
    Ok(())
}
