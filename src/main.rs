mod cli;

use anyhow::Result;
use clap::Parser;
use cli::*;

fn main() -> Result<()> {
    let args = Cli::parse();
    if let Some(command) = args.command {
        match command {
            Commands::Countries => println!("You selected the countries command "),
            Commands::Data(data_args) => {
                println!("You selected the Data command {:#?}", data_args)
            }
            Commands::Metrics(metric_args) => {
                println!("You selected the Metrics command {:#?}", metric_args)
            }
            Commands::Surveys => println!("You selected the surveys command"),
        }
    }
    Ok(())
}
