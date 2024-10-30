use clap::{Args, Parser, Subcommand};
use langchain_rust::vectorstore::qdrant::{Qdrant, StoreBuilder};
use popgetter::Popgetter;
use popgetter_llm::{
    chain::generate_recipe,
    embedding::{init_embeddings, query_embeddings},
    utils::{api_key, azure_open_ai_embedding},
};
use serde::{Deserialize, Serialize};
use strum::EnumString;

#[derive(Parser)]
#[command(version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    Query(QueryArgs),
}

#[derive(Clone, Debug, Deserialize, Serialize, EnumString, PartialEq, Eq)]
#[strum(ascii_case_insensitive)]
enum OutputFormat {
    SearchResults,
    DataRequestSpec,
}

#[derive(Args)]
struct QueryArgs {
    #[arg(index = 1)]
    query: String,
    #[arg(long)]
    limit: usize,
    #[arg(long)]
    output_format: OutputFormat,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();

    // Initialize Embedder
    let embedder = azure_open_ai_embedding(&api_key()?);

    // Initialize the qdrant_client::Qdrant
    // Ensure Qdrant is running at localhost, with gRPC port at 6334
    // docker run -p 6334:6334 qdrant/qdrant
    let client = Qdrant::from_url("http://localhost:6334").build().unwrap();

    // Init store
    let mut store = StoreBuilder::new()
        .embedder(embedder)
        .client(client)
        .collection_name("popgetter")
        .build()
        .await?;

    let popgetter = Popgetter::new_with_config_and_cache(Default::default()).await?;

    match cli.command {
        Commands::Init => {
            // Init embeddings
            init_embeddings(&mut store).await?;
        }
        Commands::Query(query_args) => {
            match query_args.output_format {
                OutputFormat::SearchResults => {
                    // TODO: see if we can subset similarity search by metadata values
                    let results =
                        query_embeddings(&query_args.query, query_args.limit, &store).await?;

                    // TODO: Add filtering by metadata values (e.g. country)
                    // https://qdrant.tech/documentation/concepts/hybrid-queries/?q=color#re-ranking-with-payload-values
                    if results.is_empty() {
                        println!("No results found.");
                        return Ok(());
                    } else {
                        results.iter().for_each(|r| {
                            println!("Document: {:#?}", r);
                        });
                    }
                }
                OutputFormat::DataRequestSpec => {
                    let data_request_spec = generate_recipe(
                        &query_args.query,
                        &store,
                        &popgetter,
                        query_args.limit,
                        // TODO: uses human readable name to generate metric text, update to config
                        false,
                    )
                    .await?;
                    println!("Recipe:\n{:#?}", data_request_spec);
                }
            }
        }
    }
    Ok(())
}
