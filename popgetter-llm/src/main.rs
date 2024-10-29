use clap::{Args, Parser, Subcommand};
use langchain_rust::vectorstore::qdrant::{Qdrant, StoreBuilder};
use popgetter_llm::{
    embedding::{init_embeddings, query_embeddings},
    utils::{api_key, azure_open_ai_embedding},
};

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

#[derive(Args)]
struct QueryArgs {
    #[arg(index = 1)]
    query: String,
    #[arg(long)]
    limit: usize,
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

    match cli.command {
        Commands::Init => {
            // Init embeddings
            init_embeddings(&mut store).await?;
        }
        Commands::Query(query_args) => {
            // TODO: see if we can subset similarity search by metadata values
            let results = query_embeddings(&query_args.query, query_args.limit, &store).await?;

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
    }
    Ok(())
}
