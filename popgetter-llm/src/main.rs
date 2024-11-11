use clap::{Args, Parser, Subcommand};
use itertools::Itertools;
use langchain_rust::vectorstore::qdrant::{Qdrant, StoreBuilder};
use polars::prelude::*;
use popgetter::{
    search::{SearchParams, SearchResults},
    Popgetter, COL,
};
use popgetter_cli::{cli::SearchParamsArgs, display::display_search_results};
use popgetter_llm::{
    chain::generate_recipe,
    embedding::{init_embeddings, query_embeddings},
    utils::{api_key, azure_open_ai_embedding},
};

use qdrant_client::qdrant::{Condition, Filter};
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
    Init(InitArgs),
    Query(QueryArgs),
}

#[derive(Args)]
struct InitArgs {
    #[arg(long)]
    sample_n: Option<usize>,
    #[arg(long)]
    seed: Option<u64>,
    #[arg(long)]
    skip: Option<usize>,
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
    #[arg(long, help = "Number of results to be returned")]
    limit: usize,
    #[command(flatten)]
    search_params_args: SearchParamsArgs,
    #[arg(long, help = "Output format: 'SearchResults' or 'DataRequestSpec'")]
    output_format: OutputFormat,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set RUST_LOG to `DEFAULT_LOGGING_LEVEL` if not set
    const DEFAULT_LOGGING_LEVEL: &str = "info";
    let _ =
        std::env::var("RUST_LOG").map_err(|_| std::env::set_var("RUST_LOG", DEFAULT_LOGGING_LEVEL));
    pretty_env_logger::init_timed();

    let cli = Cli::parse();

    // Initialize Embedder
    let embedder = azure_open_ai_embedding(&api_key()?);

    // Initialize the qdrant_client::Qdrant
    // Ensure Qdrant is running at localhost, with gRPC port at 6334
    // docker run -p 6334:6334 qdrant/qdrant
    let client = Qdrant::from_url("http://localhost:6334").build().unwrap();

    let popgetter = Popgetter::new_with_config_and_cache(Default::default()).await?;

    match cli.command {
        Commands::Init(init_args) => {
            // Init store
            let mut store = StoreBuilder::new()
                .embedder(embedder)
                .client(client)
                .collection_name("popgetter")
                .build()
                .await?;
            // Init embeddings
            init_embeddings(
                &mut store,
                init_args.sample_n,
                init_args.seed,
                init_args.skip,
            )
            .await?;
        }
        Commands::Query(query_args) => {
            let search_params: SearchParams = query_args.search_params_args.into();
            // Init store
            let mut store_builder = StoreBuilder::new()
                .embedder(embedder)
                .client(client)
                .collection_name("popgetter");

            // Filtering by metadata values (e.g. country)
            // https://qdrant.tech/documentation/concepts/hybrid-queries/?q=color#re-ranking-with-payload-values
            // Add country as search filter if given
            if let Some(country) = search_params.country.as_ref() {
                let search_filter = Filter::must([Condition::matches(
                    "metadata.country",
                    country.value.to_string(),
                )]);
                store_builder = store_builder.search_filter(search_filter);
            }
            let store = store_builder.build().await?;

            match query_args.output_format {
                OutputFormat::SearchResults => {
                    // TODO: see if we can subset similarity search by metadata values
                    let results =
                        query_embeddings(&query_args.query, query_args.limit, &store).await?;

                    let ids = Series::new(
                        COL::METRIC_ID,
                        results
                            .iter()
                            .map(|doc| {
                                doc.metadata
                                    .get(COL::METRIC_ID)
                                    .unwrap()
                                    .as_str()
                                    .unwrap()
                                    .to_string()
                            })
                            .collect_vec(),
                    );

                    // Filter afterwards with `COL::METRIC_ID`
                    let results = popgetter
                        .search(&search_params)
                        .0
                        .lazy()
                        .filter(col(COL::METRIC_ID).is_in(lit(ids)))
                        .collect()
                        .unwrap();

                    if results.shape().0.eq(&0) {
                        println!("No results found.");
                        return Ok(());
                    } else {
                        display_search_results(SearchResults(results), None, false).unwrap();
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
