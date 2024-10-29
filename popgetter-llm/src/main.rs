// To run this example execute: cargo run --example vector_store_qdrant --features qdrant

use langchain_rust::{
    embedding::openai::openai_embedder::OpenAiEmbedder,
    llm::AzureConfig,
    schemas::Document,
    vectorstore::{
        qdrant::{Qdrant, StoreBuilder},
        VectorStore,
    },
};
use serde_json::Value;

use std::{collections::HashMap, io::Write};

use anyhow::anyhow;

use popgetter::{Popgetter, COL};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let color_map = HashMap::from([
        ("Scotland", "red"),
        ("Belgium", "green"),
        ("Northern Ireland", "blue"),
        ("United States", "yellow"),
        ("England and Wales", "orange"),
    ]);

    // Initialize Embedder

    use langchain_rust::vectorstore::VecStoreOptions;

    let endpoint = "https://demolandllm.openai.azure.com/";
    let azure_config = AzureConfig::default()
        .with_api_key(std::env::var("AZURE_OPEN_AI_KEY")?)
        .with_api_base(endpoint)
        .with_api_version("2023-05-15")
        .with_deployment_id("popgetter");

    let embedder = OpenAiEmbedder::new(azure_config);

    // Embedding
    // let result = embedder.embed_query("Why is the sky blue?").await.unwrap();
    // println!("{:?}", result);
    // println!("{:?}", result.len());

    // Requires OpenAI API key to be set in the environment variable OPENAI_API_KEY
    // let embedder = OpenAiEmbedder::default();

    // Initialize the qdrant_client::Qdrant
    // Ensure Qdrant is running at localhost, with gRPC port at 6334
    // docker run -p 6334:6334 qdrant/qdrant
    let client = Qdrant::from_url("http://localhost:6334").build().unwrap();

    let store = StoreBuilder::new()
        .embedder(embedder)
        .client(client)
        .collection_name("popgetter_1000_400")
        .build()
        .await
        .unwrap();

    let popgetter = Popgetter::new_with_config_and_cache(Default::default()).await?;
    let combined_metadata = popgetter
        .metadata
        .combined_metric_source_geometry()
        .0
        .collect()?;
    let mut v = vec![];
    for (description, country) in combined_metadata
        .column(COL::METRIC_HUMAN_READABLE_NAME)?
        .str()?
        .into_iter()
        .zip(
            combined_metadata
                .column(COL::COUNTRY_NAME_SHORT_EN)?
                .str()?
                .into_iter(),
        )
        .step_by(400)
        .take(1000)
    {
        let s: String = description.ok_or(anyhow!("Not a str"))?.into();

        // TODO: add method to return HashMap of a row with keys (columns) and values
        // Could just use the IDs and lookup in polars too.
        let mut hm: HashMap<String, Value> = HashMap::new();
        hm.insert(
            "country".to_owned(),
            Value::String(country.unwrap().to_string()),
        );
        hm.insert(
            "color".to_owned(),
            Value::String(color_map.get(country.unwrap()).unwrap().to_string()),
        );
        // TODO: add other metadata
        let doc = Document::new(s).with_metadata(hm);
        v.push(doc);
    }

    // // Add documents to the database
    // TODO: make this `popgetter llm init`
    // store
    //     .add_documents(&v, &VecStoreOptions::default())
    //     .await
    //     .unwrap();

    // `popgetter llm query "QUERY"`
    loop {
        // Ask for user input
        print!("Query> ");
        std::io::stdout().flush().unwrap();
        let mut query = "".to_string();
        std::io::stdin().read_line(&mut query).unwrap();

        // TODO: see if we can subset similarity search by metadata values
        let results = store
            .similarity_search(&query, 4, &VecStoreOptions::default())
            .await
            .unwrap();

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
