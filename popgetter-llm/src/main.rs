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

use std::io::Write;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize Embedder

    use langchain_rust::vectorstore::VecStoreOptions;

    let endpoint = "https://demolandllm.openai.azure.com/";
    let azure_config = AzureConfig::default()
        .with_api_key(std::env::var("AZURE_OPEN_AI_KEY")?)
        .with_api_base(endpoint)
        .with_api_version("2023-05-15")
        // .with_deployment_id("text-embedding-ada-002");
        .with_deployment_id("popgetter");

    let embedder = OpenAiEmbedder::new(azure_config);

    // Requires OpenAI API key to be set in the environment variable OPENAI_API_KEY
    // let embedder = OpenAiEmbedder::default();

    // Initialize the qdrant_client::Qdrant
    // Ensure Qdrant is running at localhost, with gRPC port at 6334
    // docker run -p 6334:6334 qdrant/qdrant
    let client = Qdrant::from_url("http://localhost:6334").build().unwrap();

    let store = StoreBuilder::new()
        .embedder(embedder)
        .client(client)
        .collection_name("langchain-rs")
        .build()
        .await
        .unwrap();

    // Add documents to the database
    let doc1 = Document::new(
        "langchain-rust is a port of the langchain python library to rust and was written in 2024.",
    );
    let doc2 = Document::new(
        "langchaingo is a port of the langchain python library to go language and was written in 2023."
    );
    let doc3 = Document::new(
        "Capital of United States of America (USA) is Washington D.C. and the capital of France is Paris."
    );
    let doc4 = Document::new("Capital of France is Paris.");
    let doc5 = Document::new("The sky is blue because of nitrogen");

    store
        .add_documents(
            &vec![doc1, doc2, doc3, doc4, doc5],
            &VecStoreOptions::default(),
        )
        .await
        .unwrap();

    // Ask for user input
    // print!("Query> ");
    // std::io::stdout().flush().unwrap();
    let mut query = "why is the sky blue?".to_string();
    // std::io::stdin().read_line(&mut query).unwrap();

    let results = store
        .similarity_search(&query, 4, &VecStoreOptions::default())
        .await
        .unwrap();

    if results.is_empty() {
        println!("No results found.");
        return Ok(());
    } else {
        results.iter().for_each(|r| {
            // println!("Document: {:?}", r.page_content);
            println!("Document: {:?}", r);
        });
    }
    Ok(())
}
