use langchain_rust::{
    embedding::openai::OpenAiEmbedder,
    llm::{AzureConfig, OpenAI},
    vectorstore::qdrant::{Qdrant, Store, StoreBuilder},
};

use crate::error::PopgetterLLMResult;

// TODO: make config
const GPT4O_ENDPOINT: &str = "https://popgetterllm.openai.azure.com";
const GPT4O_API_VERSION: &str = "2024-08-01-preview";
const GPT4O_DEPLOYMENT_ID: &str = "gpt-4o";
const EMBEDDING_ENDPOINT: &str = "https://popgetterllm.openai.azure.com";
const EMBEDDING_API_VERSION: &str = "2023-05-15";
const EMBEDDING_DEPLOYMENT_ID: &str = "text-embedding-3-small";

pub fn api_key() -> anyhow::Result<String> {
    Ok(std::env::var("AZURE_OPEN_AI_KEY")?)
}

pub fn azure_open_ai_gpt4o(api_key: &str) -> OpenAI<AzureConfig> {
    let azure_config = AzureConfig::default()
        .with_api_key(api_key)
        .with_api_base(GPT4O_ENDPOINT)
        .with_api_version(GPT4O_API_VERSION)
        .with_deployment_id(GPT4O_DEPLOYMENT_ID);
    OpenAI::new(azure_config)
}

pub fn azure_open_ai_embedding(api_key: &str) -> OpenAiEmbedder<AzureConfig> {
    let azure_config = AzureConfig::default()
        .with_api_key(api_key)
        .with_api_base(EMBEDDING_ENDPOINT)
        .with_api_version(EMBEDDING_API_VERSION)
        .with_deployment_id(EMBEDDING_DEPLOYMENT_ID);
    OpenAiEmbedder::new(azure_config)
}

pub async fn get_store() -> PopgetterLLMResult<Store> {
    // Initialize Embedder
    let embedder = azure_open_ai_embedding(&api_key()?);

    // Initialize the qdrant_client::Qdrant
    // Ensure Qdrant is running at localhost, with gRPC port at 6334
    // docker run -p 6334:6334 qdrant/qdrant
    let client = Qdrant::from_url("http://localhost:6334").build().unwrap();

    // Init store
    Ok(StoreBuilder::new()
        .embedder(embedder)
        .client(client)
        .collection_name("popgetter")
        .build()
        .await?)
}
