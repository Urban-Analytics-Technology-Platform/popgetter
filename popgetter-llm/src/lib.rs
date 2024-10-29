use anyhow::Result;
use langchain_rust::{
    chain::{Chain, LLMChainBuilder},
    fmt_message, fmt_template,
    llm::{openai::OpenAI, AzureConfig},
    message_formatter,
    prompt::HumanMessagePromptTemplate,
    prompt_args,
    schemas::messages::Message,
    template_fstring,
};
use serde::{Deserialize, Serialize};

const ENDPOINT_GPT4O: &str = "https://popgetterllm.openai.azure.com";
const ENDPOINT_EMBEDDING: &str = "https://popgetterllm.openai.azure.com/openai/deployments/text-embedding-3-small/embeddings?api-version=2023-05-15";

fn get_open_ai() -> OpenAI<AzureConfig> {
    // Azure config
    let azure_config = AzureConfig::default()
        .with_api_key(api_key().unwrap())
        .with_api_base(ENDPOINT_GPT4O)
        .with_api_version("2024-08-01-preview")
        .with_deployment_id("gpt-4o");
    OpenAI::new(azure_config)
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct GeographicEntity {
    pub place: String,
}

impl GeographicEntity {
    pub fn new(place: &str) -> GeographicEntity {
        GeographicEntity {
            place: place.into(),
        }
    }
}

pub fn api_key() -> anyhow::Result<String> {
    Ok(std::env::var("AZURE_OPEN_AI_KEY")?)
}

// Process:
// Step 1 (Stuart to start): get BBoxes (mapbox geocoder)
// Step 2 (move Qdrant protoypes into lib): provide top n metrics of interest
// Step 3 (Sam to start): combine above and ask to generate recipe that looks like a DataRequestSpec
//   - System prompt: Rust structs (e.g. DataRequestSpec), vec of BBoxes, vec of Metric details (e.g. top n)
//   - Return expected to be the recipe.json

// TODO (step 1): add function to take Vec<GeographicEntit> and return Vec<BBox> (use an external API endpoint)

pub async fn extract_geographic_entities(prompt: &str) -> Result<Vec<GeographicEntity>> {
    let open_ai = get_open_ai();

    // We can also guide it's response with a prompt template. Prompt templates are used to convert raw user input to a better input to the LLM.
    let system_prompt = message_formatter![
        fmt_message!(Message::new_system_message(
            r#"\You are a very accomplished geographer. Extract a list of geographic entities or \
            areas, such as Scotland, Manchester, Hackney etc from the user prompt.

            Your output should always be in JSON format with the following as an example
                [{
                    "place":"Glasgow"
                }]
                "#
        )),
        fmt_template!(HumanMessagePromptTemplate::new(template_fstring!(
            "{input}", "input"
        )))
    ];

    // We can now combine these into a simple LLM chain:
    let chain = LLMChainBuilder::new()
        .prompt(system_prompt)
        .llm(open_ai.clone())
        .build()
        .unwrap();

    // We can now invoke it and ask the same question. It still won't know the answer, but it should
    // respond in a more proper tone for a technical writer!
    let raw_result = chain
        .invoke(prompt_args! {
            "input" => prompt,
        })
        .await?;
    let result: Vec<GeographicEntity> = serde_json::from_str(&raw_result)?;
    Ok(result)
}

#[cfg(test)]
mod tests {

    use langchain_rust::language_models::llm::LLM;

    use super::*;

    const TEST_PROMPT: &str = "Test prompt";

    #[tokio::test]
    async fn test_llm_example() {
        let open_ai = get_open_ai();
        let response = open_ai.invoke(TEST_PROMPT).await.unwrap();
        println!("{}", response);
    }

    #[tokio::test]
    async fn geopgraphic_entries_should_be_extracted() {
        let expected_entries = vec![
            GeographicEntity::new("Glasgow"),
            GeographicEntity::new("London"),
            GeographicEntity::new("Hackney"),
            GeographicEntity::new("Leith"),
            GeographicEntity::new("Edinburgh"),
        ];
        let prompt = r#"Build a dataset of the population of men over 20 in Glasgow, London and Hackney.
        Also for population in Leith which is within Ediburgh."#;

        let entries: Vec<GeographicEntity> = extract_geographic_entities(prompt).await.unwrap();
        println!("{:#?}", entries);

        // Assert all entries are in the same order and have the same value as expected
        assert!(entries
            .into_iter()
            .zip(expected_entries)
            .all(|(actual, expected)| actual.eq(&expected)))
    }
}
