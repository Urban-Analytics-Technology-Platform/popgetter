use indoc::indoc;
use itertools::Itertools;
use langchain_rust::{
    chain::{Chain, LLMChainBuilder},
    fmt_message, fmt_template, message_formatter,
    prompt::HumanMessagePromptTemplate,
    prompt_args,
    schemas::messages::Message,
    template_fstring,
    vectorstore::qdrant::Store,
};
use polars::prelude::*;
use popgetter::{
    data_request_spec::{DataRequestSpec, GeometrySpec, MetricSpec},
    Popgetter, COL,
};
use serde::{Deserialize, Serialize};

use crate::{
    embedding::query_embeddings,
    error::PopgetterLLMResult,
    utils::{api_key, azure_open_ai_gpt4o},
};

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

// Process:
// Step 1 (Stuart to start): get BBoxes (mapbox geocoder)
// Step 2 (move Qdrant protoypes into lib): provide top n metrics of interest
// Step 3 (Sam to start): combine above and ask to generate recipe that looks like a DataRequestSpec
//   - System prompt: Rust structs (e.g. DataRequestSpec), vec of BBoxes, vec of Metric details (e.g. top n)
//   - Return expected to be the recipe.json

// TODO (step 1): add function to take Vec<GeographicEntit> and return Vec<BBox> (use an external API endpoint)

pub async fn extract_geographic_entities(
    prompt: &str,
) -> PopgetterLLMResult<Vec<GeographicEntity>> {
    let open_ai = azure_open_ai_gpt4o(&api_key()?);

    // We can also guide it's response with a prompt template. Prompt templates are used to convert raw user input to a better input to the LLM.
    let system_prompt = message_formatter![
        fmt_message!(Message::new_system_message(indoc! {r#"
        You are a very accomplished geographer. Extract a list of geographic entities or areas, such
        as Scotland, Manchester, Hackney etc from the user prompt.

        Your output should always be in JSON format with the following as an example
            [{
                "place":"Glasgow"
            }]
        "#})),
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

pub async fn generate_recipe(
    prompt: &str,
    store: &Store,
    popgetter: &Popgetter,
    limit: usize,
    use_metric_ids: bool,
) -> PopgetterLLMResult<DataRequestSpec> {
    // Step 1: generate the BBoxes
    // TODO: update this to get the exact BBox
    // let _geographic_entities = extract_geographic_entities(prompt).await?;

    // Step 2: generate suggested metrics
    let top_metrics = query_embeddings(prompt, limit, store).await?;

    let metric_ids = Series::new(
        "",
        top_metrics
            .iter()
            // TODO: fix unwraps
            .map(|result| {
                result
                    .metadata
                    .get(COL::METRIC_ID)
                    .unwrap()
                    .as_str()
                    .unwrap()
            })
            .collect_vec(),
    );

    // TODO: refine API for recipe generation but for now use either the human readable names or the
    // metric IDs
    let metric_col = if use_metric_ids {
        COL::METRIC_ID
    } else {
        COL::METRIC_HUMAN_READABLE_NAME
    };
    // Get human readable names (TODO: consider other columns) and add \n\n
    let results = popgetter
        .metadata
        .combined_metric_source_geometry()
        .as_df()
        .filter(col(COL::METRIC_ID).is_in(lit(metric_ids)))
        .select([col(metric_col)])
        .collect()?
        .column(metric_col)
        .into_iter()
        .next()
        // Unwrap: a column is selected so will not be none
        .unwrap()
        .str()?
        .into_iter()
        .flatten()
        .collect_vec()
        .join("\n\n");

    // Step 3: With a new prompt with data request spec and top metrics, send query
    let open_ai = azure_open_ai_gpt4o(&api_key()?);

    // We can also guide it's response with a prompt template. Prompt templates are used to convert raw user input to a better input to the LLM.
    let system_prompt = message_formatter![
        fmt_message!(Message::new_system_message(indoc! {r#"
            You are a very accomplished geographer and can interpret Rust data types.

            Convert the following set of metrics into a `Vec<MetricSpec>`. The `MetricSpec` is a Rust type:
            ```
            #[derive(Clone, Serialize, Deserialize, Debug)]
            pub enum MetricSpec {
                MetricId(MetricId),
                MetricText(String),
                DataProduct(String),
            }
            ```

            Your output should always be in JSON format with the following as an example of a JSON
            version of a `DataRequestSpec`:
            ```json
            [
                {
                    "MetricId": {
                    "id": "f29c1976"
                    }
                },
                {
                    "MetricId": {
                    "id": "079f3ba3"
                    }
                },
                {
                    "MetricId": {
                    "id": "81cae95d"
                    }
                },
                {
                    "MetricText": "Key: uniqueID, Value: B01001_001;"
                }
            ]
            ```
            Ignore all references to location and instead populate the metrics specified only.

            Only return the JSON string without any code backticks."#})),
        fmt_template!(HumanMessagePromptTemplate::new(template_fstring!(
            "{input}", "input"
        )))
    ];

    // We can now combine these into a simple LLM chain:
    let chain = LLMChainBuilder::new()
        .prompt(system_prompt)
        .llm(open_ai.clone())
        .build()?;

    // We can now invoke it and ask the same question. It still won't know the answer, but it should
    // respond in a more proper tone for a technical writer!
    // let combined_query_and_metrics = format!("Query: {prompt}\n\nTop metric results: {results}");
    let combined_query_and_metrics = format!("Top metric results: {results}");
    let raw_result = chain
        .invoke(prompt_args! {
            "input" => combined_query_and_metrics
        })
        .await?;

    // Once can generate a data request spec, deserialize to type
    // let result: DataRequestSpec = serde_json::from_str(&raw_result)?;
    // Ok(raw_result)
    log::debug!("{raw_result}");

    let result: Vec<MetricSpec> = serde_json::from_str(&raw_result)?;

    Ok(DataRequestSpec {
        geometry: Some(GeometrySpec {
            geometry_level: None,
            include_geoms: true,
        }),
        // TODO: add BBox from step 1 query
        // region: RegionSpec::BoundingBox(()) {
        // },
        region: vec![],
        metrics: result,
        years: None,
    })
}

#[cfg(test)]
mod tests {

    use langchain_rust::language_models::llm::LLM;
    use pretty_env_logger::env_logger;

    use crate::utils::{azure_open_ai_gpt4o, get_store};

    use super::*;

    const TEST_PROMPT: &str = "Test prompt";

    #[tokio::test]
    async fn test_llm_example() {
        let open_ai = azure_open_ai_gpt4o(&api_key().unwrap());
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

    #[tokio::test]
    async fn data_request_spec_should_be_extracted() {
        let _ = env_logger::try_init();
        let prompt = r#"Build a dataset of the population of men over 20 in Glasgow, London and Hackney.
        Also for population in Leith which is within Ediburgh."#;

        let popgetter = Popgetter::new_with_config_and_cache(Default::default())
            .await
            .unwrap();
        let store = get_store().await.unwrap();
        let result = generate_recipe(prompt, &store, &popgetter, 10, false)
            .await
            .unwrap();
        println!("{:?}", result);
    }

    #[tokio::test]
    async fn data_request_spec_should_be_downloaded() {
        let _ = env_logger::try_init();
        let prompt = r#"Build a dataset of the population of men over 20 in Glasgow, London and Hackney.
        Also for population in Leith which is within Ediburgh."#;

        let popgetter = Popgetter::new_with_config_and_cache(Default::default())
            .await
            .unwrap();
        let store = get_store().await.unwrap();

        // TODO: to ensure only one geometry, currently limit to 1 result
        let result = generate_recipe(prompt, &store, &popgetter, 1, true)
            .await
            .unwrap();
        println!("{:?}", result);

        let df = popgetter.download_data_request_spec(&result).await.unwrap();
        println!("{}", df.head(None));
    }
}
