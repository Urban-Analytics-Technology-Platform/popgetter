# popgetter-llm (experimental)

`popgetter-llm` provides library functionality for LLM applications with Popgetter.

Please refer to [`popgetter-cli` (LLM integration)](https://crates.io/crates/popgetter-cli) for a guide on using the LLM integration from the Popgetter CLI.

## Aim and general approach

The `popgetter-llm` crate aims to build a natural language interface for Popgetter that enables the user to execute queries like:
> "Produce me a file containing the population breakdown by age of men living in Manchester as a geojson"

An overview of the general approach take towards this is:
- Create some tools that interface with the popgetter library to execute generated recipes
- Create an LLM agent to generate those recipes from the query
- Allow that agent to be able to search our metadata for relevant metrics, geometries, etc for the query

## Workflow

The LLM integration comprises two distinct parts: initialization and querying.

### Initialization
- Populate the vector database with the embeddings for each metric description.

### Query, search results and recipe
- User enters prompt for the LLM to consider the user's query and produce a list of datasets that will be required to answer it
- Generate an embedding vector from the prompt using the embedding API
- Find the top `n` closest vectors in the database
- Populate a new query with the metadata from those results along with the original query and prompt the generation of the recipe
- Run returned recipe to generate the resulting data
