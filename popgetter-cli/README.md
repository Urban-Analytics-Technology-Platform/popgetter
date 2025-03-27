# popgetter-cli

Library and associated command-line application for exploring and fetching [popgetter](https://github.com/Urban-Analytics-Technology-Platform/popgetter) data.

## Quickstart

- Install [Rust](https://www.rust-lang.org/tools/install)
- Install CLI:
  ```sh
  cargo install popgetter-cli
  ```
- Run the CLI with e.g.:
  ```sh
  popgetter --help
  ```

## Examples

### List countries with `countries` subcommand

Get a list of available data:

```sh
popgetter countries
```

### Searching metadata with `metrics` subcommand

#### Summarising and specific metadata fields

Get a summary of all data:

```sh
popgetter metrics --summary
```

Get a summary of data for a given country:

```sh
popgetter metrics --summary --country "united states"
```

Get the list of metadata fields:

```sh
popgetter metrics --display-metadata-columns
```

Get a list of geometry levels for a given country:

```sh
popgetter metrics --country "united states" \
  --unique geometry_level
```

#### Searching metrics

An example search using a regex for search text combined with a given country and geometry level:

```sh
popgetter metrics \
  --text " car[^a-z] | cars " \
  --country "northern ireland" \
  --geometry-level sdz21
```

#### Downloading data

An example search using a regex for search text combined with a given country and geometry level:

```sh
popgetter data
  --id 38757cf9 \
  --output-file popgetter.geojson \
  --output-format geojson
  --dev
```

where the `--dev` flag is used here to enable output with CRS transformed to EPSG:4326 since all data is provided here in EPSG:4326.

#### Downloading data with recipes

Recipe files provide an alternative to using the command line flags. An example [recipe](../test_recipe.json) can be downloaded with:

```sh
popgetter recipe test_recipe.json \
  --output-format csv --output-file popgetter.csv
```

## LLM integration (experimental)

It is possible to also search and generate data requests supported by LLMs.

The below steps are required for this experimental functionality implemented in the [`popgetter-llm`](../popgetter-llm/) crate.

- Install with `llm` feature:

```sh
cargo install popgetter-cli --features llm
```

- Set-up two Azure LLM endpoints for:
  - Text embeddings (`text-embedding-3-small`)
  - Text generation (`gpt-4o`)
- Assign the API key for the two endpoints to the following environment variable, with e.g.:

```sh
export AZURE_OPEN_AI_KEY="REPLACE_WITH_API_KEY"
```

_Note: currently only Azure endpoints are supported._

- Install and run [Docker](https://www.docker.com/)
- Initialize the [Qdrant](https://qdrant.tech/) database:

  ```sh
  cd ../popgetter-llm/
  docker compose up
  ```

- Construct the database with embeddings derived from metadata using the popgetter CLI:

```sh
popgetter llm init
```

This process will take several hours to run and will construct the Qdrant database for all the metadata (around 3GB total size).

- With the database populated, search queries can be performed using the embeddings to:

  - Return search results based on embedding similarity
  - Generate a data request specifications directly from the query

- For search results based on embedding similarity, e.g.:

```sh
popgetter llm query \
  "cars and household size" \
  --limit 10 \
  --output-format SearchResults \
  --country "United States"
```

- With `output-format` set to `--output-format SearchResultsToRecipe`, the metric IDs from the search results are included in a recipe:

```sh
popgetter llm query \
  "cars and household size" \
  --limit 10 \
  --output-format SearchResultsToRecipe \
  --country "United States"
```

- With `output-format` set to `--output-format DataRequestSpec`, the data request specification is produced directly from the search results through a second prompt:

```sh
RUST_LOG=info popgetter llm query \
  "cars and household size" \
  --limit 10 \
  --output-format DataRequestSpec \
  --country "United States"
```

_Note: This output format is highly experimental and may produce incorrect data request specifications._
