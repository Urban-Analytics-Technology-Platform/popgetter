# popgetter_py

Python bindings for popgetter library for searching and downloading [popgetter](https://github.com/Urban-Analytics-Technology-Platform/popgetter) data.

## Quickstart

Install:

- [Python](https://www.python.org/)
- [Rust](https://www.rust-lang.org/tools/install)
- [maturin](https://github.com/PyO3/maturin)

Create a virtual environment and activate (e.g. with zsh):

```shell
python -m venv .venv
source .venv/bin/activate
```

Install polars and popgetter:

```shell
pip install polars
git clone https://github.com/Urban-Analytics-Technology-Platform/popgetter-cli.git
cd popgetter-cli/popgetter_py/
maturin develop --release
```

## Examples

Search and download data with popgetter using text, metric IDs or search params:

```python
import popgetter

# Search and download data with text or comma-separated metric IDs
metric_ids = "f29c1976,079f3ba3,81cae95d"

# Get search results
search_results = popgetter.search(metric_ids)
print(search_results)

# Get data
data = popgetter.download(metric_ids)
print(data.head())

# Search and download data with search params
search_params = {
    "metric_id": [],
    "text": [{
      "text": "Key: uniqueID, Value: B01001_001;",
      "context": ["Hxl", "HumanReadableName", "Description"]
    }],
    "geometry_level": "tract",
    "year_range": [{"Between": [2021, 2021]}],
    "country": "USA",
    "region_spec": [
      {"BoundingBox": [-74.251785, 40.647043, -73.673286, 40.91014]}
    ]
}
search_results = popgetter.search(search_params)
print(search_results)
data = popgetter.download(search_params)
print(data.head())
```

Download data with popgetter using a data request spec (see e.g. [test_recipe.json](../test_recipe.json))

```python
import popgetter

# Download data with a data request spec
data_request_spec = {
  "region": [
    {"BoundingBox": [-74.251785, 40.647043, -73.673286, 40.91014]}
  ],
  "metrics": [
    {"MetricId": "f29c1976"},
    {"MetricId": "079f3ba3"},
    {"MetricId": "81cae95d"},
    {"MetricText": "Key: uniqueID, Value: B01001_001;"}
  ],
  "years": ["2021"],
  "geometry": {
    "geometry_level": "tract",
    "include_geoms": True
  }
}

# Get data
data = popgetter.download_data_request(data_request_spec)
print(data.head())
```
