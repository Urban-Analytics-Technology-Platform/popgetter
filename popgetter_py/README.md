# popgetter_py

Python bindings for popgetter library for searching and downloading [popgetter](https://github.com/Urban-Analytics-Technology-Platform/popgetter) data.

## Quickstart

- Install [Python](https://www.python.org/)
- Install [Rust](https://www.rust-lang.org/tools/install)
- Install [maturin](https://github.com/PyO3/maturin)
- Create a virtual environment and activate (e.g. with zsh):
  ```shell
  python -m venv .venv
  source .venv/bin/activate
  ```
- Install popgetter
  ```shell
  maturin develop --release
  ```
- Run popgetter with a data requst specification (see e.g. [test_recipe.json](../test_recipe.json)):
    ```python
    import json
    import popgetter
    with open("../test_recipe.json", "r") as f:
        df = popgetter.download_data_request(json.load(f))
    print(df.head())
    ```