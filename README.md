# popgetter-cli

Library and associated command-line application for exploring and fetching [popgetter](https://github.com/Urban-Analytics-Technology-Platform/popgetter) data.

## Quickstart

- Install [Rust](https://www.rust-lang.org/tools/install)
- Install CLI:
  ```shell
  cargo install --git https://github.com/Urban-Analytics-Technology-Platform/popgetter-cli.git
  ```
- Run the CLI with e.g.:
  ```shell
  popgetter --help
  ```

### Popgetter version compatibility

Each version of `popgetter-cli` is tied to one specific version of `popgetter` to ensure consistency of data _types_.
(Note that updates to the actual data and metadata themselves do not lead to a version bump.)

| popgetter | popgetter-cli |
| --------- | ------------- |
| 0.1.0     |               |
| 0.2.0     | 0.2.0         |
| ...       | ...           |
