# popgetter-cli

Library and associated command-line application for exploring and fetching [popgetter](https://github.com/Urban-Analytics-Technology-Platform/popgetter) data.

## Quickstart

- Install [Rust](https://www.rust-lang.org/tools/install)
- Install CLI:
  ```shell
  cargo install popgetter-cli
  ```
- Run the CLI with e.g.:
  ```shell
  popgetter --help
  ```

## About the data

The data used by `popgetter` is collated into a format ready for use, by a tool called [`poppusher`](https://github.com/Urban-Analytics-Technology-Platform/poppusher). See that tool for details about which data is available.

## Popgetter and Poppusher version compatibility

Each version of `popgetter-core` is tied to one specific version of `poppusher` to ensure consistency of data _types_. Changes to `popgetter-core` may require changes to downstream [dependents](https://crates.io/crates/popgetter-core/reverse_dependencies).
(Note that updates to the actual data and metadata themselves do not lead to a version bump.)

| poppusher | popgetter-core | popgetter-cli | popgetter-py |
| --------- | -------------- | ------------- | ------------ |
| 0.1.0     | N/A            | N/A           | N/A          |
| 0.2.0     | 0.2.0          | 0.2.0         | 0.2.0        |
| 0.2.0     | 0.2.1          | 0.2.1         | 0.2.1        |
| ...       | ...            |               |              |


## Developer guide

### Editable install from source

- Install [Rust](https://www.rust-lang.org/tools/install)
- Clone the repo:
  ```shell
  git clone git@github.com:Urban-Analytics-Technology-Platform/popgetter.git
  cd popgetter
  ```
- Build:
  ```shell
  cargo build
  ```
  and in release mode:
  ```shell
  cargo build --release
  ```
- Run the CLI with e.g.:
  ```shell
  cargo run --bin popgetter -- --help
  ```


### Release process

The release process in managed by the [`release-plz`](https://release-plz.dev/docs/github) GitHub Action ([workflow](.github/workflows/python.yml)).

Note: the `popgetter-py` module is deployed to [TestPyPI](https://test.pypi.org/project/popgetter/) at the earilest opportunity (eg when a PR is created which bumps the version number). However it is not deployed to [crates.io](https://crates.io/crates/popgetter-py) until that PR is merged to `main`.
