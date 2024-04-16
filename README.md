# popgetter-cli

Library and associated command-line application for exploring and fetching [popgetter](https://github.com/Urban-Analytics-Technology-Platform/popgetter) data.


### Updating popgetter types

Apart from the data itself, popgetter also exports type-level information _about_ this data, i.e. what fields are present and what types they have.
This information is stored as a JSON schema.
Since the Rust library requires this information at compile-time, the JSON is included in the repository at `src/schema.json`.
To generate this file, run from the top level of this repository: `./setup-metadata.sh`.
