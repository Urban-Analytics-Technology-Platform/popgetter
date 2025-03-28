# About Popgetter

Popgetter is a collection of tools, designed to make it convenient to download census data from a number of different jurisdictions and coercing the data into common formats. The aim is that city or region scale analysis can be easily replicated for different geographies, using the most detailed, locally available data.

The tools are:

- **[popgetter-core](popgetter-core.md)**: The core library, which provides the data structures and functions to manipulate the data. This library is written in Rust and can be used directly in other Rust projects.
- **[popgetter-cli](popgetter-cli.md)**: A command line interface to the core library, which provides a simple way to download and manipulate data.
- **[popgetter-py](popgetter-py.md)**: A Python wrapper around the core library, which provides a simple way to download and manipulate data in Python.
- **[popgetter-browser](https://popgetter.readthedocs.io/projects/popgetter-browser/en/latest/)**: A web application to browse the data and metadata.
- **[poppusher](https://popgetter.readthedocs.io/projects/poppusher/en/latest/)**: A standalone tool to collate the source data into a format ready for use by popgetter. 
