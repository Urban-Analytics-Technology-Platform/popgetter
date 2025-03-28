# About Popgetter

Popgetter is a collection of tools, designed to make it convenient to download census data from a number of different jurisdictions and coercing the data into common formats. The aim is that city or region scale analysis can be easily replicated for different geographies, using the most detailed, locally available data.

The tools are:

- **popgetter-core**: The core library, which provides the data structures and functions to manipulate the data. This library is written in Rust and can be used directly in other Rust projects.
- **popgetter-cli**: A command line interface to the core library, which provides a simple way to download and manipulate data.
- **popgetter-py**: A Python wrapper around the core library, which provides a simple way to download and manipulate data in Python.
- **[popgetter-browser](projects/popgetter-browser/)**: A web application to browse the data and metadata.
- **[poppusher](projects/poppusher/)**: A standalone tool to collate the source data into a format ready for use by popgetter. 
