#!/usr/bin/env bash
# This script is used to download metadata which this library needs at
# compile-time. Specifically, it downloads the JSON schema which the main
# popgetter library exports.
#
# Usage:
#    ./setup-metadata.sh [azure|github]
#
# Use 'azure' to just download the JSON from an Azure blob storage container,
# or 'github' to install the popgetter library and export the schema. If no
# argument is provided, the script will default to 'azure'.

set -e

SOURCE=${1:-"azure"}
DIR="$( dirname -- "${BASH_SOURCE[0]}"; )"
OUTPUT_FILE="${DIR}/schema.json"

if [ "${SOURCE}" = "azure" ]; then
    echo "Downloading schema from Azure blob storage..."
    curl -s "https://popgetter.blob.core.windows.net/popgetter-cli-test/schema.json" -o ${OUTPUT_FILE}
    echo "Done."
elif [ "${SOURCE}" = "github" ]; then
    echo "Exporting schema from popgetter."
    echo "Setting up virtual environment..."
    VENV_DIR="${DIR}/.popgetter_venv_temp"
    python -m venv ${VENV_DIR}
    source ${VENV_DIR}/bin/activate
    echo "Installing popgetter..."
    python -m pip install -q git+https://github.com/Urban-Analytics-Technology-Platform/popgetter.git@main
    echo "Exporting schema..."
    popgetter-export-schema > ${OUTPUT_FILE} 2>/dev/null
    echo "Cleaning up..."
    deactivate
    rm -rf ${VENV_DIR}
    echo "Done."
else
    echo "Usage: $0 [azure|github]"
    exit 1
fi
