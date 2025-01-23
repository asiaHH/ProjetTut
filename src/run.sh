#!/usr/bin/env bash

function _download() {
    wget https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz && \
    echo "Extracting..." && \
    gzip -d en.openfoodfacts.org.products.csv.gz && \
    echo "Done."
}

function _fix() {
    if [ -f en.openfoodfacts.org.products.csv ]
    then
        echo "Fixing formatting..."
        awk -f fix_csv.awk en.openfoodfacts.org.products.csv > ready.csv && \
        echo "Fixed."
    fi
}

function _import() {
    if [ -f ready.csv ]
    then
        echo "Importing..."
        psql
    fi
}

_download
_fix
_import

