#!/usr/bin/env bash

function _download() {
    if ! [ -f en.openfoodfacts.org.products.csv ]
    then
        wget https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz && \
        echo "Extracting..." && \
        gzip -d en.openfoodfacts.org.products.csv.gz && \
        echo "Done."
    fi
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
        export PGPASSWORD='etudiant'
        psql -U etudiant madm2023 -f import.sql
        echo "Done."
    fi
}

_download
_fix
_import

