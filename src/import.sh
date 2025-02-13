#!/usr/bin/env bash

function _download() {
    if ! [ -f en.openfoodfacts.org.products.csv ]
    then
        if ! [ -f en.openfoodfacts.org.products.csv.gz ]
        then
            wget https://static.openfoodfacts.org/data/en.openfoodfacts.org.products.csv.gz
        fi
        echo "Extracting..." && \
        gzip -d en.openfoodfacts.org.products.csv.gz && \
        echo "Done."
    fi
}

function _fix() {
    if ! [ -f ready.csv ]
    then
        echo "Fixing..."
        cat en.openfoodfacts.org.products.csv | tr '"' '_' > ready.csv
        echo "Fixed."
    fi
}

function _import() {
    if [ -f ready.csv ]
    then
        echo "Importing..."
        export PGPASSWORD='gb232322' && \
        psql -U gb232322 gb232322 < import.sql && \
        echo "Done."
    fi
}

_download
_fix
_import


