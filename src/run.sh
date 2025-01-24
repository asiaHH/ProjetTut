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
    if ! [ -f ready.csv ]
    then
        echo "Getting head..."
        head -n 1 en.openfoodfacts.org.products.csv | tr '-' '_' > h.txt && \
        echo "Getting content..." && \
        cat en.openfoodfacts.org.products.csv | sed -n '2,$p' > c.txt && \
        echo "Concatenate..." && \
        cat h.txt c.txt > hc.csv && \
        echo "Fixing formatting..." && \
        awk -f fix_csv.awk hc.csv > ready.csv && \
        echo "Fixed."
    fi
}

function _import() {
    if [ -f ready.csv ]
    then
        echo "Importing..."
        export PGPASSWORD='etudiant' && \
        psql -U etudiant madm2023 < import.sql && \
        echo "Done."
    fi
}

_download
_fix
_import


