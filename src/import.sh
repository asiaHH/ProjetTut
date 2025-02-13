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
    if ! [ -f col.txt ]
    then
        echo "Fixing..."
        cat en.openfoodfacts.org.products.csv | tr '"' '_' > ready.csv
        head -n 1 ready.csv | tr '\t' '\n' | tr '-' '_' > col.txt
        echo "Fixed."
    fi
}

function _import() {
    if ! [ -f import.sql ]
    then
        echo "Importing..."
        # We create a table with the same columns as the csv file
        echo -n "DROP TABLE IF EXISTS off_origin;CREATE TABLE off_origin (" > import.sql
        cat col.txt | awk '{print $1" TEXT NULL,"}' | tr '\n' ' ' | sed 's/,.$//' >> import.sql
        echo ");" >> import.sql
        echo "\copy off_origin FROM 'ready.csv' WITH (FORMAT csv, DELIMITER E'\t', HEADER true);" >> import.sql
        export PGPASSWORD='gb232322' && \
        psql -U gb232322 gb232322 < import.sql && \
        echo "Done."
    fi
}

function _stats() {
    if ! [ -f stats.sql ]
    then
        echo "Stats..."
        # For each column, we select all distinct values and count their occurences and store it in a new table called off_stats_colname
        cat col.txt | awk '{print "DROP TABLE IF EXISTS off_stats_"$1";CREATE TABLE off_stats_"$1" AS SELECT "$1", COUNT(*) FROM off_origin GROUP BY "$1";"}' > stats.sql
        export PGPASSWORD='gb232322' && \
        psql -U gb232322 gb232322 < stats.sql && \
        echo "Done."
    fi
}

mkdir -p data && cd data

_download
_fix
_import
_stats

cd -
