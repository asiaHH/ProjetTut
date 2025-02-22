#!/usr/bin/env bash

_lang=fr
_file=$_lang.openfoodfacts.org.products.csv

function _download() {
    if ! [ -f $_file ]
    then
        if ! [ -f $_file.gz ]
        then
            wget https://static.openfoodfacts.org/data/$_file.gz
        fi
        echo "Extracting..." && \
        gzip -d $_file.gz && \
        echo "Done."
    fi
}

function _fix() {
    if ! [ -f col.txt ]
    then
        echo "Fixing..."
        cat $_file | tr '"' '_' > ready.csv
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
        cat col.txt | awk '{print "DROP TABLE IF EXISTS off_origin_stats_"$1";CREATE TABLE off_origin_stats_"$1" AS SELECT "$1", COUNT(*) FROM off_origin GROUP BY "$1";"}' > stats.sql
        export PGPASSWORD='gb232322' && \
        psql -U gb232322 gb232322 < stats.sql && \
        echo "Done."
    fi
}

function _export() {
    if ! [ -f export.sql ]
    then
        echo "Exporting..."
        cat col.txt | awk '{print "\copy off_origin_stats_"$1" TO off_origin_stats_"$1".csv WITH (FORMAT csv, HEADER true);"}' > export.sql
        export PGPASSWORD='gb232322' && \
        psql -U gb232322 gb232322 < export.sql && \
        echo "Done."
    fi
}       

mkdir -p data && cd data

_download
_fix
_import
_stats
_export

cd -
