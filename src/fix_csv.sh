#!/usr/bin/env bash

csv_file="../res/fr.openfoodfacts.org.products.csv"
output_file="../etl/ready.csv"

cat $csv_file | awk -f fix_csv.awk > $output_file
