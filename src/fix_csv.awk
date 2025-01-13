#!/bin/awk

# This script is used to preprocess the csv file
# It replaces the tabs in the records with a comma
# and put the records in double quotes

BEGIN {
    FS = "\t"
    OFS = ","
}

{
    for (i = 1; i <= NF; i++) {
        gsub(/"/, "\"\"", $i)
        $i = "\"" $i "\""
    }
    print
}
