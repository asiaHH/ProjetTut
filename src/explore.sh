#!/usr/bin/env bash

_lang="fr"
_off_csv_file="$_lang.openfoodfacts.org.products.csv"
_sql_file="request.sql"
_csv_file="ready.csv"
_col_file="column.txt"
_schema_file="../schema.txt"

function _download() {
    if ! [ -f $_col_file ]
    then
        if ! [ -f $_file.gz ]
        then
            wget https://static.openfoodfacts.org/data/$_off_csv_file.gz
        fi
        echo "Extracting..." && \
        gzip -d $_off_csv_file.gz && \
        echo "Fixing..."
        cat $_off_csv_file | tr '"' '_' > $_csv_file
        head -n 1 $_csv_file | tr '\t' '\n' | tr '-' '_' > $_col_file
        sort $_col_file -o sorted_$_col_file &
        echo "Done."
    fi
}

function _import() {
    echo "Cleaning..."
    echo "" > $_sql_file

    echo "Importing..."
    # We create a table with the same columns as the csv file
    echo -n "DROP TABLE IF EXISTS off_origin;CREATE TABLE off_origin (" >> $_sql_file
    cat $_col_file | awk '{print $1" TEXT NULL,"}' | tr '\n' ' ' | sed 's/,.$//' >> $_sql_file
    echo ");" >> $_sql_file
    # We import the csv file into the table
    echo "\copy off_origin FROM '$_csv_file' WITH (FORMAT csv, DELIMITER E'\t', HEADER true);" >> $_sql_file

    echo "Stats (origin)..."
    # For each column, we select all distinct values and count their occurences and store it in a new table called off_stats_count_colname and export the stats table to a csv file
    cat $_col_file | awk '{print "DROP TABLE IF EXISTS off_origin_stats_count_"$1";CREATE TABLE off_origin_stats_count_"$1" AS SELECT "$1", COUNT(*) FROM off_origin GROUP BY "$1";"}' >> $_sql_file
    cat $_col_file | awk '{print "\\copy off_origin_stats_count_"$1" TO off_origin_stats_count_"$1".csv WITH (FORMAT csv, HEADER true);"}' >> $_sql_file

    echo "Splitting..."
    # Add a new column id and split each column into a new table off_alpha_colname with columns id and colname
    echo "DROP TABLE IF EXISTS off_alpha;CREATE TABLE off_alpha AS SELECT ROW_NUMBER() OVER(ORDER BY code) AS id, * FROM off_origin;" >> $_sql_file
    cat $_col_file | awk '{print "DROP TABLE IF EXISTS off_alpha_"$1";CREATE TABLE off_alpha_"$1" AS SELECT id, "$1" FROM off_alpha;"}' >> $_sql_file

    echo "Unlisting..."
    _to_unlist=$(echo {brands,cities,data_quality_errors,ingredients{,_analysis},manufacturing_places,nutrient_levels,popularity}_tags \
                      {categories,countries,labels,origins,states,traces}{,_tags,_$_lang} emb_codes{,_tags} \
                      {additives,food_groups}{_tags,_$_lang} packaging{,_tags,_$_lang,_text} allergens purchase_places stores)
    echo $_to_unlist | tr ' ' '\n' | while read _c
    do
        echo "DROP TABLE IF EXISTS off_alpha_split_$_c;CREATE TABLE off_alpha_split_$_c AS SELECT id, UNNEST(STRING_TO_ARRAY($_c, ',')) AS $_c FROM off_alpha_$_c;" >> $_sql_file
        echo "DROP TABLE IF EXISTS off_alpha_$_c;CREATE TABLE off_alpha_$_c AS SELECT * FROM off_alpha_split_$_c GROUP BY id, $_c;" >> $_sql_file
    done

    echo "Nullifying..."
    # For each off_alpha_colname, update colname to an empty string if it's only composed by ponctuation
    # and change empty strings to 'unknown' if it's a value from a table of the list _change_to_unknown
    # or to 'null' if it's a value from a table of the list _change_to_null
    _change_to_unknown=$(echo pnns_groups_{1,2} {nutriscore,environmental_score}_grade)
    _change_to_null=$(echo no_nutrition_data)
    cat $_col_file | while read _c
    do
        # Set the column to an empty string if it's a string containing only spaces or ponctuation
        echo "UPDATE off_alpha_$_c SET $_c = TRIM(' -;\*\?.\(\)~\[\]\{\}' FROM $_c);" >> $_sql_file
        if [[ $_change_to_unknown == *"$_c"* ]]
        then
            echo "UPDATE off_alpha_$_c SET $_c = 'unknown' WHERE $_c = '';" >> $_sql_file
        elif [[ $_change_to_null == *"$_c"* ]]
        then
            echo "UPDATE off_alpha_$_c SET $_c = 'null' WHERE $_c = '';" >> $_sql_file
        fi
    done

    echo "Stats (alpha)..."
    # For each off_alpha_colname, we count the occurences of each value and store it in a new table off_alpha_stats_count_colname
    cat $_col_file | awk '{print "DROP TABLE IF EXISTS off_alpha_stats_count_"$1";CREATE TABLE off_alpha_stats_count_"$1" AS SELECT "$1", COUNT(*) FROM off_alpha_"$1" GROUP BY "$1";"}' >> $_sql_file
    cat $_col_file | awk '{print "\\copy off_alpha_stats_count_"$1" TO off_alpha_stats_count_"$1".csv WITH (FORMAT csv, HEADER true);"}' >> $_sql_file
    # For each off_alpha_colname, we count the number of rows with the same id and store it in a new table off_alpha_stats_multiple_colname
    cat $_col_file | awk '{print "DROP TABLE IF EXISTS off_alpha_stats_multiple_"$1";CREATE TABLE off_alpha_stats_multiple_"$1" AS SELECT id, COUNT(*) FROM off_alpha_"$1" GROUP BY id HAVING COUNT(*) > 1;"}' >> $_sql_file
    cat $_col_file | awk '{print "\\copy off_alpha_stats_multiple_"$1" TO off_alpha_stats_multiple_"$1".csv WITH (FORMAT csv, HEADER true);"}' >> $_sql_file

    echo "Recomposing..."
    # For each line of the schema file, we create a new table off_beta_tablename with all columns from the schema file
    # and we join all tables from the schema file using the id column
    cat $_schema_file | while read _line
    do
        _table=$(echo $_line | awk '{print $1}')
        _columns=$(echo $_line | awk '{for (i=2; i<=NF; i++) print $i}')
        echo "DROP TABLE IF EXISTS off_beta_$_table;CREATE TABLE off_beta_$_table AS SELECT * " >> $_sql_file
        _a=0
        for _c in $_columns
        do
            _a=$((_a+1))
            if [ $_a -eq 1 ]
            then
                echo "FROM off_alpha_$_c " >> $_sql_file
            else
                echo "JOIN off_alpha_$_c USING (id) " >> $_sql_file
            fi
        done
        echo "ORDER BY id ;" >> $_sql_file
    done

    echo "Stats (beta)..."
    # For each line of the schema file, we count the occurences of each value for each column and store it in a new table off_beta_stats_count_tablename_colname
    cat $_schema_file | while read _line
    do
        _table=$(echo $_line | awk '{print $1}')
        _columns=$(echo $_line | awk '{for (i=2; i<=NF; i++) print $i}')
        echo "DROP TABLE IF EXISTS off_beta_stats_count_$_table;CREATE TABLE off_beta_stats_count_$_table AS SELECT * FROM (SELECT id, COUNT(id) AS count FROM off_beta_$_table GROUP BY id ) WHERE count > 1 ; " >> $_sql_file
        echo "\\copy off_beta_stats_count_$_table TO off_beta_stats_count_$_table.csv WITH (FORMAT csv, HEADER true);" >> $_sql_file
        echo "\\copy off_beta_$_table TO off_beta_$_table.csv WITH (FORMAT csv, HEADER true);" >> $_sql_file
    done

    echo "Processing..."
    export PGPASSWORD='gb232322' && \
    psql -h kafka -p 5432 -U gb232322 gb232322 < $_sql_file && \
    echo "Done."
}

mkdir -p data && cd data

echo "_lang=$_lang" > config.txt

_download
_import

mkdir -p origin alpha beta
mv off_origin_* origin
mv off_alpha* alpha
mv off_beta* beta

cd -
