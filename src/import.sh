#!/usr/bin/env bash

_lang="fr"
_off_csv_file="$_lang.openfoodfacts.org.products.csv"
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
        echo "Done."
    fi
}

function _send_sql() {
    _file=$1
    if [ -f $_file ]
    then
        echo "Processing $_file..."
        export PGPASSWORD='gb232322' && \
        psql -h kafka -p 5432 -U gb232322 gb232322 < $_file && \
        echo "$_file processed."
    fi
}

function _import() {
    if [ -f $_col_file ]
    then
        _sql_file="import.sql"
        echo "Importing..."
        echo "" > $_sql_file
        # We create a table with the same columns as the csv file
        echo -n "DROP TABLE IF EXISTS off_o;CREATE TABLE off_o (" >> $_sql_file
        cat $_col_file | awk '{print $1" TEXT,"}' | tr '\n' ' ' | sed 's/,.$//' >> $_sql_file
        echo ");" >> $_sql_file
        # We import the csv file into the table
        echo "\copy off_o FROM '$_csv_file' WITH (FORMAT csv, DELIMITER E'\t', HEADER true);" >> $_sql_file
        echo "DROP TABLE IF EXISTS off_oa;CREATE TABLE off_oa AS SELECT ROW_NUMBER() OVER(ORDER BY code) AS id, * FROM off_o;" >> $_sql_file
        _send_sql $_sql_file
    fi
}

function _transform() {
    if [ -f $_schema_file ]
    then
        _b=0
        echo "Transforming..."
        # We create a table off_ with the same columns as the schema file
        cat $_schema_file | while read _line
        do
            _table=$(echo $_line | awk '{print $1}')
            _columns=$(echo $_line | awk '{for (i=2; i<=NF; i++) print $i}')
            _to_unlist=$(echo {brands,cities,data_quality_errors,ingredients{,_analysis},manufacturing_places,nutrient_levels,popularity}_tags \
                        {categories,countries,labels,origins,states,traces}{,_tags,_$_lang} emb_codes{,_tags} \
                        {additives,food_groups}{_tags,_$_lang} packaging{,_tags,_$_lang,_text} allergens purchase_places stores)
            _change_to_unknown=$(echo pnns_groups_{1,2} {nutriscore,environmental_score}_grade)
            _change_to_null=$(echo no_nutrition_data)
            _sql_file="transform_$_table.sql"
            echo "" > $_sql_file
            for _c in $_columns
            do
                # Splitting...
                echo "DROP TABLE IF EXISTS off_oa_$_c;CREATE TABLE off_oa_$_c AS SELECT id, $_c FROM off_oa;" >> $_sql_file
                # Unlisting...
                if [[ $_to_unlist == *"$_c"* ]]
                then
                    echo "DROP TABLE IF EXISTS off_oas_$_c;CREATE TABLE off_oas_$_c AS SELECT id, UNNEST(STRING_TO_ARRAY($_c, ',')) AS $_c FROM off_oa_$_c;" >> $_sql_file
                    echo "DROP TABLE IF EXISTS off_oa_$_c;CREATE TABLE off_oa_$_c AS SELECT * FROM off_oas_$_c GROUP BY id, $_c;" >> $_sql_file
                fi
                # Nullifying...
                echo "UPDATE off_oa_$_c SET $_c = TRIM(' -;\*\?.\(\)~\[\]\{\}' FROM $_c);" >> $_sql_file
                if [[ $_change_to_unknown == *"$_c"* ]]
                then
                    echo "UPDATE off_oa_$_c SET $_c = 'unknown' WHERE $_c = '';" >> $_sql_file
                elif [[ $_change_to_null == *"$_c"* ]]
                then
                    echo "UPDATE off_oa_$_c SET $_c = 'null' WHERE $_c = '';" >> $_sql_file
                fi
            done
            echo "DROP TABLE IF EXISTS off_$_table;CREATE TABLE off_$_table AS SELECT * " >> $_sql_file
            _a=0
            for _c in $_columns
            do
                _a=$((_a+1))
                if [ $_a -eq 1 ]
                then
                    echo "FROM off_oa_$_c " >> $_sql_file
                else
                    echo "JOIN off_oa_$_c USING (id) " >> $_sql_file
                fi
            done
            echo "ORDER BY id ;" >> $_sql_file
            echo "\copy off_$_table TO 'off_$_table.csv' WITH (FORMAT csv, HEADER true);" >> $_sql_file
            _b=$((_b+1))
            if [ $_b -eq 19 ]
            then
                _send_sql $_sql_file
            else
                _send_sql $_sql_file &
            fi
        done
    fi
}

mkdir -p data && cd data

echo "_lang=$_lang" > config.txt

_download
_import
_transform

mkdir -p export 
mv off_*.csv export

cd -
