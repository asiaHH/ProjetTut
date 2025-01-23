DROP TABLE IF EXISTS off_origin;

CREATE TABLE off_origin(
);

COPY off_origin FROM 'reeady.csv' WITH (FORMAT csv);

