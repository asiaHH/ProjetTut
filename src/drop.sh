#!/usr/bin/env bash

# Drop all tables off_* in the database
export PGPASSWORD='gb232322' && \
psql -h kafka -p 5432 -U gb232322 gb232322 -c "SELECT 'DROP TABLE IF EXISTS ' || tablename || ';' FROM pg_tables WHERE tablename LIKE 'off_%';" | psql -h kafka -p 5432 -U gb232322 gb232322
