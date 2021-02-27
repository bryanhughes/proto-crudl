#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# docker exec -it example-db <command>
# docker run -it --link some-postgis:postgres --rm postgres sh -c 'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U erl_crudl'

echo "Dropping Postgres database erl_crudl..."
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "DROP DATABASE erl_crudl"

echo "Recreating Postgres database erl_crudl..."
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "CREATE DATABASE erl_crudl WITH OWNER = erl_crudl ENCODING = 'UTF8' TEMPLATE = template0 CONNECTION LIMIT = -1;"

echo "Granting privileges..."
PGPASSWORD=erl_crudl psql -h localhost -p 5432 -U erl_crudl << EOF
    GRANT ALL PRIVILEGES ON DATABASE erl_crudl TO erl_crudl;
    CREATE EXTENSION if not exists "postgis";
    CREATE EXTENSION if not exists "uuid-ossp";
EOF

echo "Creating schema from ${DIR}/../sql/example.sql"
PGPASSWORD=erl_crudl psql -h localhost -p 5432 -U erl_crudl -d erl_crudl -a -f "${DIR}"/../sql/example.sql

#psql -d $2 -a -f $3
#
#if [[ $# -eq 4 ]]; then
#    echo "Loading seed data..."
#    psql -d $2 -a -f $4
#fi
