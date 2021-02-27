#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# docker exec -it example-db <command>
# docker run -it --link some-postgis:postgres --rm postgres sh -c 'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U proto_crudl'

echo "Dropping Postgres database proto_crudl..."
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "DROP DATABASE proto_crudl"

echo "Recreating Postgres database proto_crudl..."
PGPASSWORD=postgres psql -h localhost -p 5432 -U postgres -c "CREATE DATABASE proto_crudl WITH OWNER = proto_crudl ENCODING = 'UTF8' TEMPLATE = template0 CONNECTION LIMIT = -1;"

echo "Granting privileges..."
PGPASSWORD=proto_crudl psql -h localhost -p 5432 -U proto_crudl << EOF
    GRANT ALL PRIVILEGES ON DATABASE proto_crudl TO proto_crudl;
    CREATE EXTENSION if not exists "postgis";
    CREATE EXTENSION if not exists "uuid-ossp";
EOF

echo "Creating schema from ${DIR}/../sql/example.sql"
PGPASSWORD=proto_crudl psql -h localhost -p 5432 -U proto_crudl -d proto_crudl -a -f "${DIR}"/../sql/example.sql

#psql -d $2 -a -f $3
#
#if [[ $# -eq 4 ]]; then
#    echo "Loading seed data..."
#    psql -d $2 -a -f $4
#fi
