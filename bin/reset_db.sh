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

echo "Creating schema from ${DIR}/../example/database/example.sql"
PGPASSWORD=proto_crudl psql -h localhost -p 5432 -U proto_crudl -d proto_crudl -a -f "${DIR}"/../example/database/example.sql

