CREATE USER proto_crudl WITH SUPERUSER PASSWORD 'proto_crudl' CREATEDB;
GRANT ALL PRIVILEGES ON DATABASE proto_crudl TO proto_crudl;
CREATE EXTENSION if not exists "uuid-ossp";
CREATE EXTENSION if not exists "postgis";