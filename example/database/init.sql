CREATE USER erl_crudl WITH SUPERUSER PASSWORD 'erl_crudl' CREATEDB;
GRANT ALL PRIVILEGES ON DATABASE erl_crudl TO erl_crudl;
CREATE EXTENSION if not exists "uuid-ossp";
CREATE EXTENSION if not exists "postgis";