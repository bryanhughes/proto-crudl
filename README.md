# proto-crudl
------
An escript that generates protobuffers and Erlang CRUDL (Create, Read, Update, Delete, List/Lookup) based on your
relational data model. The tool will generate code that supports either records or maps. The tool supports the 
Erlang [gpb](https://github.com/tomas-abrahamsson/gpb) library, though not required. Currently, it only supports Postgres. 

Aside from simple CRUDL, this tool allows you to generate functions based on any standard SQL query, lookups, and
transformations. The custom mappings and transformations are very powerful. 

Please look at the `example` directory to an example schema and configuration file to generate not just simple
CRUD, but search/lookup operations based on indexed fields, as well as custom mapping queries including how to use
transformations to work with PostGis where you want to tranform lat/lon to geography columns. 

# Installing Erlang
Make sure you have a minimum of Erlang/OTP 23 installed. Currently, this has only been tested on MacOS.

## Mac OS X
Using Homebrew:

    brew install erlang

Using MacPorts:

    sudo port install erlang

## Linux
Most operating systems have pre-built Erlang distributions in their package management systems.
For Ubuntu/Debian:

    sudo apt-get update
    sudo apt-get install erlang

For Fedora:

    sudo yum install erlang

For FreeBSD

    sudo pkg update
    sudo pkg install erlang

##  Windows

[Download the Window installer](https://www.erlang.org/downloads)

<hr>

# Download and install Rebar3
Please follow the [getting started instruction](https://rebar3.readme.io/docs/getting-started)

<hr>

# Install Docker
If you are setting up docker for the first time on Ubuntu using snap, the follow these instructions. Due to the 
confinement issues on snappy, it requires some manual setup to make docker-snap works on your machine.

## Mac OS X
Using Homebrew:

    brew install --cask docker
    open /Applications/Docker.app

## Ubuntu
On Ubuntu classic, before installing the docker snap,
please run the following command to add the login user into docker group.

    sudo addgroup --system docker
    sudo adduser $USER docker
    newgrp docker

Once installed, you can then manage the services

    sudo snap services
    
To start the docker service

    sudo snap start docker
    
## Setting Up Database For Building The Examples
First, start the docker image in the examples subdirectory and then create the `proto_crudl` Role and Database with 
create database and superuser privileges and password `proto_crudl`.

    cd example
    docker-compose up
 
Now log in as the user locally to postgres running in the container

    psql -h localhost -p 5432 -U proto_crudl

or you can try...

    docker run -it --link some-postgis:postgres --rm postgres sh -c 'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U proto_crudl'


## Resetting the Example Database
This helper script will allow you to rapidly drop and recreate your database. This expects the role and database `proto_crudl`

    bin/reset_db.sh

<hr>

Build
-----

    rebar3 escriptize

Run
---

    cd example
    bin/generate.sh

or directly

    _build/default/bin/proto_crudl <config>

Test
----
Several of the eunit tests rely on the presence of the example database. Currently unit and functional tests are commingled.

    rebar3 eunit


## Building the Example
The project includes an example schema and scripts to build located in the `example` directory. You
will find the build scripts in `example/bin` to create or reset the example database as well as to generate
the code in the example schema, which is located in `example/database`. The schema was generated using [DbSchema](https://www.dbschema.com/).

**PLEASE NOTE:** You will see errors and warnings in the output. This is intentional as the example schema includes a
lot of corner cases, like a table without a primary key and unsupported postgres types.

### Testing the Generated Code    
The example code also contains some tests that tests the results of the generated code. To run these tests They 
are located in `example/apps/test/example_test.erl` and do full CRUDL test.

    cd example
    rebar3 eunit


Please refer to these test to better understand how to use the generated code.
    
# Using proto_crudl 
Currently, I have not had time to write a github pull script for the `escript` executable 
artifact: `_build/default/bin/proto_crudl`. At this time I manually copy the file to my project. So not build friendly
just yet.

## proto_crudl.config
You will want to look at [example/config/proto_crudl.config](example/config/proto_crudl.config) as a guide
for your own config. It gives a complete example with inline documentation of the current functionality of the tool.

Almost all the configs follow the pattern of a list of tuples where the first element in the tuple is the table, and
the second element is then a list. 

The more complex feature of `proto_crudl` is the ability to apply transformations or sql functions. If you are using PostGIS,
or need to apply other functions, you will need to use this feature.

```
{transforms, [
    {"test_schema.user", [
        {insert, [{"geog", "ST_POINT($lon, $lat)::geography"}]},
        {update, [{"geog", "ST_POINT($lon, $lat)::geography"}]},
        {select, [{"lat", "ST_Y(geog::geometry)"},
                  {"lon", "ST_X(geog::geometry)"}]}]},
    {"public.foo", [
        {select, [{"foobar", "1"}]}]}
]}.
```
Following the same pattern of a list of tables with a list of tuples. In the case of converting a `lat` and `lon` to a 
`geography`, you must define each of the operations insert/create, update, and select/read on how the column values will
be handled to and from the database. The result is that the columns `lat` and `lon` will be generated as `virtual` 
columns in the mapping. Note that when referencing them in the function body (the second element of the tuple), you will
need to prepend them with the `$` so that `proto_crudl` knows they are the virtual columns being operated on. 
For the `insert` operation, a single tuple is defined which will
result in the extension function `ST_POINT($lat, $lon)::geography` to be applied to the bind values of the `INSERT` 
statement. Resulting in the following code:

```erlang
-define(INSERT, "INSERT INTO test_schema.user (first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, version, geog) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, 0, ST_POINT($14, $13)::geography) RETURNING user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, version, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon").
```
and
```
create(M = #{lon := Lon, lat := Lat, due_date := DueDate, updated_on := UpdatedOn, created_on := CreatedOn, number_value := NumberValue, user_type := UserType, my_array := MyArray, aka_id := AkaId, enabled := Enabled, user_token := UserToken, email := Email, last_name := LastName, first_name := FirstName}) when is_map(M) ->
    Params = [FirstName, LastName, Email, UserToken, Enabled, AkaId, MyArray, user_type_value(UserType), NumberValue, ts_decode_map(CreatedOn), ts_decode_map(UpdatedOn), DueDate, Lat, Lon],
    case pgo:query(?INSERT, Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/2}]}) of
        #{command := insert, num_rows := 0} ->
            {error, failed_to_insert};
        #{command := insert, num_rows := 1, rows := [Row]} ->
            {ok, Row};
        {error, {pgsql_error, #{code := <<"23505">>}}} ->
             {error, exists};
        {error, Reason} ->
            {error, Reason}
    end;
create(M = #{lon := Lon, lat := Lat, due_date := DueDate, updated_on := UpdatedOn, created_on := CreatedOn, number_value := NumberValue, user_type := UserType, my_array := MyArray, aka_id := AkaId, email := Email, last_name := LastName, first_name := FirstName}) when is_map(M) ->
    Params = [FirstName, LastName, Email, AkaId, MyArray, user_type_value(UserType), NumberValue, ts_decode_map(CreatedOn), ts_decode_map(UpdatedOn), DueDate, Lat, Lon],
    case pgo:query(?INSERT_DEFAULTS, Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/2}]}) of
        #{command := insert, num_rows := 0} ->
            {error, failed_to_insert};
        #{command := insert, num_rows := 1, rows := [Row]} ->
            {ok, Row};
        {error, {pgsql_error, #{code := <<"23505">>}}} ->
             {error, exists};
        {error, Reason} ->
            {error, Reason}
    end;
create(_M) ->
    {error, invalid_map}.
```

I would recommend that you build the example project and then review the generated code for `test_schema_user_db.erl` to get a better
understanding.

## The special version column
The proto_crudl framework implements all the necessary code to support handling stale changes to a record with a version column. 

    {options, [{version_column, "version"}, indexed_lookups, check_constraints_as_enums]},

In this example, the column is called `version`. When a table has this column, proto_crudl will generate code that will 
automatically handle updating the column value on a good update, as well as guard against updating the record from a 
stale update. If the current value of `version` is `100` and you attempt to update using a map or record that has the version value 
of 90, the update will return with `notfound`, otherwise update returns `{ok, Map}` or `{ok, Record}` (depending on how you generated your code).

If the column is not present, the tool will automatically inject the column into the table by performing an `ALERT TABLE ...`

In our example database, the `user` table has a column named `version`. 
```
-define(UPDATE, "UPDATE test_schema.user SET first_name = $2, last_name = $3, email = $4, user_token = $5, enabled = $6, aka_id = $7, my_array = $8, user_type = $9, number_value = $10, created_on = $11, updated_on = $12, due_date = $13, version = version + 1, geog = ST_POINT($16, $15)::geography WHERE user_id = $1 AND version = $14 RETURNING user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, version, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon").
```

This results in the following code generation and logic for UPDATES. Please note that the INSERT sql and code is also different.
```
update(M = #{lon := Lon, lat := Lat, version := Version, due_date := DueDate, updated_on := UpdatedOn, created_on := CreatedOn, number_value := NumberValue, user_type := UserType, my_array := MyArray, aka_id := AkaId, enabled := Enabled, user_token := UserToken, email := Email, last_name := LastName, first_name := FirstName, user_id := UserId}) when is_map(M) ->
    Params = [UserId, FirstName, LastName, Email, UserToken, Enabled, AkaId, MyArray, user_type_value(UserType), NumberValue, ts_decode_map(CreatedOn), ts_decode_map(UpdatedOn), DueDate, Version, Lat, Lon],
    case pgo:query(?UPDATE, Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/2}]}) of
        #{command := update, num_rows := 0} ->
            notfound;
        #{command := update, num_rows := 1, rows := [Row]} ->
            {ok, Row};
        {error, Reason} ->
            {error, Reason}
    end;
update(_M) ->
    {error, invalid_map}.
```

## erleans/pgo

Finally, `proto_crudl` uses [erleans/pgo](https://github.com/erleans/pgo) for its Postgres connectivity (which is currently)
the only supported database. If you want the pgo client to return UUID as binary strings, set an application environment
variable or in your sys.config:

    {pg_types, [{uuid_format, string}]},

    {pgo, [{pools, [{default, #{pool_size => 10,
                                host => "127.0.0.1",
                                database => "proto_crudl",
                                user => "proto_crudl",
                                password => "proto_crudl"}}]}]}

Size the pool according to your requirements.    
    
## Generating Protobuffers

proto_crudl will generate a `.proto` that maps to each table in the schema (unless explicitly excluded). 

The package for each proto will be the schema that the table is located in. The `.proto` files will be generated in 
the `output` directory specified in the config file with those proto to table mappings being written to a subdirectory 
that corresponds to the schema the table is in.  

Relational databases are inherently namespaced by schema. This means that when supporting multiple schemas, and the fact that Erlang has
no concept of namespaces, the module name will be the name of the table prepended by the schema name (if set in the config).
Note that protobuffers are correctly generated where the schema maps to a package.

It is recommended that you download and install the protocol buffer compiler, which is necessary to compile to other
languages such as Java, Objective-C, or even Go. If you are new to protocol buffers, start
[by reading the developer docs](https://developers.google.com/protocol-buffers/).

It is important to note that the default configuration is to use `gpb` to compile and support protobuffers in Erlang.

PLEASE NOTE: There is a bug that has been filed in `gpb` where protos of the same name in different packages will get 
overwritten since there is no namespacing.

# Using In Your Project
You will need to copy the `proto_crudl` escript to your project.

Next, create your `proto_crudl.config`. You can simply copy and modify the one in the repo. Currently, I have a manual
generate script that I locate in the top level `bin` directory:

```
#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

"$DIR"/proto_crudl "$DIR"/../config/proto_crudl.config
```

## Using the Maps or Records

`proto_crutl` supports code generation using both maps and records. It is important to note that there are several issues
with using maps with `gpb` and `pgo` that do align very well when using together. 

First `pgo` favors maps and uses the atom (as expected) `null` for NULL values. Unfortunately, here `gpb` seems to 
favor records when serializing and deserializing, this is likely a bug. For now, `proto_crudl` generates code based on
each library implementations support for maps and records. When using the `maps` config option with `gpb`, fields that are
explicitly `undefined` are not handled properly and cause an exception as the library attempts to serialize them. The
`gpb` generated code for records does not have this issue. As expected, `undefined` is treated as missing.

Next, `pgo` does not handle query parameters that have `undefined` present. While this is strictly correct, it breaks
when trying to map between the code generated by `gpo` and query parameters expected by `pgo`. This project includes a
forked version of `pgo` that is more lenient and allows `undefined` to also mean `null`. This has not been committed 
back to the original project yet.

Because of the subtle misalignment between libraries, using maps requires the use of `to_proto/1` and `from_proto/1` 
to correctly convert the serialized protobuffers as maps as input values to queries. It also means that the developer
has to handle `null` values from the `pgo` and ultimately `proto_crutl` and remember to convert to `undefined` when
serializing. Maps are very flexible, but also very problematic.
