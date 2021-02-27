An escript that generates Erlang code that handles Search, Create, Read, Update, and Delete (SCRUD) operations against 
a relational database. This tool can also generate the Protobuf .proto files based on the schema. Please note that the 
tool does not yet support complex many-to-many relationships cleanly. 

Aside from simple CRUD, this tool allows you to generate functions based on any standard SQL query, lookups, and
transformations. The custom mappings and transformations are very powerful.

Please look at the `example` directory to an example schema and configuration file to generate not just simple
CRUD, but search/lookup operations based on indexed fields, as well as custom mapping queries. 

# Installing Erlang
Make sure you have Erlang/OTP 23 installed. 

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
The recommended way to install rebar3 is to simply fetch the escript from S3 and use the `install` feature to unpack 
the escript to `~/.cache/rebar3/bin/rebar3`:

```
wget https://s3.amazonaws.com/rebar3/rebar3
chmod +x rebar3
./rebar3 local upgrade
```

You'll want to then add `~/.cache/rebar3/bin/` to your shell's `$PATH` in `~/.bashrc` or `~/.zshrc`.

When updates to rebar3 are made you can update the install with `rebar3 local upgrade`.

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
First, start the docker image in the examples sub directory and then create the Role and    Database

    cd example
    docker-compose up
 
Now log in as the user locally to postgres running in the container

    psql -h localhost -p 5432 -U proto_crudl

or you can try...

    docker run -it --link some-postgis:postgres --rm postgres sh -c 'exec psql -h "$POSTGRES_PORT_5432_TCP_ADDR" -p "$POSTGRES_PORT_5432_TCP_PORT" -U proto_crudl'


## Resetting the Example Database
This helper script will allow you to rapidly drop and recreate your database

    bin/reset_db.sh proto_crudl proto_crudl example/sql/example_schema.sql example/sql/example_data.sql

<hr>

Build
-----

    rebar3 escriptize

Run
---

    example/bin/generate.sh

or directly

    _build/default/bin/proto_crudl <config>


## Building the Example
The project includes an example schema and scripts to build located in the `example` directory. You
will find the build scripts in `example/bin` to create or reset the example database as well as to generate
the code in the example schema, which is located in `example/sql`. The schema was generated using [DbSchema](https://www.dbschema.com/).

**PLEASE NOTE:** You will see errors and warnings in the output. This is intentional as the example schema includes a
lot of corner cases, like a table without a primary key.

## Running the eunit tests
The eunit tests are inline with the code at the end of the modules. Several of them expect that proto_crudl database
from the example directory to have been built.

    rebar3 eunit
    
You can run the eunit tests again after your generate the example code to then test the generated code `user_db` against the
`user` table to make sure everything works.
    
## Testing the Generated Code    
Please note that there are two eunit tests which will test the generated code. They are located in `proto_crudl` and are 
called `crud_test` and `change_id_test`.
    
Using proto_crudl
---
Include this as a dependency in your `rebar.config`. I would recommend that you copy the script
`generate_code.sh` to your project and modify accordingly. You will need to run this the first time, and
any other time you alter your database schema. 

# proto_crudl.config
You will want to look at [example/config/proto_crudl.config](example/config/proto_crudl.config) as a guide
for your own config. It gives a complete example with inline documentation of the current functionality of the tool.

Almost all of the configs follow the pattern of a list of tuples where the first element in the tuple is the table and
the second element is then a list. For the table `lookup` (aka search), the list is a list of one or more keys/columns. 
For example:

```
{lookup, [
    {"test_schema.address", [
        ["postcode"],
        ["city", "state", "postcode"]
    ]},
    ...
]}.
```
Will generate two `lookup/1` functions on the `product_db` module. It is advised that you have matching indexes on the
table covering the columns. The second config will generate a lookup that covers the three columns so a corresponding 
composite index will be required.

The more complex feature of `proto_crudl` is the ability to apply transformations or sql functions. If you are using PostGIS,
or need to apply other functions, you will need to use this feature.

```
{transforms, [
    {"test_schema.user", [
        {insert, [{"geog", "ST_POINT($lat, $lon)::geography"}]},
        {update, [{"geog", "ST_POINT($lat, $lon)::geography"}]},
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
-define(INSERT, "INSERT INTO test_schema.user (first_name, last_name, email, user_token, enabled, change_id, geog) VALUES ($1, $2, $3, $4, $5, 0, ST_POINT($6, $7)::geography) RETURNING user_id").
```
and
```
create(M = #{first_name := FirstName, last_name := LastName, email := Email, user_token := UserToken, enabled := Enabled, lat := Lat, lon := Long}) when is_map(M) ->
    Params = [FirstName, LastName, Email, UserToken, Enabled, Lat, Long],
    case pgo:query(?INSERT, Params) of
        #{command := insert, num_rows := 1, rows := [{UserId}]} ->
            {ok, M#{user_id => UserId, change_id => 0}};
        {error, Reason} ->
            {error, Reason}
    end;
create(_M) ->
    {error, invalid_map}.
```

I would recommend that you build the example project and then review the generated code for `user_db.erl` to get a better
understanding.

## The special change_id column
The proto_crudl framework implements all the necessary code to support tracking changes to a table that has a column called 
`change_id`. When a table has this column, proto_crudl will generate code that will automatically handle updating the 
column value on a good update, as well as guard against updating the table from a stale map. If the current value of
`change_id` is `100` and you attempt to update using a map that has the change_id value of 90, the update will return
with `not_found`, otherwise update returns `{ok, Map}`.

In our example database, the `user` table has a column named `change_id`. 
```
-define(UPDATE, "UPDATE test_schema.user SET first_name=$2, last_name=$3, email=$4, user_token=$5, enabled=$6, aka_id=$7, change_id=change_id + 1, geog=ST_POINT($9, $10)::geography WHERE user_id=$1 AND change_id<=$8 RETURNING user_id, first_name, last_name, email, user_token, enabled, aka_id, change_id, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon").
```

This results in the following code generation and logic for UPDATES. Please note that the INSERT sql and code is also different.
```
update(M = #{user_id := UserId, first_name := FirstName, last_name := LastName, email := Email, user_token := UserToken, enabled := Enabled, aka_id := AkaId, change_id := ChangeId, lat := Lat, lon := Lon}) when is_map(M) ->
    Params = [UserId, FirstName, LastName, Email, UserToken, Enabled, AkaId, ChangeId, Lat, Lon],
    case pgo:query(?UPDATE, Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}]}) of
        #{command := update, num_rows := 0} ->
            not_found;
        #{command := update, num_rows := 1, rows := [Row]} ->
            {ok, Row};
        {error, Reason} ->
            {error, Reason}
    end;
update(_M) ->
    {error, invalid_map}.
```

# erleans/pgo

Finally, `proto_crudl` uses [erleans/pgo](https://github.com/erleans/pgo) for its Postgres connectivity (which is currently)
the only supported database. If you want the pgo client to return UUID as binary strings, set an application environment
variable or in your sys.config:

    {pg_types, {uuid, string}},

    {pgo, [{pools, [{default, #{pool_size => 10,
                                host => "127.0.0.1",
                                database => "proto_crudl",
                                user => "proto_crudl",
                                password => "proto_crudl"}}]}]}

Size the pool according to your requirements.    
    
Generating Protobuffers
---
proto_crudl will generate protobuffers that map to the relational schema including foreign key relationships. The tool
correctly handles relationships across schemas. 

*TODO:*
While the tool will handle many-to-many relationships through the 
join table, it does not yet add the relating message in the referencing message. For example if the join table is many
customers with many addresses, the tool does not yet pull the many addresses into the customer message as a repeating
Address message.  

The package for each proto will be the schema that the table is located in. The `.proto` files will be generated in 
the `output` directory specified in the config file with those proto to table mappings being written to a subdirectory 
that corresponds to the schema the table is in.  

The tool will correctly generate proto files with foreign key relationships that cross schemas. It will also handle
naming collisions due to tables with the same names in different schemas by appending a number that increments starting
at 1. A namespace of schema_table was considered, but rejected for the time being due to the very lon field names that
can be generated.

It is recommended that you download and install the protocol buffer compiler. If you are new to protocol buffers, start
[by reading the developer docs](https://developers.google.com/protocol-buffers/).

Using In Your Project
---
You will need to include the deps in your `rebar.config`:

    {proto_crudl, {git, "https://github.com/bryanhughes/proto_crudl.git", {branch, "master"}}},

Next, create your `proto_crudl.config`. You can simply copy and modify the one in the repo. Also, copy and rename
the `example/bin/build_example.sh` and move it into your `bin` or other directory. Run it once to generate the 
code and protos, and any other time you modify your schema.