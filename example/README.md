# erl_crudl Example

This is an example project that will use the `erl_crudl` escript command line tool. This repository
consists of an example schema (generated using [DBSchema](https://dbschema.com/)). The example build
includes a `docker-compose` and scripts to generate the schema and load the seed files.

In a new terminal window, change to the example directory and type:

    docker-compose up

Now, in another terminal window, type the following:

    bin/generate_code.sh

If `erl_crudl` has not yet been built or needs to be rebuilt, the script will detect this and instead
of generating the protobuffers and mapped erlang code, it will build the tool. In this case, you will need
to rerun the script.

To connect to the docker postgres, you have two options. Either execute `psql` from inside, or outside the
container. 

From inside the container:

    bin/docker-psql.sh

From outside the container:

    psql -h localhost -p 5433 -U erl_crudl

The password is `erl_crudl`

To reset the schema in the container:

    bin/reset.db

Good luck!