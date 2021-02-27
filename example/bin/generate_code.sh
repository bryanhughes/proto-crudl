#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [[ -f "$DIR"/../../_build/default/bin/proto_crudl ]]
then
    "$DIR"/../../_build/default/bin/proto_crudl "$DIR"/../config/proto_crudl.config

    mkdir -p "$DIR/../output/java"

    printf "\nCompiling protobufs..."
    for d in "$DIR"/../output/proto/public/*.proto; do
      if [ -f "$d" ]; then
        echo "    $d"
        protoc -I="$DIR"/../output/proto --java_out=output/java "$d"
      fi
    done

    for d in "$DIR"/../output/proto/test_schema/*.proto; do
      if [ -f "$d" ]; then
        echo "    $d"
        protoc -I="$DIR"/../output/proto --java_out=output/java "$d"
      fi
    done

    printf "Switching to %s/.. and running rebar3\n" "$DIR"
    cd "$DIR"/.. || exit
    rebar3 compile
    printf "\nTo test, run 'rebar3 eunit'\n\n"
else
    printf "Running 'rebar3 escriptize' in the proto_crudl lib directory"
    cd "$DIR"/../.. || exit
    rebar3 escriptize
    printf ""
    printf ">> Rerun this script to generate the example code"
fi
