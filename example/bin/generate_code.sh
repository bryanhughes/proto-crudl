#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

if [[ -f "$DIR"/../../_build/default/bin/erl_crudl ]]
then
    "$DIR"/../../_build/default/bin/erl_crudl "$DIR"/../config/erl_crudl.config

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

    echo "Switching to $DIR/.. and running rebar3"
    cd "$DIR"/.. || exit
    rebar3 compile
else
    echo "Running 'rebar3 escriptize' in the erl_crudl lib directory"
    cd "$DIR"/../.. || exit
    rebar3 escriptize
    echo ""
    echo ">> Rerun this script to generate the example code"
fi
