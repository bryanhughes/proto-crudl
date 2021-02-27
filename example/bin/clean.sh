#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

cd "$DIR"/../ || exit

rebar3 clean
rebar3 as test clean
rm -rf apps/example/proto/*
rm apps/example/src/*_pb.erl &> /dev/null
rm apps/example/src/*_db.erl &> /dev/null
rm apps/example/include/*_pb.hrl &> /dev/null
