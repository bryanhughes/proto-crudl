%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Feb 2021 7:47 AM
%%%-------------------------------------------------------------------
-module(proto_crudl_maps).
-author("bryan").

%% API
-export([generate_functions/3]).

-include("proto_crudl.hrl").

-spec generate_functions(postgres | any(), string(), #table{}) -> ok.
generate_functions(postgres, FullPath, Table) ->
    ok = file:write_file(FullPath, to_from_proto(), [append]),
    ok = file:write_file(FullPath, ts_support(orddict:to_list(Table#table.columns)), [append]),
    ok = file:write_file(FullPath, empty_map(Table), [append]),
    ok = file:write_file(FullPath, row_decoder(Table), [append]),
    case Table#table.pkey_list of
        [] ->
            ok;
        _ ->
            ok = file:write_file(FullPath, proto_crudl_psql:limit_fun(undefined), [append])
    end,
    ok = file:write_file(FullPath, proto_crudl_psql:create_fun(undefined, Table), [append]),
    case Table#table.pkey_list of
        [] ->
            ok;
        _ ->
            ok = file:write_file(FullPath, proto_crudl_psql:upsert_fun(undefined, Table), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:read_fun(undefined, Table), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:update_fun(undefined, Table), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:delete_fun(undefined, Table), [append])
    end,
    ok = file:write_file(FullPath, proto_crudl_psql:list_lookup_fun(undefined, Table), [append]),
    ok = file:write_file(FullPath, proto_crudl_psql:mappings_fun(undefined, Table), [append]);
generate_functions(Provider, _FullPath, _Table) ->
    io:format("ERROR: Provider ~p is not supported yet.~n", [Provider]),
    erlang:error(provider_not_supported).

row_decoder(#table{columns = ColDict}) ->
    Columns = orddict:to_list(ColDict),
    "decode_row(Row = #{" ++ lists:join(",", build_row_args(Columns, [])) ++ "}, _Fields, _Params) ->\n" ++
    "    Row#{" ++ lists:join(",", build_row_assigns(Columns, [])) ++ "};\n" ++
    "decode_row(Row, _Fields, _Params) ->\n" ++
    "    Row.\n\n";
row_decoder(_Table) ->
    "decode_row(Row, _Fields, _Params) ->\n"
    "    Row.\n\n".

build_row_args([], Acc) ->
    Acc;
build_row_args([{_Key, #column{name = N, valid_values = V}} | Rest], Acc) when length(V) > 0 ->
    build_row_args(Rest, [proto_crudl_utils:to_list(N) ++ " := " ++ proto_crudl_utils:camel_case(N) | Acc]);
build_row_args([{_Key, #column{name = N, udt_name = <<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>}} | Rest], Acc) ->
    build_row_args(Rest, [proto_crudl_utils:to_list(N) ++ " := " ++ proto_crudl_utils:camel_case(N) | Acc]);
build_row_args([{_Key, #column{name = N, udt_name = <<"date">>}} | Rest], Acc) ->
    build_row_args(Rest, [proto_crudl_utils:to_list(N) ++ " := " ++ proto_crudl_utils:camel_case(N) | Acc]);
build_row_args([_Head | Rest], Acc) ->
    build_row_args(Rest, Acc).

build_row_assigns([], Acc) ->
    Acc;
build_row_assigns([{_Key, #column{name = N, valid_values = V}} | Rest], Acc) when length(V) > 0 ->
    Name = proto_crudl_utils:to_list(N),
    build_row_assigns(Rest, [Name ++ " => " ++ Name ++ "_enum(" ++ proto_crudl_utils:camel_case(N) ++ ")" | Acc]);
build_row_assigns([{_Key, #column{name = N, udt_name = <<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>}} | Rest], Acc) ->
    Name = proto_crudl_utils:to_list(N),
    build_row_assigns(Rest, [Name ++ " => " ++ "ts_encode(" ++ proto_crudl_utils:camel_case(N) ++ ")" | Acc]);
build_row_assigns([{_Key, #column{name = N, udt_name = <<"date">>}} | Rest], Acc) ->
    Name = proto_crudl_utils:to_list(N),
    build_row_assigns(Rest, [Name ++ " => " ++ "date_encode(" ++ proto_crudl_utils:camel_case(N) ++ ")" | Acc]);
build_row_assigns([_Head | Rest], Acc) ->
    build_row_assigns(Rest, Acc).


-spec ts_support([{binary(), #column{}}]) -> string().
ts_support([]) ->
    "";
ts_support([{_Key, #column{udt_name = UN}} | Rest]) ->
    case string:prefix(UN, <<"timestamp">>) of
        nomatch ->
            ts_support(Rest);
        _ ->
            "ts_encode(Datetime={{_, _, _}, {_, _, Seconds}}) when is_integer(Seconds) ->\n"
            "    Secs = calendar:datetime_to_gregorian_seconds(Datetime) - 62167219200,\n"
            "    #{seconds => Secs, nanos => 0};\n"
            "ts_encode({{Year, Month, Day}, {Hours, Minutes, Seconds}}) when is_float(Seconds)->\n"
            "    IntegerSeconds = trunc(Seconds),\n"
            "    US = trunc((Seconds - IntegerSeconds) * 1000000),\n"
            "    Secs = calendar:datetime_to_gregorian_seconds({{Year, Month, Day},\n"
            "                                                   {Hours, Minutes, IntegerSeconds}}) - 62167219200,\n"
            "    #{seconds => Secs, nanos => US};\n"
            "ts_encode(V) ->\n"
            "    V.\n"
            "\n"
            "ts_decode(#{seconds := S, nanos := N}) ->\n"
            "    {Date, {Hour, Min, Seconds}} = calendar:gregorian_seconds_to_datetime(S + 62167219200),\n"
            "    Seconds1 = add_usecs(Seconds, N),\n"
            "    Time = {Hour, Min, Seconds1},\n"
            "    {Date, Time};\n"
            "ts_decode(V) ->\n"
            "    V.\n\n"
            "add_usecs(Secs, 0) ->\n"
            "    %% leave seconds as an integer if there are no usecs\n"
            "    Secs;\n"
            "add_usecs(Secs, USecs) ->\n"
            "    Secs + (USecs / 1000000).\n\n"
    end.

to_from_proto() ->
    "from_proto(Map) ->\n"
    "    maps:merge(new(), Map).\n\n"
    "to_proto(Map) when is_map(Map) ->\n"
    "    maps:fold(fun(_Key, null, AccIn) -> AccIn;\n"
    "             (Key, Value, AccIn) when is_list(Value) -> AccIn#{Key => [to_proto(V) || V <- Value]};\n"
    "             (Key, Value, AccIn) when is_map(Value) -> AccIn#{Key => to_proto(Value)};\n"
    "             (Key, Value, AccIn) -> AccIn#{Key => Value}\n"
    "          end, #{}, Map);\n"
    "to_proto(Value) ->\n"
    "    Value.\n\n".

empty_map(Table) ->
    "new() ->\n"
    "    " ++ build_empty_map(false, Table) ++ ".\n\n"
    "new_default() ->\n"
    "    " ++ build_empty_map(true, Table) ++ ".\n\n".

build_empty_map(true, Table) ->
    ColDict = Table#table.columns,
    ColumnList = lists:reverse(Table#table.select_list),
    lists:flatten("#{" ++ lists:join(", ", [proto_crudl_utils:to_string(C) ++ " => null" || C <- ColumnList,
                                            lists:member(C, Table#table.default_list) == false andalso
                                            proto_crudl_code:is_version(ColDict, C) == false]) ++ "}");
build_empty_map(_, Table) ->
    ColDict = Table#table.columns,
    ColumnList = lists:reverse(Table#table.select_list),
    lists:flatten("#{" ++ lists:join(", ", [proto_crudl_utils:to_string(C) ++ " => null" || C <- ColumnList,
                                            proto_crudl_code:is_version(ColDict, C) == false]) ++ "}").
