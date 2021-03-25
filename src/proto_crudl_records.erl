%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Feb 2021 5:35 PM
%%%-------------------------------------------------------------------
-module(proto_crudl_records).
-author("bryan").

%% API
-export([generate_functions/4]).

-include("proto_crudl.hrl").

-spec generate_functions(postgres | any(), string(), boolean(), #table{}) -> ok.
generate_functions(postgres, FullPath, UsePackage, T = #table{schema = S, name = N}) ->
    RecordName = case UsePackage of
                     true ->
                         "'" ++ proto_crudl_utils:to_string(S) ++ "." ++ proto_crudl_utils:camel_case(N) ++ "'";
                     false ->
                         "'" ++ proto_crudl_utils:camel_case(N) ++ "'"
                 end,
    ok = file:write_file(FullPath, ts_support(orddict:to_list(T#table.columns)), [append]),
    ok = file:write_file(FullPath, empty_record(RecordName, T), [append]),
    Schema = proto_crudl_utils:to_string(T#table.schema),
    ok = file:write_file(FullPath, custom_row_decoders(Schema, orddict:to_list(T#table.mappings), []), [append]),
    ok = file:write_file(FullPath, table_row_decoder(T), [append]),
    ok = file:write_file(FullPath, proto_crudl_psql:limit_fun(RecordName), [append]),
    ok = file:write_file(FullPath, proto_crudl_psql:create_fun(RecordName, T), [append]),
    case T#table.pkey_list of
        [] ->
            ok;
        _ ->
            ok = file:write_file(FullPath, proto_crudl_psql:read_or_create_fun(RecordName), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:read_fun(RecordName, T), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:update_fun(RecordName, T), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:delete_fun(RecordName, T), [append])
    end,
    ok = file:write_file(FullPath, proto_crudl_psql:list_lookup_fun(RecordName, T), [append]),
    ok = file:write_file(FullPath, proto_crudl_psql:mappings_fun(proto_crudl_utils:to_string(S), T), [append]);
generate_functions(Provider, _FullPath, _UsePackage, _Table) ->
    io:format("ERROR: Provider ~p is not supported yet.~n", [Provider]),
    erlang:error(provider_not_supported).

-spec ts_support([{binary(), #column{}}]) -> string().
ts_support([]) ->
    "";
ts_support([{_Key, #column{udt_name = UN}} | Rest]) ->
    case string:prefix(UN, <<"timestamp">>) of
        nomatch ->
            ts_support(Rest);
        _ ->
            "ts_encode_map(Datetime={{_, _, _}, {_, _, Seconds}}) when is_integer(Seconds) ->\n"
            "    Secs = calendar:datetime_to_gregorian_seconds(Datetime) - 62167219200,\n"
            "    #{seconds => Secs, nanos => 0};\n"
            "ts_encode_map({{Year, Month, Day}, {Hours, Minutes, Seconds}}) when is_float(Seconds)->\n"
            "    IntegerSeconds = trunc(Seconds),\n"
            "    US = trunc((Seconds - IntegerSeconds) * 1000000),\n"
            "    Secs = calendar:datetime_to_gregorian_seconds({{Year, Month, Day},\n"
            "                                                   {Hours, Minutes, IntegerSeconds}}) - 62167219200,\n"
            "    #'google.protobuf.Timestamp'{seconds = Secs, nanos = US};\n"
            "ts_encode_map(null) ->\n"
            "    undefined;\n"
            "ts_encode_map(V) ->\n"
            "    V.\n"
            "\n"
            "ts_decode_map(#'google.protobuf.Timestamp'{seconds = S, nanos = N}) ->\n"
            "    {Date, {Hour, Min, Seconds}} = calendar:gregorian_seconds_to_datetime(S + 62167219200),\n"
            "    Seconds1 = add_usecs(Seconds, N),\n"
            "    Time = {Hour, Min, Seconds1},\n"
            "    {Date, Time};\n"
            "ts_decode_map(V) ->\n"
            "    V.\n\n"
            "add_usecs(Secs, 0) ->\n"
            "    %% leave seconds as an integer if there are no usecs\n"
            "    Secs;\n"
            "add_usecs(Secs, USecs) ->\n"
            "    Secs + (USecs / 1000000).\n\n"
    end.

empty_record(RecordName, Table) ->
    "new() ->\n"
    "    " ++ build_empty_record(false, RecordName, Table) ++ ".\n\n"
    "new_default() ->\n"
    "    " ++ build_empty_record(true, RecordName, Table) ++ ".\n\n".

%% NOTE: Custom queries can not map enumerations to and from valid values.
custom_row_decoders(_SchemaName, [], Acc) ->
    Acc;
custom_row_decoders(SchemaName, [{_Key, #custom_query{name = Name, result_set = ResultSets}} | Rest], Acc) when is_list(SchemaName),
                                                                                                                length(ResultSets) > 0 ->
    RecordName = SchemaName ++ "." ++ proto_crudl_utils:camel_case(Name),
    Code = "decode_row(Row, _Fields, ['" ++ RecordName ++ "']) ->\n"
    "    F = fun(Field, Acc) ->\n"
    "            case Field of\n" ++
    decode_resultset(ResultSets, []) ++
    "                _ ->\n"
    "                    [case maps:get(Field, Row, undefined) of null -> undefined; V -> V end | Acc]\n"
    "            end\n"
    "        end,\n"
    "    L = lists:reverse(lists:foldl(F, ['" ++ RecordName ++ "'], record_info(fields, '" ++ RecordName ++ "'))),\n"
    "    list_to_tuple(L);\n",
    custom_row_decoders(SchemaName, Rest, [Code | Acc]);
custom_row_decoders(SchemaName, [_Head | Rest], Acc) when is_list(SchemaName) ->
    custom_row_decoders(SchemaName, Rest, Acc).

decode_resultset([], Acc) ->
    lists:reverse(Acc);
decode_resultset([#bind_var{name = Name, data_type = <<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>} | Rest], Acc) ->
    LowerName = proto_crudl_utils:to_string(Name),
    CamelCase = proto_crudl_utils:camel_case(Name),
    Code = "                " ++ LowerName ++ " ->\n" ++
           "                    " ++ CamelCase ++ " = maps:get(" ++ LowerName ++ ", Row, undefined),\n" ++
           "                    [ts_encode_map(" ++ CamelCase ++ ") | Acc];\n",
    decode_resultset(Rest, [Code | Acc]);
decode_resultset([_Head | Rest], Acc) ->
    decode_resultset(Rest, Acc).

table_row_decoder(#table{columns = ColDict, schema = S, name = N}) ->
    Columns = orddict:to_list(ColDict),
    RecordName = proto_crudl_utils:to_string(S) ++ "." ++ proto_crudl_utils:camel_case(N),
    "decode_row(Row, _Fields, ['" ++ RecordName ++ "']) ->\n"
    "    F = fun(Field, Acc) ->\n"
    "            case Field of\n" ++
    decode_columns(Columns, []) ++
    "                _ ->\n"
    "                    [case maps:get(Field, Row, undefined) of null -> undefined; V -> V end | Acc]\n"
    "            end\n"
    "        end,\n"
    "    L = lists:reverse(lists:foldl(F, ['" ++ RecordName ++ "'], record_info(fields, '" ++ RecordName ++ "'))),\n"
    "    list_to_tuple(L);\n"
    "decode_row(Row, _Fields, []) ->\n"
    "    Row.\n\n".


decode_columns([], Acc) ->
    lists:reverse(Acc);
decode_columns([{_Key, #column{name = N, valid_values = V}} | Rest], Acc) when length(V) > 0 ->
    Ln = proto_crudl_utils:to_list(N),
    Cc = proto_crudl_utils:camel_case(N),
    Code = ["                " ++ Ln ++ " ->\n" ++
            "                    " ++ Cc ++ " = maps:get(" ++ Ln ++ ", Row, undefined),\n" ++
            "                    [" ++ Ln ++ "_enum(" ++ Cc ++ ") | Acc];\n" | Acc],
    decode_columns(Rest, Code);
decode_columns([{_Key, #column{name = N, udt_name = <<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>}} | Rest], Acc) ->
    Ln = proto_crudl_utils:to_list(N),
    Cc = proto_crudl_utils:camel_case(N),
    Code = ["                " ++ Ln ++ " ->\n" ++
            "                    " ++ Cc ++ " = maps:get(" ++ Ln ++ ", Row, undefined),\n" ++
            "                    [ts_encode_map(" ++ Cc ++ ") | Acc];\n" | Acc],
    decode_columns(Rest, Code);
decode_columns([_Head | Rest], Acc) ->
    decode_columns(Rest, Acc).


build_empty_record(true, RecordName, #table{select_list = SelectList, default_list = DefaultList, columns = ColDict}) ->
    lists:flatten("#" ++ RecordName ++ "{" ++
                  lists:join(", ", [proto_crudl_utils:to_string(C) ++ " = undefined" || C <- SelectList,
                                    lists:member(C, DefaultList) == false andalso
                                    proto_crudl_code:is_version(ColDict, C) == false]) ++ "}");
build_empty_record(_, RecordName, #table{select_list = SelectList, columns = ColDict}) ->
    lists:flatten("#" ++ RecordName ++ "{" ++
                   lists:join(", ", [proto_crudl_utils:to_string(C) ++ " = undefined" || C <- SelectList,
                                     proto_crudl_code:is_version(ColDict, C) == false]) ++ "}").
