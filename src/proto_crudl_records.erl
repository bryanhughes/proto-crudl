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
    ok = file:write_file(FullPath, array_support(orddict:to_list(T#table.columns)), [append]),
    ok = file:write_file(FullPath, ts_support(proto_crudl_code:has_timestamps(T)), [append]),
    ok = file:write_file(FullPath, date_support(orddict:to_list(T#table.columns)), [append]),
    ok = file:write_file(FullPath, to_proto(T), [append]),
    ok = file:write_file(FullPath, from_proto(T), [append]),
    ok = file:write_file(FullPath, new_fun(RecordName, T), [append]),
    ok = file:write_file(FullPath, to_proplist(RecordName, T), [append]),
    ok = file:write_file(FullPath, from_proplist(RecordName, T), [append]),
    ok = file:write_file(FullPath, merge(RecordName), [append]),
    Schema = proto_crudl_utils:to_string(T#table.schema),
    ok = file:write_file(FullPath, raw_row_decoder(T), [append]),
    ok = file:write_file(FullPath, custom_row_decoders(Schema, orddict:to_list(T#table.mappings), []), [append]),
    ok = file:write_file(FullPath, table_row_decoder(T), [append]),
    ok = file:write_file(FullPath, proto_crudl_psql:limit_fun(RecordName), [append]),
    ok = file:write_file(FullPath, proto_crudl_psql:create_fun(RecordName, T), [append]),
    case T#table.pkey_list of
        [] ->
            ok;
        _ ->
            ok = file:write_file(FullPath, proto_crudl_psql:upsert_fun(RecordName, T), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:read_fun(RecordName, T), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:update_fun(RecordName, T), [append]),
            ok = file:write_file(FullPath, proto_crudl_psql:delete_fun(RecordName, T), [append])
    end,
    [ok = file:write_file(FullPath, proto_crudl_psql:update_fkeys_fun(RecordName, Rel, T), [append]) || Rel <- T#table.relations],
    ok = file:write_file(FullPath, proto_crudl_psql:list_lookup_fun(RecordName, T), [append]),
    ok = file:write_file(FullPath, proto_crudl_psql:mappings_fun(proto_crudl_utils:to_string(S), T), [append]);
generate_functions(Provider, _FullPath, _UsePackage, _Table) ->
    io:format("ERROR: Provider ~p is not supported yet.~n", [Provider]),
    erlang:error(provider_not_supported).

-spec array_support([{binary(), #column{}}]) -> string().
array_support([]) ->
    "";
array_support([{_Key, #column{data_type = <<"ARRAY">>}} | _Rest]) ->
    "ensure_array(undefined) ->
        [];
    ensure_array(null) ->
        [];
    ensure_array(V) when is_list(V) ->
        V;
    ensure_array(_V) ->
        erlang:error(list_expected).\n\n";
array_support([_Key | Rest]) ->
    array_support(Rest).

-spec ts_support(boolean()) -> string().
ts_support(false) ->
    "";
ts_support(true) ->
    "ts_encode(Datetime={{_, _, _}, {_, _, Seconds}}) when is_integer(Seconds) ->\n"
    "    Secs = calendar:datetime_to_gregorian_seconds(Datetime) - 62167219200,\n"
    "    #'google.protobuf.Timestamp'{seconds = Secs, nanos = 0};\n"
    "ts_encode({{Year, Month, Day}, {Hours, Minutes, Seconds}}) when is_float(Seconds)->\n"
    "    IntegerSeconds = trunc(Seconds),\n"
    "    US = trunc((Seconds - IntegerSeconds) * 1000000),\n"
    "    Secs = calendar:datetime_to_gregorian_seconds({{Year, Month, Day},\n"
    "                                                   {Hours, Minutes, IntegerSeconds}}) - 62167219200,\n"
    "    #'google.protobuf.Timestamp'{seconds = Secs, nanos = US};\n"
    "ts_encode(null) ->\n"
    "    undefined;\n"
    "ts_encode(V) ->\n"
    "    V.\n"
    "\n"
    "ts_decode(#'google.protobuf.Timestamp'{seconds = S, nanos = N}) ->\n"
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
    "    Secs + (USecs / 1000000).\n\n".

-spec date_support([{binary(), #column{}}]) -> string().
date_support([]) ->
    "";
date_support([{_Key, #column{udt_name = UN}} | Rest]) ->
    case string:prefix(UN, <<"date">>) of
        nomatch ->
            date_support(Rest);
        _ ->
            "date_encode(Date={Year, _, _}) when is_number(Year) ->\n"
            "    Secs = calendar:datetime_to_gregorian_seconds({Date, {0,0,0}}) - 62167219200,\n"
            "    #'google.protobuf.Timestamp'{seconds = Secs, nanos = 0};\n"
            "date_encode(null) ->\n"
            "    undefined;\n"
            "date_encode(V) ->\n"
            "    V.\n"
            "\n"
            "date_decode(#'google.protobuf.Timestamp'{seconds = S, nanos = _N}) ->\n"
            "    {Date, _} = calendar:gregorian_seconds_to_datetime(S + 62167219200),\n"
            "    Date;\n"
            "date_decode(V) ->\n"
            "    V.\n\n"
    end.

to_proplist(RecordName, Table) ->
    "to_proplist(R) ->\n"
    "    [" ++ build_to_proplist(RecordName, Table) ++ "].\n\n".

build_to_proplist(RecordName, #table{columns = ColDict}) ->
    L = ["{" ++ Col ++ ", R#" ++ RecordName ++ "." ++ Col ++ "}" || Col <-
        [proto_crudl_utils:to_string(C) || C <- orddict:fetch_keys(ColDict),
         proto_crudl_code:is_excluded(ColDict, C) == false]],
    lists:flatten(lists:join(", ", L)).

from_proplist(RecordName, Table) ->
    "from_proplist(PropList) ->\n"
    "    #" ++ RecordName ++ "{" ++ build_from_proplist(Table) ++ "}.\n\n".

build_from_proplist(#table{columns = ColDict}) ->
    Fun = fun({ColName, Column}, Acc) ->
             C = proto_crudl_utils:to_string(ColName),
             case Column#column.is_excluded of
                 true ->
                     Acc;
                 _ ->
                     ToFun = get_to_fun(proto_crudl_psql:sql_to_erlang_datatype(Column#column.udt_name)),
                     [C ++ " = " ++ ToFun ++"(proplists:get_value(<<\"" ++ C ++ "\">>, PropList))" | Acc]
             end
          end,
    L = lists:foldl(Fun, [], orddict:to_list(ColDict)),
    lists:flatten(lists:join(", ", L)).

get_to_fun("string") ->
    "proto_crudl_utils:to_string";
get_to_fun("binary") ->
    "proto_crudl_utils:to_binary";
get_to_fun("integer") ->
    "proto_crudl_utils:to_integer";
get_to_fun("float") ->
    "proto_crudl_utils:to_float";
get_to_fun("boolean") ->
    "proto_crudl_utils:to_boolean";
get_to_fun(_) ->
    "proto_crudl_utils:to_string".

merge(RecordName) ->
    "%%%----------------------------------------------------------------------------\n"
    "%%% @spec merge(To, From) -> #" ++ RecordName ++ "{}\n"
    "%%%     To = #" ++ RecordName ++ "{}\n"
    "%%%     From = #" ++ RecordName ++ "{}\n"
    "%%%\n"
    "%%% @doc Merges two #" ++ RecordName ++ "{} instances. The first takes precedence.\n"
    "%%% @end\n"
    "%%%----------------------------------------------------------------------------\n"
    "merge(To, To) ->\n"
    "   To;\n"
    "merge(To, From) when is_record(To, " ++ RecordName ++ "),\n"
    "                     is_record(From, " ++ RecordName ++ ") ->\n"
    "   list_to_tuple(lists:append([" ++ RecordName ++ "],\n"
    "                 merge(tl(tuple_to_list(To)),\n"
    "                       tl(tuple_to_list(From)),\n"
    "                       tl(tuple_to_list(#" ++ RecordName ++ "{})),\n"
    "                       []))).\n\n"
    "merge([D|ATail], [D|BTail], [D|DTail], To) ->\n"
    "   merge(ATail, BTail, DTail, [D|To]); % If default, take from D\n"
    "merge([D|ATail], [B|BTail], [D|DTail], To) ->\n"
    "   merge(ATail, BTail, DTail, [B|To]); % If only A default, take from B\n"
    "merge([A|ATail], [_|BTail], [_|DTail], To) ->\n"
    "   merge(ATail, BTail, DTail, [A|To]); % Otherwise take from A\n"
    "merge([],        [],        [],        To) ->\n"
    "   lists:reverse(To).\n\n".

raw_row_decoder(#table{columns = ColDict, schema = S, name = N}) ->
    Columns = orddict:to_list(ColDict),
    RecordName = proto_crudl_utils:to_string(S) ++ ".Raw" ++ proto_crudl_utils:camel_case(N),
    "decode_row(Row, _Fields, ['" ++ RecordName ++ "']) ->\n"
    "    F = fun(Field, Acc) ->\n"
    "            case Field of\n" ++
    decode_columns(Columns, []) ++
    "                _ ->\n"
    "                    [case maps:get(Field, Row, undefined) of null -> undefined; V -> V end | Acc]\n"
    "            end\n"
    "        end,\n"
    "    L = lists:reverse(lists:foldl(F, ['" ++ RecordName ++ "'], record_info(fields, '" ++ RecordName ++ "'))),\n"
    "    list_to_tuple(L);\n".

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
           "                    [ts_encode(" ++ CamelCase ++ ") | Acc];\n",
    decode_resultset(Rest, [Code | Acc]);
decode_resultset([#bind_var{name = Name, data_type = <<"date">>} | Rest], Acc) ->
    LowerName = proto_crudl_utils:to_string(Name),
    CamelCase = proto_crudl_utils:camel_case(Name),
    Code = "                " ++ LowerName ++ " ->\n" ++
           "                    " ++ CamelCase ++ " = maps:get(" ++ LowerName ++ ", Row, undefined),\n" ++
           "                    [date_encode(" ++ CamelCase ++ ") | Acc];\n",
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
decode_columns([{_Key, #column{name = N, data_type = <<"ARRAY">>}} | Rest], Acc) ->
    Ln = proto_crudl_utils:to_list(N),
    Code = ["                " ++ Ln ++ " ->\n" ++
            "                    [case maps:get(Field, Row, []) of null -> []; V -> V end | Acc];\n" | Acc],
    decode_columns(Rest, Code);
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
            "                    [ts_encode(" ++ Cc ++ ") | Acc];\n" | Acc],
    decode_columns(Rest, Code);
decode_columns([{_Key, #column{name = N, udt_name = <<"date">>}} | Rest], Acc) ->
    Ln = proto_crudl_utils:to_list(N),
    Cc = proto_crudl_utils:camel_case(N),
    Code = ["                " ++ Ln ++ " ->\n" ++
            "                    " ++ Cc ++ " = maps:get(" ++ Ln ++ ", Row, undefined),\n" ++
            "                    [date_encode(" ++ Cc ++ ") | Acc];\n" | Acc],
    decode_columns(Rest, Code);
decode_columns([_Head | Rest], Acc) ->
    decode_columns(Rest, Acc).

new_fun(RecordName, Table = #table{query_dict = QD}) ->
    Q = orddict:fetch("new", QD),
    InParams = lists:join(", ", Q#query.in_params),
    "new(" ++ InParams ++ ") ->\n"
    "    " ++ build_empty_record(RecordName, Table) ++ ".\n\n".
build_empty_record(RecordName, #table{insert_list = ColumnList, columns = ColDict}) ->
    lists:flatten("#" ++ RecordName ++ "{" ++
                  lists:join(", ", [proto_crudl_utils:to_string(C) ++ " = " ++ proto_crudl_utils:camel_case(C) || C <- ColumnList,
                                    proto_crudl_code:is_version(ColDict, C) == false]) ++ "}").


to_proto(#table{columns = ColDict, schema = S, name = N}) ->
    Columns = orddict:to_list(ColDict),
    RecordName = proto_crudl_utils:to_string(S) ++ "." ++ proto_crudl_utils:camel_case(N),
    lists:flatten(["to_proto(R) when is_record(R, '" ++ RecordName ++ "') ->\n",
                   "    R#'" ++ RecordName ++ "'{",
                   [Field || Field <- lists:join(",\n    ", to_proto_field(RecordName, Columns, []))],
                   "\n    };\n"
                   "to_proto(R) ->\n"
                   "    logger:error(\"Invalid record type. Expected 'test_schema.User' got ~p\", [R]),\n",
                   "    {error, invalid_record}.\n\n"]).

to_proto_field(_RecordName, [], Acc) ->
    Acc;
to_proto_field(RecordName, [{_Key, #column{name = N, udt_name = <<"date">>}} | Rest], Acc) ->
    Name = proto_crudl_utils:to_string(N),
    Code = Name ++ " = date_encode(R#'" ++ RecordName ++ "'." ++ Name ++ ")",
    to_proto_field(RecordName, Rest, [Code | Acc]);
to_proto_field(RecordName, [{_Key, #column{name = N, udt_name = <<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>}} | Rest], Acc) ->
    Name = proto_crudl_utils:to_string(N),
    Code = Name ++ " = ts_encode(R#'" ++ RecordName ++ "'." ++ Name ++ ")",
    to_proto_field(RecordName, Rest, [Code | Acc]);
to_proto_field(RecordName, [_Column | Rest], Acc) ->
    to_proto_field(RecordName, Rest, Acc).

from_proto(#table{columns = ColDict, schema = S, name = N}) ->
    Columns = orddict:to_list(ColDict),
    RecordName = proto_crudl_utils:to_string(S) ++ "." ++ proto_crudl_utils:camel_case(N),
    lists:flatten(["from_proto(R) when is_record(R, '" ++ RecordName ++ "') ->\n",
                   "    R#'" ++ RecordName ++ "'{",
                   [Field || Field <- lists:join(",\n    ", from_proto_field(RecordName, Columns, []))],
                   "\n    };\n"
                   "from_proto(R) ->\n"
                   "    logger:error(\"Invalid record type. Expected 'test_schema.User' got ~p\", [R]),\n",
                   "    {error, invalid_record}.\n\n"]).

from_proto_field(_RecordName, [], Acc) ->
    Acc;
from_proto_field(RecordName, [{_Key, #column{name = N, udt_name = <<"date">>}} | Rest], Acc) ->
    Name = proto_crudl_utils:to_string(N),
    Code = Name ++ " = date_decode(R#'" ++ RecordName ++ "'." ++ Name ++ ")",
    from_proto_field(RecordName, Rest, [Code | Acc]);
from_proto_field(RecordName, [{_Key, #column{name = N, udt_name = <<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>}} | Rest], Acc) ->
    Name = proto_crudl_utils:to_string(N),
    Code = Name ++ " = ts_decode(R#'" ++ RecordName ++ "'." ++ Name ++ ")",
    from_proto_field(RecordName, Rest, [Code | Acc]);
from_proto_field(RecordName, [_Column | Rest], Acc) ->
    from_proto_field(RecordName, Rest, Acc).

