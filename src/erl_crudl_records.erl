%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 16. Feb 2021 5:35 PM
%%%-------------------------------------------------------------------
-module(erl_crudl_records).
-author("bryan").

%% API
-export([generate_functions/4]).

-include("erl_crudl.hrl").

-spec generate_functions(postgres | any(), string(), boolean(), #table{}) -> ok.
generate_functions(postgres, FullPath, UsePackage, T = #table{schema = S, name = N}) ->
    RecordName = case UsePackage of
                     true ->
                         "'" ++ erl_crudl_utils:to_string(S) ++ "." ++ erl_crudl_utils:camel_case(N) ++ "'";
                     false ->
                         erl_crudl_utils:to_string(N)
                 end,
    ok = file:write_file(FullPath, empty_record(RecordName, T), [append]),
    ok = file:write_file(FullPath, erl_crudl_psql:limit_fun(), [append]),
    ok = file:write_file(FullPath, erl_crudl_psql:create_fun(RecordName, T), [append]),
    case T#table.pkey_list of
        [] ->
            ok;
        _ ->
            ok = file:write_file(FullPath, erl_crudl_psql:read_or_create_fun(RecordName), [append]),
            ok = file:write_file(FullPath, erl_crudl_psql:read_fun(RecordName, T), [append]),
            ok = file:write_file(FullPath, erl_crudl_psql:update_fun(RecordName, T), [append]),
            ok = file:write_file(FullPath, erl_crudl_psql:delete_fun(RecordName, T), [append])
    end,
    ok = file:write_file(FullPath, erl_crudl_psql:list_lookup_fun(RecordName, T), [append]),
    ok = file:write_file(FullPath, erl_crudl_psql:mappings_fun(RecordName, T), [append]);
generate_functions(Provider, _FullPath, _UsePackage, _Table) ->
    io:format("ERROR: Provider ~p is not supported yet.~n", [Provider]),
    erlang:error(provider_not_supported).


empty_record(RecordName, Table) ->
    "new() ->\n"
    "    " ++ build_empty_record(RecordName, Table) ++ ".\n\n".

build_empty_record(RecordName, #table{select_list = SelectList, default_list = DefaultList}) ->
    lists:flatten("#" ++ RecordName ++ "{" ++
                  lists:join(", ", [erl_crudl_utils:to_string(C) ++ " = null" || C <- SelectList,
                                    lists:member(C, DefaultList) == false]) ++ "}").
