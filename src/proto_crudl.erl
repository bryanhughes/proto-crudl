%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2020, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Nov 2020 5:10 PM
%%%-------------------------------------------------------------------
-module(proto_crudl).
-author("bryan").

%% API
-export([main/1, make_fqn/1, format_fqn/1, process_configs/2]).

-include("proto_crudl.hrl").

%% escript Entry point
main(Args = [ConfigFile]) ->
    io:format("~n"),
    io:format("Erlang Protobuffer Database Mapping Code Generator~n"),
    io:format("==================================================~n"),
    io:format("Args: ~p~n~n", [Args]),

    case file:consult(ConfigFile) of
        {error, Reason} ->
            io:format(
                "ERROR: Failed to consult config file. ConfigFile=~p, Reason=~p, pwd=~p~n",
                [ConfigFile, Reason, file:get_cwd()]);
        {ok, []} ->
            io:format("ERROR: Config file is empty. ConfigFile=~p~n", [ConfigFile]),
            erlang:exit(-1);
        {ok, Terms} ->
            ProviderConfig = proplists:get_value(database, Terms),
            GeneratorConfig = proplists:get_value(generator, Terms),
            OutputConfig = proplists:get_value(output, Terms),
            ProtoConfig = proplists:get_value(proto, Terms),

            application:ensure_all_started(pgo),
            application:set_env(pg_types, uuid_format, string),

            start_provider(ProviderConfig),

            case read_database(ProviderConfig, GeneratorConfig) of
                {ok, Database0} ->
                    % Process the configs
                    {ok, Database1} = process_configs(GeneratorConfig, Database0),

                    % Now generate the protobuffers
                    ok = proto_crudl_proto:generate(ProtoConfig, Database1),

                   % Generate the Erlang code
                   ok = proto_crudl_code:generate(ProviderConfig, OutputConfig, Database1),

                    io:format("~n---------------- DONE -----------------~n"),
                    ok;
                {error, Reason} ->
                    io:format("ERROR: ~p", [Reason])
            end
    end;
main(Args) ->
    io:format("Invalid args: ~p~n~n", [Args]),
    show_usage().


show_usage() ->
    io:format("usage: proto_crudl <proto_crudl.config>~n~n"),
    io:format("This script will generate all the Create, Read, Update, Delete, and List/Search functions for each~n"),
    io:format("table in the database as a module.~n~n").

start_provider([{provider, postgres}, {host, Host}, {port, Port}, {database, DBName},
                {username, User}, {password, Password}]) ->
    pgo:start_pool(default, #{pool_size => 10, host => Host, port => Port, database => DBName,
                              user => User, password => Password});
start_provider([{provider, Provider}, _, _, _, _, _]) ->
    io:format("ERROR: Provider ~p is not supported yet.~n", [Provider]),
    erlang:error(provider_not_supported).


-spec read_database(Provider :: list(), Generator :: list()) -> {ok, Database :: #database{}} | {error, Reason :: any()}.
read_database(ProviderOptions, Generator) ->
    case proplists:get_value(provider, ProviderOptions) of
        postgres ->
            proto_crudl_psql:read_database(Generator);
        Other ->
            {error, {unsupported_provider, Other}}
    end.


-spec process_configs([tuple()], dict:dict()) -> {ok, #database{}} | {error, Reason :: any()}.
process_configs([], Database) ->
    {ok, Database};
process_configs([{options, Options} | Rest], Database) ->
    io:format("~nProcessing options...~n"),
    case process_options(Options, Database) of
        {ok, Database1} ->
            process_configs(Rest, Database1);
        {error, Reason} ->
            {error, Reason}
    end;
process_configs([{exclude_columns, Columns} | Rest], Database) ->
    io:format("~nProcessing exclude columns...~n"),
    case exclude_columns(Columns, Database) of
        {ok, Database1} ->
            process_configs(Rest, Database1)
    end;
process_configs([{extensions, Extensions} | Rest], Database) ->
    io:format("~nProcessing proto extensions...~n"),
    case process_extensions(Extensions, Database) of
        {ok, Database1} ->
            process_configs(Rest, Database1);
        {error, Reason} ->
            {error, Reason}
    end;
process_configs([{mapping, Mappings} | Rest], Database) ->
    io:format("~nProcessing custom mappings...~n"),
    case process_mappings(Mappings, Database) of
        {ok, Database1} ->
            process_configs(Rest, Database1)
    end;
process_configs([{transforms, Transforms} | Rest], Database) ->
    io:format("~nProcessing transformations...~n"),
    case process_transforms(Transforms, Database) of
        {ok, Database1} ->
            process_configs(Rest, Database1)
    end;
process_configs([{Unknown, _} | Rest], Database) when Unknown == schemas orelse Unknown == excluded ->
    process_configs(Rest, Database);
process_configs([Unknown | Rest], Database) ->
    io:format("~nWARNING! Unknown config: ~0p~n", [Unknown]),
    process_configs(Rest, Database).


-spec process_transforms([{string(), [{atom(), string()}]}], #database{}) -> {ok, #database{}} | {error, any()}.
process_transforms([], Database) ->
    {ok, Database};
process_transforms([{FQN, XForms} | Rest], Db = #database{tables = TableDict}) ->
    FQN1 = format_fqn(FQN),
    io:format("    Table: ~p~n", [FQN1]),
    case dict:find(FQN1, TableDict) of
        {ok, Table} ->
            case apply_table_transforms(Table, XForms) of
                {ok, Table1} ->
                    process_transforms(Rest, Db#database{tables = dict:store(FQN1, Table1, TableDict)});
                {error, Reason} ->
                    {error, Reason}
            end;
        error ->
            io:format("        Failed to lookup table ~p~n", [FQN1]),
            process_transforms(Rest, Db)
    end.

apply_table_transforms(Table, []) ->
    {ok, Table};
apply_table_transforms(Table, [{select, XForms} | Rest]) ->
    io:format("        select: ~p~n", [XForms]),
    case apply_column_transforms(select, Table, XForms) of
        {ok, Table1} ->
            apply_table_transforms(Table1, Rest)
    end;
apply_table_transforms(Table, [{insert, XForms} | Rest]) ->
    io:format("        insert: ~p~n", [XForms]),
    case apply_column_transforms(insert, Table, XForms) of
        {ok, Table1} ->
            apply_table_transforms(Table1, Rest);
        {error, Reason} ->
            {error, Reason}
    end;
apply_table_transforms(Table, [{update, XForms} | Rest]) ->
    io:format("        update: ~p~n", [XForms]),
    case apply_column_transforms(update, Table, XForms) of
        {ok, Table1} ->
            apply_table_transforms(Table1, Rest);
        {error, Reason} ->
            {error, Reason}
    end.

apply_column_transforms(_Clause, Table, []) ->
    logger:info("~p : SelectList=~p", [Table#table.name, Table#table.select_list]),
    logger:info("~p : InsertList=~p", [Table#table.name, Table#table.insert_list]),
    logger:info("~p : UpdateList=~p", [Table#table.name, Table#table.update_list]),
    {ok, Table};
apply_column_transforms(select, Table = #table{columns = ColDict, schema = S, name = N},
                        [X = {CName, Datatype, Operator} | Rest]) ->
    Op = proto_crudl_utils:to_binary(Operator),
    CName1 = proto_crudl_utils:to_binary(CName),
    case orddict:find(CName1, ColDict) of
        {ok, Column} ->
            ColDict1 = orddict:store(CName1, Column#column{select_xform = Op}, ColDict),
            apply_column_transforms(select, Table#table{columns = ColDict1}, Rest);
        error ->
            io:format("        INFO: Adding virtual column ~0p to ~0p.~0p from transform ~0p~n", [CName1, S, N, X]),
            VirtualColumn = #column{table_schema = S, table_name = N, name = CName1, ordinal_position = 99,
                                    data_type    = <<"virtual">>, udt_name = proto_crudl_utils:to_binary(Datatype),
                                    is_virtual   = true, is_nullable = true, default = null},

            ColDict1 = orddict:store(CName1, VirtualColumn#column{select_xform = Op},
                                     ColDict),
            apply_column_transforms(select, Table#table{columns     = ColDict1,
                                                        select_list = lists:append(Table#table.select_list, [CName1]),
                                                        insert_list = lists:append(Table#table.insert_list, [CName1]),
                                                        update_list = lists:append(Table#table.update_list, [CName1])}, Rest)
    end;
apply_column_transforms(insert, Table = #table{columns = ColDict, schema = S, name = N},
                        [X = {CName, _Datatype, Operator} | Rest]) ->
    Op = proto_crudl_utils:to_binary(Operator),
    CName1 = proto_crudl_utils:to_binary(CName),
    case orddict:find(CName1, ColDict) of
        {ok, Column} ->
            ColDict1 = orddict:store(CName1, Column#column{insert_xform = Op}, ColDict),
            apply_column_transforms(insert, Table#table{columns = ColDict1}, Rest);
        error ->
            io:format("ERROR: column ~p is not part of ~p.~p => ~p~n", [CName1, S, N, X]),
            {error, notfound}
    end;
apply_column_transforms(update, Table = #table{columns = ColDict, schema = S, name = N},
                        [X = {CName, _Datatype, Operator} | Rest]) ->
    Op = proto_crudl_utils:to_binary(Operator),
    CName1 = proto_crudl_utils:to_binary(CName),
    case orddict:find(CName1, ColDict) of
        {ok, Column} ->
            ColDict1 = orddict:store(CName1, Column#column{update_xform = Op}, ColDict),
            apply_column_transforms(update, Table#table{columns = ColDict1}, Rest);
        error ->
            io:format("ERROR: column ~p is not part of ~p.~p => ~p~n", [CName1, S, N, X]),
            {error, notfound}
    end.

-spec process_mappings([{string(), [{atom(), string()}]}], #database{}) -> {ok, #database{}}.
process_mappings([], Database) ->
    {ok, Database};
process_mappings([{FQN, QueryList} | Rest], Db = #database{tables = TablesDict}) ->
    FQN1 = format_fqn(FQN),
    case dict:find(FQN1, TablesDict) of
        {ok, T0} ->
            io:format("    Table: ~p~n", [FQN1]),
            case process_mapping(QueryList, T0) of
                {ok, T1} ->
                    process_mappings(Rest, Db#database{tables = dict:store(FQN1, T1, TablesDict)})
            end;
        error ->
            io:format("    Failed to find table ~p~n", [FQN1]),
            process_mappings(Rest, Db)
    end.

process_mapping([], Table) ->
    {ok, Table};
process_mapping([{Name, Query} | Rest], Table) ->
    io:format("        ~p : ~p~n", [Name, Query]),
    process_mapping(Rest, Table#table{mappings = orddict:store(Name, Query, Table#table.mappings)}).


-spec process_extensions([tuple()], #database{}) -> {ok, #database{}} | {error, Reason :: any()}.
process_extensions([], Database) ->
    {ok, Database};
process_extensions([{FQN, Extension} | Rest], Db = #database{tables = TablesDict}) ->
    FQN1 = format_fqn(FQN),
    case dict:find(FQN1, TablesDict) of
        {ok, T0} ->
            io:format("    ~p : ~p~n", [FQN1, Extension]),
            T1 = T0#table{proto_extension = Extension},
            process_extensions(Rest, Db#database{tables = dict:store(FQN1, T1, TablesDict)});
        error ->
            io:format("     WARNING: Table not found while processing extensions for ~p : ~p~n", [FQN, Extension]),
            process_extensions(Rest, Db)
    end.


-spec process_options([tuple()], dict:dict()) -> {ok, #database{}} | {error, Reason :: any()}.
process_options([], Database) ->
    {ok, Database};
process_options([{version_column, ColumnName} | Rest], Database = #database{tables = TablesDict}) ->
    %% Test for existence and inject column if not there
    case maybe_inject_version(proto_crudl_utils:to_binary(ColumnName), dict:to_list(TablesDict), Database) of
        {ok, Database1} ->
            process_options(Rest, Database1);
        {error, Reason} ->
            {error, Reason}
    end;
process_options([check_constraints_as_enums | Rest], Database) ->
    process_options(Rest, Database);
process_options([indexed_lookups | Rest], Database) ->
    process_options(Rest, Database);
process_options([Unknown | Rest], Database) ->
    io:format("Unknown option - ~p~n", [Unknown]),
    process_options(Rest, Database).


-spec maybe_inject_version(binary(), [{binary(), #table{}}], #database{}) -> {ok, #database{}} | {error, Reason :: any()}.
maybe_inject_version(_ColumnName, [], Database) ->
    {ok, Database};
maybe_inject_version(ColumnName, [{_Key, T0 = #table{columns = Columns0}} | Rest], Db = #database{tables = TableDict}) ->
    case orddict:find(ColumnName, Columns0) of
        {ok, #column{data_type = <<"bigint">>}} ->
            logger:info("Found version column ~p on table ~p~p", [ColumnName, T0#table.schema, T0#table.name]),
            io:format("    Found version column ~p on table ~p.~p~n", [ColumnName, T0#table.schema, T0#table.name]),
            maybe_inject_version(ColumnName, Rest, Db);
        {ok, _} ->
            io:format("    WARNING: Found version column ~p on table ~p.~p but is not a bigint~n",
                      [ColumnName, T0#table.schema, T0#table.name]),
            maybe_inject_version(ColumnName, Rest, Db);
        error ->
            logger:info("Injecting version column ~p on table ~p~p", [ColumnName, T0#table.schema, T0#table.name]),
            io:format("    Injecting version column ~p on table ~p.~p~n", [ColumnName, T0#table.schema, T0#table.name]),
            Alter = "ALTER TABLE " ++ binary_to_list(T0#table.schema) ++ "." ++ binary_to_list(T0#table.name) ++
                    " ADD COLUMN " ++ binary_to_list(ColumnName) ++ " bigint",
            case pgo:query(Alter, [], #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}]}) of
                #{command := alter, num_rows := table, rows := []} ->
                    % Reread the columns
                    case proto_crudl_psql:read_table(T0#table.schema, T0#table.name, ColumnName) of
                        {ok, T1} ->
                            FQN = make_fqn(T1),
                            maybe_inject_version(ColumnName, Rest, Db#database{tables = dict:store(FQN, T1, TableDict)});
                        {error, Reason} ->
                            io:format("    ERROR: Failed to read columns from table ~p.~p. Reason=~p~n",
                                      [T0#table.schema, T0#table.name, Reason]),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    io:format(
                        "    ERROR: Failed to get alter table ~p.~p. Reason=~p, Stmt=~p~n",
                        [T0#table.schema, T0#table.name, Reason, Alter]),
                    {error, Reason}
            end
    end.


-spec exclude_columns([tuple()], #database{}) -> {ok, #database{}}.
exclude_columns([], Database) ->
    {ok, Database};
exclude_columns([{FQN, ColumnList} | Rest], Db = #database{tables = TablesDict}) ->
    % FQN is the fully qualified name: schema.table. If it is just table, then it will be the default schema
    FQN1 = format_fqn(FQN),
    case dict:find(FQN1, TablesDict) of
        {ok, T0} ->
            io:format("    Excluding columns ~p on table ~p.~p~n", [ColumnList, T0#table.schema, T0#table.name]),
            case mark_excluded(T0, ColumnList) of
                {ok, T1} ->
                    FQN1 = make_fqn(T1),
                    exclude_columns(Rest, Db#database{tables = dict:store(FQN1, T1, TablesDict)})
            end;
        error ->
            io:format("    Table ~p was not found~n", [FQN]),
            exclude_columns(Rest, Db)
    end.


-spec mark_excluded(#table{}, [string()]) -> {ok, #table{}}.
mark_excluded(T, []) ->
    {ok, T};
mark_excluded(T = #table{select_list = SList, insert_list = IList, update_list = UList, columns = ColDict},
              [CName | Rest]) ->
    ColName = proto_crudl_utils:to_binary(CName),
    case orddict:find(ColName, ColDict) of
        {ok, Column} ->
            case Column#column.is_pkey of
                true ->
                    io:format("    WARNING: Not able to exclude a primary key column ~p~n", [ColName]),
                    mark_excluded(T, Rest);
                _ ->
                    ColDict1 = orddict:store(ColName, Column#column{is_excluded = true}, ColDict),
                    mark_excluded(T#table{select_list = lists:delete(ColName, SList),
                                          insert_list = lists:delete(ColName, IList),
                                          update_list = lists:delete(ColName, UList),
                                          columns     = ColDict1}, Rest)
            end;
        error ->
            io:format("    WARNING: Failed to lookup column ~p~n", [ColName]),
            mark_excluded(T, Rest)
    end.

-spec format_fqn(binary()) -> binary() | failed.
format_fqn(FQN) ->
    B_FQN = proto_crudl_utils:to_binary(FQN),
    case binary:split(B_FQN, <<".">>) of
        [T] ->
            <<<<"public.">>/binary, T/binary>>;
        [_, _] ->
            B_FQN;
        _ ->
            failed
    end.

-spec make_fqn(#table{}) -> binary().
make_fqn(#table{schema = S, name = N}) ->
    <<S/binary, <<".">>/binary, N/binary>>.

%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(DROP_DB, "DROP DATABASE proto_crudl").
-define(CREATE_DB, "CREATE DATABASE proto_crudl WITH OWNER = proto_crudl ENCODING = 'UTF8' TEMPLATE = template0 CONNECTION LIMIT = -1").

first_test() ->
    logger:set_primary_config(level, info),
    application:ensure_all_started(pgo),
    application:set_env(pg_types, uuid_format, string),
    pgo:start_pool(default, #{pool_size => 10,
                              host => "127.0.0.1",
                              port => 5432,
                              database => "proto_crudl",
                              user => "proto_crudl",
                              password => "proto_crudl"}),

    ok.

process_transforms_test() ->
    ?LOG_INFO("====================== process_transforms_test() START ======================"),

    {ok, Database} = proto_crudl_psql:read_database([{schemas, ["public", "test_schema"]},
                                                   {excluded, ["public.excluded", "spatial_ref_sys"]}]),

    Configs = [{transforms, [
        {"test_schema.user", [
            % For the select transform, we need to know the datatype of the product of the transform. This is needed for
            % generating the protobufs
            {select, [{"lat", "decimal", "ST_Y(geog::geometry)"},
                      {"lon", "decimal", "ST_X(geog::geometry)"}]},
            {insert, [{"geog", "geography", "ST_POINT($lon, $lat)::geography"}]},
            {update, [{"geog", "geography", "ST_POINT($lon, $lat)::geography"}]}]},

        {"public.foo", [
            {select, [{"foobar", "integer", "1"}]}]}
    ]}],

    % Transformations are applied to each column based on the operation. Only select transforms can result in
    % the addition of virtual columns. The table map that the code generates will include all the table columns
    % except those in the excluded list. Note, that the excluded list may still be included in a write operation
    % depending on the transformation mapping.

    {ok, Database1} = process_configs(Configs, Database),
    TablesDict = Database1#database.tables,
    {ok, Table} = dict:find(<<"test_schema.user">>, TablesDict),

    SelectList = Table#table.select_list,
    ?LOG_INFO("SelectList=~p", [SelectList]),

    ExpectedList = [<<"user_id">>, <<"first_name">>, <<"last_name">>, <<"email">>,
                    <<"geog">>, <<"pword_hash">>, <<"user_token">>, <<"enabled">>,
                    <<"aka_id">>, <<"my_array">>, <<"user_type">>,
                    <<"number_value">>, <<"created_on">>, <<"updated_on">>,
                    <<"due_date">>, <<"lat">>, <<"lon">>],
    ?assertEqual(ExpectedList, SelectList),

    InsertList = Table#table.insert_list,
    ?LOG_INFO("InsertList=~p", [InsertList]),

    ColumnDict = Table#table.columns,

    Geog = orddict:fetch(<<"geog">>, ColumnDict),
    ?LOG_INFO("Geog=~p", [Geog]),
    ?assertEqual(#column{table_name   = <<"user">>, table_schema = <<"test_schema">>, name = <<"geog">>,
                         data_type    = <<"USER-DEFINED">>, default = null, is_nullable = true, is_pkey = false,
                         is_sequence  = false, is_virtual = false, ordinal_position = 5,
                         udt_name     = <<"geography">>,
                         insert_xform = <<"ST_POINT($lon, $lat)::geography">>,
                         update_xform = <<"ST_POINT($lon, $lat)::geography">>}, Geog),

    Lat = orddict:fetch(<<"lat">>, ColumnDict),
    ?LOG_INFO("Lat=~p", [Lat]),
    ?assertEqual(#column{table_name  = <<"user">>, table_schema = <<"test_schema">>, name = <<"lat">>,
                         data_type   = <<"virtual">>, default = null, is_nullable = true, is_pkey = false,
                         is_sequence = false, is_virtual = true, ordinal_position = 99,
                         udt_name    = <<"decimal">>, select_xform = <<"ST_Y(geog::geometry)">>}, Lat),


    ?LOG_INFO("====================== process_transforms_test() END ======================"),
    ok.

process_mappings_test() ->
    ?LOG_INFO("====================== process_mappings_test() START ======================"),

    {ok, Database} = proto_crudl_psql:read_database([{schemas, ["public", "test_schema"]},
                                                   {excluded, ["public.excluded", "spatial_ref_sys"]}]),

    Configs = [{mapping, [
        {"test_schema.user", [
            {get_pword_hash, "SELECT pword_hash FROM test_schema.user WHERE email = $1"},
            {update_pword_hash, "UPDATE test_schema.user SET pword_hash = $2 WHERE email = $1"},
            {reset_pword_hash, "UPDATE test_schema.user SET pword_hash = NULL WHERE email = $1"},
            {disable_user, "UPDATE test_schema.user SET enabled = false WHERE email = $1"},
            {enable_user, "UPDATE test_schema.user SET enabled = true WHERE email = $1"},
            {delete_user_by_email, "DELETE FROM test_schema.user WHERE email = $1"},
            {set_token, "UPDATE test_schema.user SET user_token = uuid_generate_v4() WHERE user_id = $1 RETURNING user_token"},
            {find_nearest, "SELECT *, ST_X(geog::geometry) AS lon, ST_Y(geog::geometry) AS lat FROM address "
                           "WHERE ST_DWithin( geog, Geography(ST_MakePoint($1, $2)), $3 ) AND lat != 0.0 AND lng != 0.0 "
                           "ORDER BY geog <-> ST_POINT($1, $2)::geography"}
        ]}
    ]}],

    {ok, Database1} = process_configs(Configs, Database),
    TablesDict = Database1#database.tables,
    {ok, UserTable} = dict:find(<<"test_schema.user">>, TablesDict),

    Mappings = UserTable#table.mappings,
    ?assertEqual(8, orddict:size(Mappings)),
    Query = orddict:fetch(get_pword_hash, Mappings),
    ?LOG_INFO("Query=~p", [Query]),
    ?assertEqual("SELECT pword_hash FROM test_schema.user WHERE email = $1", Query),

    {ok, ProductTable} = dict:find(<<"public.product">>, TablesDict),

    Mappings1 = ProductTable#table.mappings,
    ?assertEqual(0, orddict:size(Mappings1)),
    ?assertEqual(error, orddict:find(get_pword_hash, Mappings1)),

    ?LOG_INFO("====================== process_mappings_test() END ======================"),
    ok.

mark_excluded_columns_test() ->
    ?LOG_INFO("====================== mark_excluded_columns_test() START ======================"),

    {ok, Database} = proto_crudl_psql:read_database([{schemas, ["public", "test_schema"]},
                                                   {excluded, ["public.excluded", "spatial_ref_sys"]}]),

    Configs = [{exclude_columns, [
        {"test_schema.user", ["pword_hash", "geog"]}
    ]}],

    {ok, Database1} = process_configs(Configs, Database),
    TablesDict = Database1#database.tables,
    {ok, UserTable} = dict:find(<<"test_schema.user">>, TablesDict),

    ColumnDict = UserTable#table.columns,

    Geog = orddict:fetch(<<"geog">>, ColumnDict),
    ?LOG_INFO("Geog=~p", [Geog]),
    ?assertEqual(#column{table_name   = <<"user">>, table_schema = <<"test_schema">>, name = <<"geog">>,
                         data_type    = <<"USER-DEFINED">>, default = null, is_nullable = true, is_pkey = false,
                         is_sequence  = false, is_virtual = false, is_excluded = true, ordinal_position = 5,
                         udt_name     = <<"geography">>,
                         insert_xform = undefined,
                         update_xform = undefined}, Geog),

    SelectList = UserTable#table.select_list,
    ?LOG_INFO("SelectList=~p", [SelectList]),
    ?assertEqual([<<"user_id">>, <<"first_name">>, <<"last_name">>, <<"email">>,
                  <<"user_token">>, <<"enabled">>, <<"aka_id">>, <<"my_array">>,
                  <<"user_type">>, <<"number_value">>, <<"created_on">>,
                  <<"updated_on">>, <<"due_date">>], SelectList),

    ColumnList = orddict:fetch_keys(UserTable#table.columns),
    ?LOG_INFO("ColumnList=~p", [ColumnList]),
    ?assertEqual([<<"aka_id">>, <<"created_on">>, <<"due_date">>, <<"email">>,
                  <<"enabled">>, <<"first_name">>, <<"geog">>, <<"last_name">>,
                  <<"my_array">>, <<"number_value">>, <<"pword_hash">>,
                  <<"updated_on">>, <<"user_id">>, <<"user_token">>, <<"user_type">>], ColumnList),

    ?LOG_INFO("====================== mark_excluded_columns_test() END ======================"),
    ok.

cleanup_version([]) ->
    ok;
cleanup_version([{_Key, T0} | Rest]) ->
    Alter = "ALTER TABLE " ++ binary_to_list(T0#table.schema) ++ "." ++ binary_to_list(T0#table.name) ++
                                                                        " DROP COLUMN version",
    case pgo:query(Alter, [], #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}]}) of
        #{command := alter, num_rows := table, rows := []} ->
            cleanup_version(Rest);
        {error, Reason} ->
            io:format("    ERROR: Failed to get alter table ~p.~p. Reason=~p, Stmt=~p~n",
                      [T0#table.schema, T0#table.name, Reason, Alter]),
            {error, Reason}
    end.

last_test() ->
    ?LOG_INFO("====================== CLEANING UP VERSION COLUMNS ======================"),
    {ok, Database} = proto_crudl_psql:read_database([{schemas, ["public", "test_schema"]},
                                                   {excluded, ["public.excluded", "spatial_ref_sys"]}]),

    TablesDict = Database#database.tables,
    ok = cleanup_version(dict:to_list(TablesDict)).

-endif.