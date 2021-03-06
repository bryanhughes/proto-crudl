%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 09. Jan 2021 11:18 AM
%%%-------------------------------------------------------------------
-module(proto_crudl_psql).
-author("bryan").

-include("proto_crudl.hrl").

%% API
-export([read_database/2, is_int64/1, is_int32/1, is_sql_float/1, read_columns/3, read_table/4,
         sql_to_proto_datatype/1, count_params/1, limit_fun/1, read_or_create_fun/1, create_fun/2,
         default_value/1, read_fun/2, update_fun/2, delete_fun/2, mappings_fun/2, list_lookup_fun/2,
         sql_to_erlang_datatype/1, create_defaults_record_fun/4, is_timestamp/1, random_value/1]).

-define(READ_TABLES, "SELECT
        t.table_schema,
        t.table_name
    FROM
        pg_namespace ns
        JOIN pg_class cls ON
            cls.relnamespace = ns.oid
        JOIN information_schema.tables t ON
            t.table_name = cls.relname
            AND t.table_schema = ns.nspname
    WHERE
        t.table_type = 'BASE TABLE'
        AND t.table_name NOT IN ($2)
        AND ns.nspname = $1
        AND cls.relispartition = 'f'
    ORDER BY table_name").

-define(READ_COLUMNS, "SELECT
        c.column_name,
        c.ordinal_position,
        c.data_type,
        c.udt_name::regtype::text,
        c.column_default,
        c.is_nullable,
        CASE WHEN pa.attname is null THEN false ELSE true END is_pkey,
        CASE WHEN pg_get_serial_sequence(table_schema || '.' || table_name, column_name) is null THEN false ELSE true END is_seq
    FROM
        pg_namespace ns
    JOIN pg_class t ON
        t.relnamespace = ns.oid
        AND t.relkind in ('r', 'p')
        AND t.relname = $2
    JOIN information_schema.columns c ON
        c.table_schema = ns.nspname
        AND c.table_name = t.relname
    LEFT OUTER JOIN pg_index pi ON
        pi.indrelid = t.oid AND pi.indisprimary = true
    LEFT OUTER JOIN pg_attribute pa ON
        pa.attrelid = pi.indrelid
        AND pa.attnum = ANY(pi.indkey)
        AND pa.attname = c.column_name
    WHERE
        ns.nspname = $1
    ORDER BY table_schema, table_name, ordinal_position DESC").

-define(READ_INDEXES, "SELECT
        i.relname AS index_name,
        a.attname AS column_name,
        ix.indisunique is_unique,
        ix.indisprimary is_pkey,
        obj_description(i.oid) AS comment
    FROM
        pg_class t,
        pg_class i,
        pg_index ix,
        pg_attribute a,
        pg_namespace ns
    WHERE
        t.oid = ix.indrelid
        AND i.oid = ix.indexrelid
        AND a.attrelid = t.oid
        AND a.attnum = ANY(ix.indkey)
        AND t.relkind = 'r'
        AND t.relname = $2
        AND t.relnamespace = ns.oid
        AND ns.nspname = $1
    ORDER BY
        i.relname").

-define(READ_FOREIGN_RELATIONSHIPS, "SELECT DISTINCT
        f_kcu.table_schema AS foreign_schema,
        f_kcu.table_name AS foreign_table,
        f_kcu.column_name AS foreign_column,
        kcu.column_name AS local_column,
        f_kcu.ordinal_position
    FROM
        information_schema.key_column_usage kcu
        JOIN information_schema.referential_constraints rc ON
            rc.constraint_schema = kcu.constraint_schema
            AND rc.constraint_name = kcu.constraint_name
        JOIN information_schema.key_column_usage f_kcu ON
            f_kcu.constraint_schema = rc.unique_constraint_schema
            AND f_kcu.constraint_name = rc.unique_constraint_name
            AND f_kcu.ordinal_position = kcu.position_in_unique_constraint
    WHERE
        kcu.table_schema = $1
        AND kcu.table_name = $2
        AND kcu.position_in_unique_constraint IS NOT NULL
    ORDER BY
        foreign_schema, foreign_table").

-define(READ_CHECK_CONSTRAINTS, "SELECT
        pg_get_constraintdef(pgc.oid) as constraint
    FROM pg_constraint pgc
        JOIN pg_namespace nsp ON nsp.oid = pgc.connamespace
        JOIN pg_class cls ON pgc.conrelid = cls.oid
        LEFT JOIN information_schema.constraint_column_usage ccu ON
            pgc.conname = ccu.constraint_name AND
            nsp.nspname = ccu.constraint_schema
    WHERE
        contype ='c'
        AND ccu.table_schema = $1
        AND ccu.table_name = $2").

-define(MONEY, <<"money">>).
-define(FLOAT, <<"float">>).
-define(NUMERIC, <<"numeric">>).
-define(REAL, <<"real">>).
-define(FLOAT8, <<"float8">>).
-define(NUMBER, <<110, 117, 109, 101, 114, 105, 99, _Rest/binary>>).
-define(DECIMAL, <<100, 101, 99, 105, 109, 97, 108, _Rest/binary>>).
-define(DOUBLE, <<100, 111, 117, 98, 108, 101, _Rest/binary>>).
-define(BIGINT, <<98, 105, 103, 105, 110, 116, _Rest/binary>>).
-define(BIGSERIAL, <<"bigserial">>).
-define(SERIAL8, <<"serial8">>).
-define(INT, <<108, 110, 116, _Rest/binary>>).
-define(BIT, <<98, 105, 116, _Rest/binary>>).
-define(SMALLINT, <<115, 109, 97, 108, 108, 105, 110, 116, _Rest/binary>>).
-define(INT2, <<"int2">>).
-define(SMALLSERIAL, <<"smallserial">>).
-define(SERIAL, <<"serial">>).


-spec read_database(Conn :: pid(), Generator :: list()) -> {ok, #database{}} | {error, Reason :: any()}.
%% @doc Reads the information tables and returns a map of schemas keyed by the schema name
read_database(C, Generator = [{schemas, Schemas} | _Rest]) ->
    case read_schemas(C, Generator, Schemas, dict:new()) of
        {ok, TableDict} -> {ok, #database{tables = TableDict}};
        {error, Reason} -> {error, Reason}
    end.

-spec read_schemas(Conn :: pid(), Generator :: list(), Schemas :: list(), TablesDict :: dict:dict()) ->
    {ok, dict:dict()} | {error, Reason :: any()}.
read_schemas(_C, _Generator, [], TablesDict) ->
    {ok, TablesDict};
read_schemas(C, Generator, [Schema | Rest], TablesDict) ->
    Excluded = proplists:get_value(excluded, Generator),
    Options = proplists:get_value(options, Generator, []),
    VersionColumn = proto_crudl_utils:to_binary(proplists:get_value(version_column, Options, <<>>)),
    logger:info("Generator=~0p, Options=~0p, VersionCollumn=~p", [Generator, Options, VersionColumn]),
    Excluded1 = [case string:split(FQN, ".") of Parts when length(Parts) == 1 -> FQN; Parts -> lists:nth(2, Parts) end || FQN <- Excluded],
    ExcludedTables1 = ["'" ++ Table ++ "'" || Table <- case Excluded1 of [] -> ["_"]; _ -> Excluded1 end],
    ExcludedTables2 = lists:flatten(lists:join(",", ExcludedTables1)),

    FixedQuery = lists:flatten(string:replace(?READ_TABLES, "$2", ExcludedTables2)),
    io:format("Schema: ~p~n", [Schema]),

    case epgsql:equery(C, FixedQuery, [Schema]) of
        {ok, _Cols, []} ->
            io:format("WARNING: No tables found in schema ~p~n", [Schema]),
            read_schemas(C, Generator, Rest, TablesDict);
        {ok, _Cols, Rows} ->
            io:format("Processing ~p tables in schema ~p~n", [length(Rows), Schema]),
            case process_tables(C, Rows, TablesDict, VersionColumn) of
                {ok, TablesDict1} ->
                    read_schemas(C, Generator, Rest, TablesDict1)
            end;
        {error, Reason} ->
            io:format(
                "    ERROR: Failed to get tables from schema ~p. Reason=~p, Stmt=~p~n",
                [Schema, Reason, FixedQuery]),
            {error, Reason}
    end.

-spec process_tables(Conn :: pid(), Rows :: list(), dict:dict(), binary()) -> {ok, dict:dict()}.
%% @doc For each table in the results, this function will read the columns, indexes, and check constraints
process_tables(_C, [], TablesDict, _VersionColumn) ->
    {ok, TablesDict};
process_tables(C, [{S, T} | Rest], TablesDict, VersionColumn) ->
    io:format("Table: ~p.~p~n", [S, T]),
    case read_table(C, proto_crudl_utils:to_binary(S), proto_crudl_utils:to_binary(T), VersionColumn) of
        {ok, Table} ->
            FQN = proto_crudl:make_fqn(Table),
            process_tables(C, Rest, dict:store(FQN, Table, TablesDict), VersionColumn);
        {error, Reason} ->
            io:format("    ERROR: Failed to read columns for table ~p.~p. Reason=~p~n", [S, T, Reason]),
            {error, Reason}
    end.

-spec read_table(pid(), binary(), binary(), binary()) -> {ok, #table{}} | {error, Reason :: any()}.
read_table(C, Schema, Name, VersionColumn) ->
    case read_columns(C, #table{name = Name, schema = Schema}, VersionColumn) of
        {ok, T0} ->
            case read_indexes(C, T0) of
                {ok, T1} ->
                    case read_relationships(C, T1) of
                        {ok, T2} ->
                            read_constraints(C, T2);
                        {error, Reason} ->
                            io:format("    ERROR: Failed to read foriegn key relations for table ~p.~p. Reason=~p~n",
                                      [Schema, Name, Reason]),
                            {error, Reason}
                    end;
                {error, Reason} ->
                    io:format("    ERROR: Failed to read indexes for table ~p.~p. REason=~p~n", [Schema, Name, Reason]),
                    {error, Reason}
            end;
        {error, Reason} ->
            io:format("    ERROR: Failed to read columns for table ~p.~p. Reason=~p~n", [Schema, Name, Reason]),
            {error, Reason}
    end.

-spec read_columns(pid(), #table{}, binary()) -> {ok, #table{}} | {error, Reason :: any()}.
read_columns(C, T0 = #table{name = N, schema = S}, VersionColumn) ->
    case epgsql:equery(C, ?READ_COLUMNS, [S, N]) of
        {ok, _Fields, []} ->
            io:format("    INFO: No columns found for table ~p.~p~n", [S, N]),
            {ok, T0};
        {ok, _Fields, Rows} ->
            case process_columns(T0, VersionColumn, Rows) of
                {ok, T1} ->
                    {ok, T1}
            end;
        {error, Reason} ->
            io:format("    ERROR: Failed to get columns for table ~p.~p. Reason=~p, Stmt=~p~n",
                      [S, N, Reason, ?READ_COLUMNS]),
            {error, Reason}
    end.

-spec process_columns(Table :: #table{}, VersionColumn :: binary(), Rows :: list()) ->
    {ok, #table{}} | {error, Reason :: any()}.
process_columns(Table, _VersionColumn, []) ->
    {ok, Table};
process_columns(Table, VersionColumn, [{CN, OP, DT, UN, CD, IN, IP, IS} | Rest]) ->
    IN0 = case IN of <<"YES">> -> true; <<"NO">> -> false end,
    IsVersion = VersionColumn == CN,
    Col = #column{table_name  = Table#table.name, table_schema = Table#table.schema, name = CN, ordinal_position = OP,
                  data_type   = DT, udt_name = UN, default = CD, is_nullable = IN0, is_pkey = IP,
                  is_version = IsVersion, is_sequence = IS},
    ColDict = orddict:store(CN, Col, Table#table.columns),
    PkList = case IP of true -> [CN | Table#table.pkey_list]; _ -> Table#table.pkey_list end,
    Sequence = case IS of true -> CN; _ -> Table#table.sequence end,
    HasTimestamps = Table#table.has_timestamps or is_timestamp(UN),
    io:format("    Column: ~p  ~p  ~p ... (default: ~p, is_pkey: ~p, is_seq: ~p, is_nullable: ~p, is_version: ~p, has_timestamps: ~p)~n",
              [CN, DT, UN, CD, IP, IS, IN0, IsVersion, HasTimestamps]),

    DefaultList = case {IsVersion, CD, IS} of
                      {true, _, _} ->
                          % Ignore the default if the column is the version column
                          Table#table.default_list;
                      {_, null, false} ->
                          Table#table.default_list;
                      {_, _, true} ->
                          Table#table.default_list;
                      _ ->
                          [CN | Table#table.default_list] end,
    InsertList = case IS of false -> [CN | Table#table.insert_list]; _ -> Table#table.insert_list end,
    UpdateList = case IP of false -> [CN | Table#table.update_list]; _ -> Table#table.update_list end,
    process_columns(Table#table{columns        = ColDict,
                                has_timestamps = HasTimestamps,
                                version_column = VersionColumn,
                                select_list    = [CN | Table#table.select_list],
                                update_list    = UpdateList,
                                insert_list    = InsertList,
                                sequence       = Sequence,
                                pkey_list      = PkList,
                                default_list   = DefaultList}, VersionColumn, Rest).

-spec read_indexes(pid(), #table{}) -> {ok, #table{}} | {error, Reason :: any()}.
read_indexes(C, T0 = #table{name = N, schema = S}) ->
    case epgsql:equery(C, ?READ_INDEXES, [S, N]) of
        {ok, _Fields, []} ->
            io:format("    INFO: No indexes found for table ~p.~p~n", [S, N]),
            {ok, T0};
        {ok, _Fields, Rows} ->
            case process_indexes(undefined, S, N, Rows, []) of
                {ok, Indexes} ->
                    {ok, T0#table{indexes = Indexes}}
            end;
        {error, Reason} ->
            io:format(
                "    ERROR: Failed to get index for table ~p.~p. Reason=~p, Stmt=~p~n",
                [S, N, Reason, ?READ_INDEXES]),
            {error, Reason}
    end.

-spec process_indexes(CurrentIdx :: #index{} | undefined, Schema :: binary(), Table :: binary(), Rows :: list(),
                      IndexList :: list()) -> {ok, [#index{}]} | {error, Reason :: any()}.
process_indexes(Idx, _S, _T, [], IndexList) when is_record(Idx, index) ->
    io:format("    Index: ~p  ~0p (~p, is_list = ~p, is_lookup = ~p)~n",
        [Idx#index.name, Idx#index.columns, Idx#index.type, Idx#index.is_list, Idx#index.is_lookup]),
    {ok, [Idx | IndexList]};
process_indexes(Idx = #index{name = IN, columns = Cols}, S, T, [{IN, CN, _, _, _} | Rest], IdxDict) when is_record(Idx, index) ->
    % As long as the name of the result set equals the index, then add to it...
    process_indexes(Idx#index{columns = [CN | Cols]}, S, T, Rest, IdxDict);
process_indexes(Idx, S, T, [{IN, CN, IU, IP, IC} | Rest], IndexList) ->
    % If the name does not match, then if this is not the first iteration, then save the record and start on the next
    Type = case IP of
               true -> primary_key;
               false -> case IU of
                            true -> unique;
                            false -> non_unique
                        end
           end,
    IsList = case IN of <<$l, $i, $s, $t, $_, _Rest0/binary>> -> true; _ -> false end,
    IsLookup = case IN of <<$l, $o, $o, $k, $u, $p, $_, _Rest1/binary>> -> true; _ -> false end,
    NewIdx = #index{table_name = T, table_schema = S, name = IN, type = Type, comment = IC, is_list = IsList,
                    is_lookup  = IsLookup, columns    = [CN]},
    case Idx of
        undefined ->
            process_indexes(NewIdx, S, T, Rest, IndexList);
        _ ->
            io:format("    Index: ~p  ~0p (~p, is_list = ~p, is_lookup = ~p)~n",
                      [Idx#index.name, Idx#index.columns, Idx#index.type, Idx#index.is_list, Idx#index.is_lookup]),
            process_indexes(NewIdx, S, T, Rest, [Idx#index{columns = lists:reverse(Idx#index.columns)} | IndexList])
    end.

-spec read_relationships(pid(), #table{}) -> {ok, #table{}} | {error, Reason :: any()}.
read_relationships(C, T0 = #table{name = N, schema = S}) ->
    case epgsql:equery(C, ?READ_FOREIGN_RELATIONSHIPS, [S, N]) of
        {ok, _Fields, []} ->
            io:format("    INFO: No foreign relationships found for table ~p.~p~n", [S, N]),
            {ok, T0};
        {ok, _Fields, Rows} ->
            case process_relationships(undefined, S, N, Rows, []) of
                {ok, Relations} ->
                    {ok, T0#table{relations = Relations}}
            end;
        {error, Reason} ->
            io:format(
                "    ERROR: Failed to get foreign relationships for table ~p.~p. Reason=~p, Stmt=~p~n",
                [S, N, Reason, ?READ_FOREIGN_RELATIONSHIPS]),
            {error, Reason}
    end.

-spec process_relationships(CurrentRel :: #foreign_relation{} | undefined, Schema :: binary(), Table :: binary(), Rows :: list(), Accum :: list()) ->
    {ok, [#foreign_relation{}]} | {error, Reason :: any()}.
process_relationships(Rel, _S, _T, [], Relations) ->
    io:format("    Relation: ~p.~p  ~0p~n", [Rel#foreign_relation.foreign_schema,
                                             Rel#foreign_relation.foreign_table,
                                             Rel#foreign_relation.foreign_columns]),
    {ok, [Rel | Relations]};
process_relationships(Rel = #foreign_relation{foreign_schema = FS, foreign_table = FT, foreign_columns = FCols}, S, T,
    [{FS, FT, FC, OP, LC} | Rest], Relations) when is_record(Rel, foreign_relation) ->
    % If the foreign table and schema are the same, then add the column mapping
    FCol = #foreign_column{foreign_name = FC, local_name = LC, ordinal_position = OP},
    process_relationships(Rel#foreign_relation{foreign_columns = [FCol | FCols]}, S, T, Rest, Relations);
process_relationships(Rel, S, T, [{FS, FT, FC, LC, OP} | Rest], Relations) ->
    FCol = #foreign_column{foreign_name = FC, local_name = LC, ordinal_position = OP},
    NewRel = #foreign_relation{foreign_schema = FS, foreign_table = FT, foreign_columns = [FCol]},
    case Rel of
        undefined ->
            process_relationships(NewRel, S, T, Rest, Relations);
        _ ->
            io:format("    Relation: ~p.~p  ~0p~n", [Rel#foreign_relation.foreign_schema,
                                                     Rel#foreign_relation.foreign_table,
                                                     Rel#foreign_relation.foreign_columns]),
            process_relationships(NewRel, S, T, Rest, [Rel | Relations])
    end.

-spec read_constraints(pid(), #table{}) -> {ok, #table{}} | {error, Reason :: any()}.
read_constraints(C, T0 = #table{name = N, schema = S}) ->
    case epgsql:equery(C, ?READ_CHECK_CONSTRAINTS, [S, N]) of
        {ok, _Fields, []} ->
            io:format("    INFO: No check contraints found for table ~p.~p~n", [S, N]),
            {ok, T0};
        {ok, _Fields, Rows} ->
            process_constraints(S, N, Rows, T0);
        {error, Reason} ->
            io:format(
                "    ERROR: Failed to get check cosntraints for table ~p.~p. Reason=~p, Stmt=~p~n",
                [S, N, Reason, ?READ_CHECK_CONSTRAINTS]),
            {error, Reason}
    end.

process_constraints(_S, _N, [], T) ->
    {ok, T};
process_constraints(S, N, [{Const} | Rest], T0) ->
    % We need to parse the constraint:
    %  CHECK ((number_value > 1))
    %  CHECK (((user_type)::text = ANY ((ARRAY['BIG SHOT'::character varying, 'LITTLE-SHOT'::character varying,
    %                                          'BUSY_GUY'::character varying, 'BUSYGAL'::character varying,
    %                                          '123FUN'::character varying])::text[])))
    case erl_scan:string(binary_to_list(Const)) of
        {ok, [{var, 1, 'CHECK'}, {'(', 1}, {'(', 1}, {'(', 1}, {atom, 1, CName} | Rest0], _} ->
            ColumnName = atom_to_binary(CName),
            % Now we want to find the token ARRAY
            case parse_constraints(start_scan, Rest0, []) of
                {ok, ValidValues} ->
                    case orddict:find(ColumnName, T0#table.columns) of
                        {ok, C0} ->
                            VV = lists:reverse(ValidValues),
                            io:format("    Valid Values for ~p: ~0p~n", [ColumnName, VV]),
                            C1 = C0#column{valid_values = VV},
                            Cols = orddict:store(ColumnName, C1, T0#table.columns),
                            process_constraints(S, N, Rest, T0#table{columns = Cols, has_valid_values = true});
                        error ->
                            io:format("    ERROR: Failed to find column ~p in table ~p.~p", [ColumnName, S, N]),
                            process_constraints(S, N, Rest, T0)
                    end;
                {error, Reason} ->
                    io:format("    ERROR: Failed to process constraint ~p on column ~p.~p.~p. Reason=~p~n",
                              [Const, S, N, ColumnName, Reason]),
                    {error, Reason}
            end;
        _ ->
            process_constraints(S, N, Rest, T0)
    end.

parse_constraints(_, [], ValidValues) ->
    {ok, ValidValues};
parse_constraints(start_scan, [{var, 1, 'ARRAY'}, _ | Rest], ValidValues) ->
    parse_constraints(in_array, Rest, ValidValues);
parse_constraints(in_array, [{atom, 1, Value} | Rest], ValidValues) ->
    case atom_to_list(Value) of
        "character" ->
            parse_constraints(in_array, Rest, ValidValues);
        "varying" ->
            parse_constraints(in_array, Rest, ValidValues);
        "text" ->
            parse_constraints(in_array, Rest, ValidValues);
        ListVal ->
            parse_constraints(in_array, Rest, [list_to_binary(ListVal) | ValidValues])
    end;
parse_constraints(State, [_Head | Rest], ValidValues) ->
    parse_constraints(State, Rest, ValidValues).

-spec is_int64(binary()) -> boolean().
is_int64(?BIGINT) ->
    true;
is_int64(?BIGSERIAL) ->
    true;
is_int64(?SERIAL8) ->
    true;
is_int64(_Value) ->
    false.

-spec is_int32(binary()) -> boolean().
is_int32(?INT) ->
    true;
is_int32(?BIT) ->
    true;
is_int32(?SMALLINT) ->
    true;
is_int32(?INT2) ->
    true;
is_int32(?SMALLSERIAL) ->
    true;
is_int32(?SERIAL) ->
    true;
is_int32(_Value) ->
    false.

-spec is_sql_float(binary()) -> boolean().
is_sql_float(?MONEY) ->
    true;
is_sql_float(?FLOAT) ->
    true;
is_sql_float(?NUMERIC) ->
    true;
is_sql_float(?REAL) ->
    true;
is_sql_float(?FLOAT8) ->
    true;
is_sql_float(?NUMBER) ->
    % number...
    true;
is_sql_float(?DECIMAL) ->
    % decimal...
    true;
is_sql_float(?DOUBLE) ->
    % double...
    true;
is_sql_float(_) ->
    false.

-spec sql_to_erlang_datatype(binary()) -> string().
sql_to_erlang_datatype(<<"bigint">>) ->
    "integer";
sql_to_erlang_datatype(<<"bigint[]">>) ->
    "integer";
sql_to_erlang_datatype(<<"{array,int8}">>) ->
    "integer";
sql_to_erlang_datatype(<<"int8">>) ->
    "integer";
sql_to_erlang_datatype(<<"bigserial">>) ->
    "integer";
sql_to_erlang_datatype(<<"serial8">>) ->
    "integer";
sql_to_erlang_datatype(<<98, 105, 116, _Rest/binary>>) ->
    % bit [N]
    "integer";
sql_to_erlang_datatype(<<"boolean">>) ->
    "boolean";
sql_to_erlang_datatype(<<"boolean[]">>) ->
    "boolean";
sql_to_erlang_datatype(<<"{array,boolean}">>) ->
    "boolean";
sql_to_erlang_datatype({regtype, <<0, 0, 3, 232>>}) ->
    "boolean";
sql_to_erlang_datatype(<<"bool">>) ->
    "boolean";
sql_to_erlang_datatype(<<"bytea">>) ->
    "binary";
sql_to_erlang_datatype(<<"varchar">>) ->
    "string";
sql_to_erlang_datatype(<<"character varying">>) ->
    "string";
sql_to_erlang_datatype(<<"character varying[]">>) ->
    "string";
sql_to_erlang_datatype(<<99, 104, 97, 114, _Rest/binary>>) ->
    % char [N]
    "string";
sql_to_erlang_datatype({regtype, <<0, 0, 3, 247>>}) ->
    % character varying[]
    "string";
sql_to_erlang_datatype(<<"date">>) ->
    "integer";
sql_to_erlang_datatype(<<"double precision">>) ->
    "float";
sql_to_erlang_datatype(<<"float8">>) ->
    "float";
sql_to_erlang_datatype({regtype, <<0, 0, 0, 23>>}) ->
    % integer[]
    "integer";
sql_to_erlang_datatype(<<"integer">>) ->
    "integer";
sql_to_erlang_datatype(<<"integer[]">>) ->
    "integer";
sql_to_erlang_datatype(<<"{array,int4}}">>) ->
    "integer";
sql_to_erlang_datatype(<<"int">>) ->
    "integer";
sql_to_erlang_datatype(<<"int4">>) ->
    "integer";
sql_to_erlang_datatype(<<"json[]">>) ->
    "string";
sql_to_erlang_datatype({regtype, <<0, 0, 0, 199>>}) ->
    % json[]
    "string";
sql_to_erlang_datatype(<<"json">>) ->
    "string";
sql_to_erlang_datatype(<<"jsonb">>) ->
    "binary";
sql_to_erlang_datatype(<<"money">>) ->
    "float";
sql_to_erlang_datatype(<<110, 117, 109, 101, 114, 105, 99, _Rest/binary>>) ->
    % number []
    "float";
sql_to_erlang_datatype(<<100, 101, 99, 105, 109, 97, 108, _Rest/binary>>) ->
    % decimal []
    "float";
sql_to_erlang_datatype(<<"real">>) ->
    "float";
sql_to_erlang_datatype(<<"float4">>) ->
    "float";
sql_to_erlang_datatype(<<"smallint">>) ->
    "integer";
sql_to_erlang_datatype(<<"smallint[]">>) ->
    "integer";
sql_to_erlang_datatype(<<"{array,int2}">>) ->
    "integer";
sql_to_erlang_datatype(<<"int2">>) ->
    "integer";
sql_to_erlang_datatype(<<"smallserial">>) ->
    "integer";
sql_to_erlang_datatype(<<"serial">>) ->
    "integer";
sql_to_erlang_datatype(<<"text">>) ->
    "string";
sql_to_erlang_datatype(<<"text[]">>) ->
    "string";
sql_to_erlang_datatype(<<"{array,text}">>) ->
    "string";
sql_to_erlang_datatype({regtype, <<0, 0, 3, 241>>}) ->
    % text[]
    "string";
sql_to_erlang_datatype(<<"uuid">>) ->
    "string";
sql_to_erlang_datatype(<<"xml">>) ->
    "string";
sql_to_erlang_datatype(<<"time">>) ->
    "integer";
sql_to_erlang_datatype(<<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>) ->
    % timestamp
    "binary";
sql_to_erlang_datatype(Unknown) ->
    io:format("    [warning] Failed to map postgres datatype to erlang: ~p. Using \"binary\"~n", [Unknown]),
    "binary".


-spec sql_to_proto_datatype(binary()) -> string().
sql_to_proto_datatype(<<"bigint">>) ->
    "int64";
sql_to_proto_datatype(<<"bigint[]">>) ->
    "int64";
sql_to_proto_datatype(<<"{array,int8}">>) ->
    "int64";
sql_to_proto_datatype(<<"int8">>) ->
    "int64";
sql_to_proto_datatype(<<"bigserial">>) ->
    "int64";
sql_to_proto_datatype(<<"serial8">>) ->
    "int64";
sql_to_proto_datatype(<<98, 105, 116, _Rest/binary>>) ->
    % bit [N]
    "int32";
sql_to_proto_datatype(<<"boolean">>) ->
    "bool";
sql_to_proto_datatype(<<"boolean[]">>) ->
    "bool";
sql_to_proto_datatype(<<"{array,boolean}">>) ->
    "bool";
sql_to_proto_datatype({regtype, <<0, 0, 3, 232>>}) ->
    "bool";
sql_to_proto_datatype(<<"bool">>) ->
    "bool";
sql_to_proto_datatype(<<"bytea">>) ->
    "bytes";
sql_to_proto_datatype(<<"varchar">>) ->
    "string";
sql_to_proto_datatype(<<"character varying">>) ->
    "string";
sql_to_proto_datatype(<<"character varying[]">>) ->
    "string";
sql_to_proto_datatype(<<99, 104, 97, 114, _Rest/binary>>) ->
    % char [N]
    "string";
sql_to_proto_datatype({regtype, <<0, 0, 3, 247>>}) ->
    % character varying[]
    "string";
sql_to_proto_datatype(<<"date">>) ->
    "int64";
sql_to_proto_datatype(<<"double precision">>) ->
    "double";
sql_to_proto_datatype(<<"float8">>) ->
    "double";
sql_to_proto_datatype({regtype, <<0, 0, 0, 23>>}) ->
    % integer[]
    "int32";
sql_to_proto_datatype(<<"integer">>) ->
    "int32";
sql_to_proto_datatype(<<"integer[]">>) ->
    "int32";
sql_to_proto_datatype(<<"{array,int4}">>) ->
    "int32";
sql_to_proto_datatype(<<"int">>) ->
    "int32";
sql_to_proto_datatype(<<"int4">>) ->
    "int32";
sql_to_proto_datatype(<<"json[]">>) ->
    "string";
sql_to_proto_datatype({regtype, <<0, 0, 0, 199>>}) ->
    % json[]
    "string";
sql_to_proto_datatype(<<"json">>) ->
    "string";
sql_to_proto_datatype(<<"jsonb">>) ->
    "bytes";
sql_to_proto_datatype(<<"money">>) ->
    "double";
sql_to_proto_datatype(<<110, 117, 109, 101, 114, 105, 99, _Rest/binary>>) ->
    % number []
    "double";
sql_to_proto_datatype(<<100, 101, 99, 105, 109, 97, 108, _Rest/binary>>) ->
    % decimal []
    "double";
sql_to_proto_datatype(<<"real">>) ->
    "float";
sql_to_proto_datatype(<<"float4">>) ->
    "float";
sql_to_proto_datatype(<<"smallint">>) ->
    "int32";
sql_to_proto_datatype(<<"smallint[]">>) ->
    "int32";
sql_to_proto_datatype(<<"{array,int2}">>) ->
    "int32";
sql_to_proto_datatype(<<"int2">>) ->
    "int32";
sql_to_proto_datatype(<<"smallserial">>) ->
    "int32";
sql_to_proto_datatype(<<"serial">>) ->
    "int32";
sql_to_proto_datatype(<<"text">>) ->
    "string";
sql_to_proto_datatype(<<"text[]">>) ->
    "string";
sql_to_proto_datatype(<<"{array,text}">>) ->
    "string";
sql_to_proto_datatype({regtype, <<0, 0, 3, 241>>}) ->
    % text[]
    "string";
sql_to_proto_datatype(<<"uuid">>) ->
    "string";
sql_to_proto_datatype(<<"xml">>) ->
    "string";
sql_to_proto_datatype(<<"time">>) ->
    "int64";
sql_to_proto_datatype(<<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>) ->
    % timestamp
    "google.protobuf.Timestamp";
sql_to_proto_datatype(Unknown) ->
    io:format("    [warning] Failed to map postgres datatype to protobuf: ~p. Using \"bytes\"~n", [Unknown]),
    "bytes".

is_timestamp(<<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>) ->
    true;
is_timestamp(_) ->
    false.


-spec count_params(string()) -> non_neg_integer().
count_params(Query) ->
    {ok, Tokens, _} = erl_scan:string(Query),
    count_params(Tokens, sets:new()).

count_params([], Set) ->
    sets:size(Set);
count_params([{char, 1, Counter} | Rest], Set) ->
    count_params(Rest, sets:add_element(Counter, Set));
count_params([_E | Rest], Set) ->
    count_params(Rest, Set).

-spec random_value(atom()) -> any().
random_value(string) ->
    <<"Hello world!">>;
random_value(integer) ->
    23;
random_value(float) ->
    2.3;
random_value(double) ->
    23.2;
random_value(binary) ->
    <<"deadbeef">>;
random_value(boolean) ->
    true;
random_value(Type) ->
    logger:error("Unsupported type: ~p", [Type]),
    io:format("ERROR: Unsupported type: ~p", [Type]),
    erlang:error(unsupported_type).


-spec default_value(string()) -> any().
default_value(<<"bigint">>) ->
    "0";
default_value(<<"bigint[]">>) ->
    "0";
default_value(<<"int8">>) ->
    "0";
default_value(<<"bigserial">>) ->
    "0";
default_value(<<"serial8">>) ->
    "0";
default_value(<<98, 105, 116, _Rest/binary>>) ->
    % bit [N]
    "0";
default_value(<<"boolean">>) ->
    "false";
default_value(<<"boolean[]">>) ->
    "false";
default_value({regtype, <<0, 0, 3, 232>>}) ->
    "false";
default_value(false) ->
    "false";
default_value(<<"bytea">>) ->
    "<<>>";
default_value(<<"character varying">>) ->
    "<<\"\">>";
default_value(<<"character varying[]">>) ->
    "<<\"\">>";
default_value(<<99, 104, 97, 114, _Rest/binary>>) ->
    % char [N]
    "<<\"\">>";
default_value({regtype, <<0, 0, 3, 247>>}) ->
    % character varying[]
    "<<\"\">>";
default_value(<<"date">>) ->
    "0";
default_value(<<"double precision">>) ->
    "0.0";
default_value(<<"float8">>) ->
    "0.";
default_value({regtype, <<0, 0, 0, 23>>}) ->
    % integer[]
    "0";
default_value(<<"integer">>) ->
    "0";
default_value(<<"integer[]">>) ->
    "0";
default_value(<<"int">>) ->
    "0";
default_value(<<"int4">>) ->
    "0";
default_value(<<"json[]">>) ->
    "<<\"\">>";
default_value({regtype, <<0, 0, 0, 199>>}) ->
    % json[]
    "<<\"\">>";
default_value(<<"json">>) ->
    "<<\"\">>";
default_value(<<"jsonb">>) ->
    "<<>>";
default_value(<<"money">>) ->
    "0.0";
default_value(<<110, 117, 109, 101, 114, 105, 99, _Rest/binary>>) ->
    % number []
    "0.0";
default_value(<<100, 101, 99, 105, 109, 97, 108, _Rest/binary>>) ->
    % decimal []
    "0.0";
default_value(<<"real">>) ->
    "0.0";
default_value(<<"float4">>) ->
    "0.0";
default_value(<<"smallint">>) ->
    "0";
default_value(<<"smallint[]">>) ->
    "0";
default_value(<<"int2">>) ->
    "0";
default_value(<<"smallserial">>) ->
    "0";
default_value(<<"serial">>) ->
    "0";
default_value(<<"text">>) ->
    "<<\"\">>";
default_value(<<"text[]">>) ->
    "<<\"\">>";
default_value({regtype, <<0, 0, 3, 241>>}) ->
    % text[]
    "<<\"\">>";
default_value(<<"uuid">>) ->
    "<<\"\">>";
default_value(<<"xml">>) ->
    "<<\"\">>";
default_value(<<"time">>) ->
    "0";
default_value(<<116, 105, 109, 101, 115, 116, 97, 109, 112, _Rest/binary>>) ->
    % timestamp
    "#{}";
default_value(Unknown) ->
    io:format("[warning] Failed to map postgres datatype to default value: ~p. Using \"bytes\"~n", [Unknown]),
    "<<>>".

-spec create_fun(string() | undefined, #table{}) -> string().
create_fun(undefined, Table) ->
    {Map, DMap} = proto_crudl_code:build_insert_map(Table),
    {Param, DParam} = proto_crudl_code:build_insert_params(Table),
    "create(M = " ++ Map ++ ") when is_map(M) ->\n" ++
    "    Params = " ++ Param ++ ",\n"
    "    case pgo:query(?INSERT, Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := insert, num_rows := 0} ->\n"
    "            {error, failed_to_insert};\n"
    "        #{command := insert, num_rows := 1, rows := [Row]} ->\n"
    "            {ok, Row};\n"
    "        {error, {pgsql_error, #{code := <<\"23505\">>}}} ->\n"
    "            {error, exists};\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to insert. Reason=~p, Query=~p, Params=~p\", [Reason, ?INSERT, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n" ++
    create_defaults_map_fun(Table, DMap, DParam) ++
    "create(_M) ->\n"
    "    {error, invalid_map}.\n\n";
create_fun(RecordName, Table = #table{default_list = DL}) ->
    {Record, DRecord} = proto_crudl_code:build_insert_record(RecordName, Table),
    {Param, DParam} = proto_crudl_code:build_insert_params(Table),
    Str = "create(R = " ++ Record ++ ") when is_record(R, " ++ RecordName ++ ") ->\n" ++
    "    Params = " ++ Param ++ ",\n"
    "    case pgo:query(?INSERT, Params, #{decode_opts => [{decode_fun_params, [" ++ RecordName ++ "]}, {return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := insert, num_rows := 0} ->\n"
    "            {error, failed_to_insert};\n"
    "        #{command := insert, num_rows := 1, rows := [Row]} ->\n"
    "            {ok, Row};\n"
    "        {error, {pgsql_error, #{code := <<\"23505\">>}}} ->\n"
    "            {error, exists};\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to insert. Reason=~p, Query=~p, Params=~p\", [Reason, ?INSERT, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n" ++
    "create(_M) ->\n"
    "    {error, invalid_record}.\n\n",
    case length(DL) > 0 of
        true -> create_defaults_record_fun(Table, RecordName, DRecord, DParam) ++ Str;
        _ -> Str
    end.

create_defaults_map_fun(#table{default_list = DL}, _DMap, _DParam) when length(DL) == 0 ->
    "";
create_defaults_map_fun(_Table, DMap, DParam) ->
    "create(M = " ++ DMap ++ ") when is_map(M) ->\n" ++
    "    Params = " ++ DParam ++ ",\n"
    "    case pgo:query(?INSERT_DEFAULTS, Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := insert, num_rows := 0} ->\n"
    "            {error, failed_to_insert};\n"
    "        #{command := insert, num_rows := 1, rows := [Row]} ->\n"
    "            {ok, Row};\n"
    "        {error, {pgsql_error, #{code := <<\"23505\">>}}} ->\n"
    "            {error, exists};\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to insert with defaults. Reason=~p, Query=~p, Params=~p\", [Reason, ?INSERT_DEFAULTS, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n".

create_defaults_record_fun(#table{default_list = DL}, _RecordName, _DRecord, _DParam) when length(DL) == 0 ->
    "";
create_defaults_record_fun(_Table, RecordName, DRecord, DParam) ->
    "create(R = " ++ DRecord ++ ") when is_record(R, " ++ RecordName ++ ") ->\n" ++
    "    Params = " ++ DParam ++ ",\n"
    "    case pgo:query(?INSERT_DEFAULTS, Params, #{decode_opts => [{decode_fun_params, [" ++ RecordName ++ "]}, {return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := insert, num_rows := 0} ->\n"
    "            {error, failed_to_insert};\n"
    "        #{command := insert, num_rows := 1, rows := [Row]} ->\n"
    "            {ok, Row};\n"
    "        {error, {pgsql_error, #{code := <<\"23505\">>}}} ->\n"
    "            {error, exists};\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to insert with defaults. Reason=~p, Query=~p, Params=~p\", [Reason, ?INSERT_DEFAULTS, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n".

-spec read_or_create_fun(string() | undefined) -> string().
read_or_create_fun(undefined) ->
    "read_or_create(M) when is_map(M) ->\n"
    "    case read(M) of\n"
    "        notfound ->\n"
    "            create(M);\n"
    "        {ok, Map} ->\n"
    "            {ok, Map};\n"
    "        {error, Reason} ->\n"
    "            {error, Reason}\n"
    "    end;\n"
    "read_or_create(_M) ->\n"
    "    {error, invalid_map}.\n\n";
read_or_create_fun(RecordName) ->
    "read_or_create(R) when is_record(R, " ++ RecordName ++ ") ->\n"
    "    case read(R) of\n"
    "        notfound ->\n"
    "            create(R);\n"
    "        {ok, Record} ->\n"
    "            {ok, Record};\n"
    "        {error, Reason} ->\n"
    "            {error, Reason}\n"
    "    end;\n"
    "read_or_create(_R) ->\n"
    "    {error, invalid_record}.\n\n".

-spec read_fun(string() | undefined, #table{}) -> string().
read_fun(undefined, Table) ->
    Map = proto_crudl_code:build_select_map(Table),
    Params = proto_crudl_code:build_select_params(Table),
    "read(M = " ++ Map ++ ") when is_map(M) ->\n"
    "    Params = " ++ Params ++ ",\n"
    "    case pgo:query(?SELECT, Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := select, num_rows := 0} ->\n"
    "            notfound;\n"
    "        #{command := select, num_rows := 1, rows := [Row]} ->\n"
    "            {ok, Row};\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to read. Reason=~p, Query=~p, Params=~p\", [Reason, ?SELECT, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n"
    "read(_M) ->\n"
    "    {error, invalid_map}.\n\n";
read_fun(RecordName, Table) ->
    Record = proto_crudl_code:build_select_record(RecordName, Table),
    Params = proto_crudl_code:build_select_params(Table),
    "read(R = " ++ Record ++ ") when is_record(R, " ++ RecordName ++ ") ->\n"
    "    Params = " ++ Params ++ ",\n"
    "    case pgo:query(?SELECT, Params, #{decode_opts => [{decode_fun_params, [" ++ RecordName ++ "]}, {return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := select, num_rows := 0} ->\n"
    "            notfound;\n"
    "        #{command := select, num_rows := 1, rows := [Row]} ->\n"
    "            {ok, Row};\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to read. Reason=~p, Query=~p, Params=~p\", [Reason, ?SELECT, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n"
    "read(_M) ->\n"
    "    {error, invalid_record}.\n\n".

-spec update_fun(string() | undefined, #table{}) -> string().
update_fun(undefined, Table) ->
    Map = proto_crudl_code:build_update_map(Table),
    Params = proto_crudl_code:build_update_params(Table),
    "update(M = " ++ Map ++ ") when is_map(M) ->\n" ++
    "    Params = " ++ Params ++ ",\n"
    "    case pgo:query(?UPDATE, Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := update, num_rows := 0} ->\n"
    "            notfound;\n"
    "        #{command := update, num_rows := 1, rows := [Row]} ->\n"
    "            {ok, Row};\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to update. Reason=~p, Query=~p, Params=~p\", [Reason, ?UPDATE, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n"
    "update(_M) ->\n"
    "    {error, invalid_map}.\n\n";
update_fun(RecordName, Table) ->
    Record = proto_crudl_code:build_update_record(RecordName, Table),
    Params = proto_crudl_code:build_update_params(Table),
    "update(R = " ++ Record ++ ") when is_record(R, " ++ RecordName ++ ") ->\n" ++
    "    Params = " ++ Params ++ ",\n"
    "    case pgo:query(?UPDATE, Params, #{decode_opts => [{decode_fun_params, [" ++ RecordName ++ "]}, {return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := update, num_rows := 0} ->\n"
    "            notfound;\n"
    "        #{command := update, num_rows := 1, rows := [Row]} ->\n"
    "            {ok, Row};\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to update. Reason=~p, Query=~p, Params=~p\", [Reason, ?UPDATE, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n"
    "update(_M) ->\n"
    "    {error, invalid_record}.\n\n".

-spec delete_fun(string() | undefined, #table{}) -> string().
delete_fun(undefined, Table) ->
    Map = proto_crudl_code:build_delete_map(Table),
    Params = proto_crudl_code:build_delete_params(Table),
    "delete(M = " ++ Map ++ ") when is_map(M) ->\n"
    "    Params = " ++ Params ++ ",\n"
    "    case pgo:query(?DELETE, Params) of\n"
    "        #{command := delete, num_rows := 0} ->\n"
    "            notfound;\n"
    "        #{command := delete, num_rows := 1} ->\n"
    "            ok;\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to delete. Reason=~p, Query=~p, Params=~p\", [Reason, ?DELETE, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n"
    "delete(_M) ->\n"
    "    {error, invalid_map}.\n\n";
delete_fun(RecordName, Table) ->
    Record = proto_crudl_code:build_delete_record(RecordName, Table),
    Params = proto_crudl_code:build_delete_params(Table),
    "delete(R = " ++ Record ++ ") when is_record(R, " ++ RecordName ++ ") ->\n"
    "    Params = " ++ Params ++ ",\n"
    "    case pgo:query(?DELETE, Params) of\n"
    "        #{command := delete, num_rows := 0} ->\n"
    "            notfound;\n"
    "        #{command := delete, num_rows := 1} ->\n"
    "            ok;\n"
    "        {error, Reason} ->\n"
    "            logger:error(\"Failed to delete. Reason=~p, Query=~p, Params=~p\", [Reason, ?DELETE, Params]),\n"
    "            {error, Reason}\n"
    "    end;\n"
    "delete(_M) ->\n"
    "    {error, invalid_record}.\n\n".

-spec limit_fun(string() | undefined) -> string().
limit_fun(undefined) ->
    "list(Limit, Offset) ->\n"
    "    case pgo:query(?SELECT_WITH_LIMIT, [Limit, Offset], #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := select, num_rows := NRows, rows := Rows} ->\n"
    "            {ok, NRows, Rows};\n"
    "        {error, Reason} ->\n"
    "            {error, Reason}\n"
    "    end.\n\n";
limit_fun(RecordName) ->
    "list(Limit, Offset) ->\n"
    "    case pgo:query(?SELECT_WITH_LIMIT, [Limit, Offset], #{decode_opts => [{decode_fun_params, [" ++ RecordName ++ "]}, {return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := select, num_rows := NRows, rows := Rows} ->\n"
    "            {ok, NRows, Rows};\n"
    "        {error, Reason} ->\n"
    "            {error, Reason}\n"
    "    end.\n\n".

-spec list_lookup_fun(string() | undefined, #table{}) -> string().
list_lookup_fun(RecordName, #table{indexes = Indexes}) ->
    list_lookup_fun(RecordName, Indexes, []).

list_lookup_fun(_RecordName, [], Acc) ->
    Acc;
list_lookup_fun(undefined, [#index{is_list = true, columns = Columns, name = N} | Rest], Acc) ->
    Map = proto_crudl_code:build_lookup_list_map(undefined, Columns),
    Params = proto_crudl_code:build_list_params(Columns),
    Name = proto_crudl_utils:to_string(N),
    S = Name ++ "(M = " ++ Map ++ ", Limit, Offset) when is_map(M) ->\n"
    "    Params = " ++ Params ++ ",\n"
    "    case pgo:query(?" ++ string:to_upper(Name) ++ ", Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := select, num_rows := NRows, rows := Rows} ->\n"
    "            {ok, NRows, Rows};\n"
    "        {error, Reason} ->\n"
    "            {error, Reason}\n"
    "    end.\n\n",
    list_lookup_fun(undefined, Rest, [S | Acc]);
list_lookup_fun(undefined, [#index{is_lookup = true, columns = Columns, name = N} | Rest], Acc) ->
    Map = proto_crudl_code:build_lookup_list_map(undefined, Columns),
    Params = proto_crudl_code:build_lookup_params(Columns),
    Name = proto_crudl_utils:to_string(N),
    S = Name ++ "(M = " ++ Map ++ ") when is_map(M) ->\n"
    "    Params = " ++ Params ++ ",\n"
    "    case pgo:query(?" ++ string:to_upper(Name) ++ ", Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{command := select, num_rows := NRows, rows := Rows} ->\n"
    "            {ok, NRows, Rows};\n"
    "        {error, Reason} ->\n"
    "            {error, Reason}\n"
    "    end.\n\n",
    list_lookup_fun(undefined, Rest, [S | Acc]);
list_lookup_fun(RecordName, [#index{is_list = true, columns = Columns, name = N} | Rest], Acc) ->
    Record = proto_crudl_code:build_lookup_list_map(RecordName, Columns),
    Params = proto_crudl_code:build_list_params(Columns),
    Name = proto_crudl_utils:to_string(N),
    S = Name ++ "(R = " ++ Record ++ ", Limit, Offset) when is_record(R, " ++ RecordName ++ ") ->\n"
        "    Params = " ++ Params ++ ",\n"
        "    case pgo:query(?" ++ string:to_upper(Name) ++ ", Params, #{decode_opts => [{decode_fun_params, [" ++ RecordName ++ "]}, {return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
        "        #{command := select, num_rows := NRows, rows := Rows} ->\n"
        "            {ok, NRows, Rows};\n"
        "        {error, Reason} ->\n"
        "            {error, Reason}\n"
        "    end.\n\n",
    list_lookup_fun(RecordName, Rest, [S | Acc]);
list_lookup_fun(RecordName, [#index{is_lookup = true, columns = Columns, name = N} | Rest], Acc) ->
    Record = proto_crudl_code:build_lookup_list_map(RecordName, Columns),
    Params = proto_crudl_code:build_lookup_params(Columns),
    Name = proto_crudl_utils:to_string(N),
    S = Name ++ "(R = " ++ Record ++ ") when is_record(R, " ++ RecordName ++ ") ->\n"
        "    Params = " ++ Params ++ ",\n"
        "    case pgo:query(?" ++ string:to_upper(Name) ++ ", Params, #{decode_opts => [{decode_fun_params, [" ++ RecordName ++ "]}, {return_rows_as_maps, true}, {column_name_as_atom, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
        "        #{command := select, num_rows := NRows, rows := Rows} ->\n"
        "            {ok, NRows, Rows};\n"
        "        {error, Reason} ->\n"
        "            {error, Reason}\n"
        "    end.\n\n",
    list_lookup_fun(RecordName, Rest, [S | Acc]);
list_lookup_fun(RecordName, [_Idx | Rest], Acc) ->
    list_lookup_fun(RecordName, Rest, Acc).


-spec mappings_fun(string() | undefined, #table{}) -> string().
mappings_fun(RecordName, #table{mappings = Mappings}) ->
    mapping_fun(RecordName, Mappings, []).

mapping_fun(_RecordName, [], Acc) ->
    lists:join("\n\n", Acc);
mapping_fun(undefined, [{Name, #custom_query{query = Query}} | Rest], Acc) ->
    Cnt = count_params(Query),
    Params = lists:flatten(lists:join(", ", ["Value" ++ integer_to_list(C) || C <- lists:seq(1, Cnt)])),
    S = proto_crudl_utils:to_string(Name) ++ "(" ++ Params ++ ") ->\n"
    "    Params = [" ++ Params ++ "],\n"
    "    case pgo:query(?" ++ string:to_upper(atom_to_list(Name)) ++ ", Params, #{decode_opts => [{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{num_rows := NRows, rows := Rows} ->\n"
    "            {ok, NRows, Rows};\n"
    "        {error, Reason} ->\n"
    "            {error, Reason}\n"
    "    end.",
    mapping_fun(undefined, Rest, [S | Acc]);
mapping_fun(RecordPrefix, [{Name, #custom_query{query = Query, result_set = RS}} | Rest], Acc) ->
    Cnt = count_params(Query),
    FunParams = case length(RS) of
                    0 ->
                        "";
                    _ ->
                        RecordName = "'" ++ RecordPrefix ++ "." ++ proto_crudl_utils:camel_case(Name) ++ "'",
                        "{decode_fun_params, [" ++ RecordName ++ "]},"
                end,
    Params = lists:flatten(lists:join(", ", ["Value" ++ integer_to_list(C) || C <- lists:seq(1, Cnt)])),
    S = proto_crudl_utils:to_string(Name) ++ "(" ++ Params ++ ") ->\n"
    "    Params = [" ++ Params ++ "],\n"
    "    case pgo:query(?" ++ string:to_upper(atom_to_list(Name)) ++ ", Params, #{decode_opts => [" ++ FunParams ++ "{return_rows_as_maps, true}, {column_name_as_atom, true}, {decode_fun, fun decode_row/3}]}) of\n"
    "        #{num_rows := NRows, rows := Rows} ->\n"
    "            {ok, NRows, Rows};\n"
    "        {error, Reason} ->\n"
    "            {error, Reason}\n"
    "    end.",
    mapping_fun(RecordPrefix, Rest, [S | Acc]).

%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

%% Make sure you have run reset_db.sh with the test/test_data.sql seed file

first_test() ->
    application:ensure_all_started(logger),
    logger:set_primary_config(level, info),
    ok.

read_database_test() ->
    ?LOG_INFO("====================== read_database_test() END ======================"),

    {ok, C} = epgsql:connect(#{host => "localhost",
                               port => 5432,
                               username => "proto_crudl",
                               password => "proto_crudl",
                               database => "proto_crudl",
                               timeout => 4000}),

    {ok, Database} = read_database(C, [{schemas, ["public", "test_schema"]},
                                       {excluded, ["public.excluded", "spatial_ref_sys"]}]),
    TablesDict = Database#database.tables,

    Size = dict:size(TablesDict),
    ?assertEqual(14, Size),

    error = dict:find(<<"public.exclude">>, TablesDict),
    error = dict:find(<<"public.spatial_ref_sys">>, TablesDict),

    {ok, Table} = dict:find(<<"test_schema.user">>, TablesDict),

    ?assertEqual(<<"user">>, Table#table.name),
    ?assertEqual(<<"test_schema">>, Table#table.schema),

    ?assertEqual([<<"user_id">>], Table#table.pkey_list),

    % The columns list
    ExpectedList = [<<"aka_id">>, <<"created_on">>, <<"due_date">>, <<"email">>,
                    <<"enabled">>, <<"first_name">>, <<"geog">>, <<"last_name">>,
                    <<"my_array">>, <<"number_value">>, <<"pword_hash">>,
                    <<"updated_on">>, <<"user_id">>, <<"user_state">>, <<"user_token">>,
                    <<"user_type">>],
    KeyList = orddict:fetch_keys(Table#table.columns),
    ?assertEqual(ExpectedList, KeyList),

    % Test columns
    {column, <<"user">>, <<"test_schema">>, <<"aka_id">>, 9,
     <<"bigint">>, <<"bigint">>, null, true, false, false,
     false, false, false, false, undefined, undefined, undefined,
     undefined, []} = orddict:fetch(<<"aka_id">>, Table#table.columns),

    {column, <<"user">>, <<"test_schema">>, <<"email">>, 4,
     <<"character varying">>, <<"character varying">>, null, false,
     false, false, false, false, false, false, undefined, undefined, undefined,
     undefined, []} = orddict:fetch(<<"email">>, Table#table.columns),

    {column,<<"user">>,<<"test_schema">>,
     <<"user_type">>,11,<<"character varying">>,
     <<"character varying">>,null,false,false, false,false,false,false,false,undefined, undefined,undefined,undefined,
     [<<"BIG SHOT">>,<<"LITTLE-SHOT">>,
      <<"BUSY_GUY">>,<<"BUSYGAL">>,<<"123FUN">>]} = orddict:fetch(<<"user_type">>, Table#table.columns),

    % Indexes
    ?assertEqual({index, <<"user">>, <<"test_schema">>,
                  <<"list_by_name">>, non_unique,
                  [<<"first_name">>, <<"last_name">>],
                  true, false, null}, lookup_index(<<"list_by_name">>, Table#table.indexes)),

    #index{table_name = <<"user">>, table_schema = <<"test_schema">>, name = <<"pk_user">>,
           type       = primary_key, columns = [<<"user_id">>], is_list = false, is_lookup = false,
           comment    = null} = lookup_index(<<"pk_user">>, Table#table.indexes),

    % Relationship
    [FR] = Table#table.relations,
    [FC] = FR#foreign_relation.foreign_columns,
    ?LOG_INFO("FR=~p, local_name=~p", [FR, FC#foreign_column.local_name]),
    ?assertEqual([#foreign_relation{foreign_schema  = <<"test_schema">>, foreign_table = <<"user">>,
                                    foreign_columns = [#foreign_column{foreign_name     = <<"user_id">>,
                                                                       local_name       = <<"aka_id">>,
                                                                       ordinal_position = 1}],
                                    relation_type   = undefined}], Table#table.relations),

    % Now test the lists (not reversed)
    ?assertEqual([<<"user_id">>, <<"first_name">>, <<"last_name">>, <<"email">>,
                  <<"geog">>, <<"pword_hash">>, <<"user_token">>, <<"enabled">>,
                  <<"aka_id">>, <<"my_array">>, <<"user_type">>,
                  <<"number_value">>, <<"created_on">>, <<"updated_on">>,
                  <<"due_date">>, <<"user_state">>], Table#table.select_list),

    % sequence should be excluded
    ?assertEqual([<<"first_name">>, <<"last_name">>, <<"email">>, <<"geog">>, <<"pword_hash">>,
                  <<"user_token">>, <<"enabled">>, <<"aka_id">>, <<"my_array">>, <<"user_type">>,
                  <<"number_value">>, <<"created_on">>, <<"updated_on">>, <<"due_date">>, <<"user_state">>], Table#table.insert_list),

    % primary key should be excluded
    ?assertEqual([<<"first_name">>, <<"last_name">>, <<"email">>, <<"geog">>, <<"pword_hash">>,
                  <<"user_token">>, <<"enabled">>, <<"aka_id">>, <<"my_array">>, <<"user_type">>,
                  <<"number_value">>, <<"created_on">>, <<"updated_on">>, <<"due_date">>, <<"user_state">>], Table#table.update_list),

    ?assertEqual(<<"user_id">>, Table#table.sequence),

    ?assertEqual([<<"user_token">>, <<"enabled">>,<<"created_on">>], Table#table.default_list),

    UserToken = orddict:fetch(<<"user_token">>, Table#table.columns),
    ?assertEqual(<<"uuid_generate_v1()">>, UserToken#column.default),

    UserType = orddict:fetch(<<"user_type">>, Table#table.columns),
    ?assertEqual( [<<"BIG SHOT">>,<<"LITTLE-SHOT">>,<<"BUSY_GUY">>,
                   <<"BUSYGAL">>,<<"123FUN">>], UserType#column.valid_values),
    ?assertEqual(true, Table#table.has_valid_values),

    {ok, Table2} = dict:find(<<"public.example_b">>, TablesDict),
    ?assertEqual(<<"public">>, Table2#table.schema),
    ?assertEqual(<<"example_b">>, Table2#table.name),

    ok = epgsql:close(C),

    ?LOG_INFO("====================== read_database_test() END ======================"),
    ok.


lookup_index(_Name, []) ->
    notfound;
lookup_index(Name, [Idx = #index{name = Name} | _Rest]) ->
    Idx;
lookup_index(Name, [_Head | Rest]) ->
    lookup_index(Name, Rest).

cleanup_version(_C, []) ->
    ok;
cleanup_version(C, [{_Key, T0} | Rest]) ->
    Alter = "ALTER TABLE " ++ binary_to_list(T0#table.schema) ++ "." ++ binary_to_list(T0#table.name) ++
                                                                        " DROP COLUMN version",
    case epgsql:squery(C, Alter) of
        {ok, _Fields, _Rows} ->
            cleanup_version(C, Rest);
        {error, Reason} ->
            io:format("    WARNING: Failed to get alter table ~p.~p. Reason=~p, Stmt=~p~n",
                      [T0#table.schema, T0#table.name, Reason, Alter]),
            ok
    end.

last_test() ->
    ?LOG_INFO("====================== CLEANING UP VERSION COLUMNS ======================"),

    {ok, C} = epgsql:connect(#{host => "localhost",
                               port => 5432,
                               username => "proto_crudl",
                               password => "proto_crudl",
                               database => "proto_crudl",
                               timeout => 4000}),

    {ok, Database} = proto_crudl_psql:read_database(C, [{schemas, ["public", "test_schema"]},
                                                        {excluded, ["public.excluded", "spatial_ref_sys"]}]),

    TablesDict = Database#database.tables,
    ok = cleanup_version(C, dict:to_list(TablesDict)),
    ok = epgsql:close(C).

-endif.
