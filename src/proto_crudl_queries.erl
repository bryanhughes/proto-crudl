%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 04. Oct 2021 10:09 AM
%%%-------------------------------------------------------------------
-module(proto_crudl_queries).
-author("bryan").

%% API
-export([build_queries/2, maybe_expand_sql/2]).

-include("proto_crudl.hrl").

%% @doc This function will build all the CRUDL and other default queries for each table
build_queries([], Acc) ->
    Acc;
build_queries([{Key, T = #table{schema = S, name = N, query_dict = QD0, columns = ColDict}} | Rest], Acc) ->
    Schema = proto_crudl_utils:to_string(S),
    Name = proto_crudl_utils:to_string(N),
    RecordName = Schema ++ "." ++ proto_crudl_utils:camel_case(Name),

    % CREATE
    InsertSql = build_insert_sql(Schema, Name, T),
    {ok, IQ, IP, IIP, IR, IM} = proto_crudl_parse:parse_query(RecordName, InsertSql, ColDict),
    QD1 = orddict:store("insert", #query{name      = "INSERT",
                                         fun_name  = "create", fun_args = "1",
                                         query     = IQ,
                                         in_params = IIP, bind_params = IP,
                                         record    = IR, map = IM}, QD0),

    % NEW
    QD1A = orddict:store("new", #query{name      = "NEW",
                                       fun_name  = "new", fun_args = integer_to_list(length(IIP)),
                                       in_params = IIP, bind_params = IP,
                                       record    = IR, map = IM}, QD1),



    InsertDefaultSql = build_insert_defaults_sql(Schema, Name, T),
    {ok, IDQ, IDP, IDIP, IDR, IDM} = proto_crudl_parse:parse_query(RecordName, InsertDefaultSql, ColDict),
    QD2 = case IDQ =:= IQ of
              true ->
                  QD1A;
              _ ->
                  orddict:store("insert_defaults", #query{name      = "INSERT_DEFAULTS",
                                                          fun_name  = "create_default", fun_args = "1",
                                                          query     = IDQ,
                                                          in_params = IDIP, bind_params = IDP,
                                                          record    = IDR, map = IDM}, QD1A)
          end,

    % NEW FEATURE: UPSERT
    QD2A = case build_upsert_sql(Schema, Name, T) of
               "" ->
                   QD2;
               UpsertSql ->
                   {ok, UpQ, UpP, UpIP, UpR, UpM} = proto_crudl_parse:parse_query(RecordName, UpsertSql, ColDict),
                   orddict:store("upsert", #query{name      = "UPSERT",
                                                  fun_name  = "upsert", fun_args = "1",
                                                  query     = UpQ,
                                                  in_params = UpIP, bind_params = UpP,
                                                  record    = UpR, map = UpM}, QD2)
    end,

    % If no primary keys, then no read, update or delete
    QD5 = case T#table.pkey_list of
              [] ->
                  QD2A;
              _ ->
                  % READ
                  SelectSql = build_select_sql(Schema, Name, T),
                  {ok, SQ, SP, SIP, SR, SM} = proto_crudl_parse:parse_query(RecordName, SelectSql, ColDict),
                  QD3 = orddict:store("select", #query{name      = "SELECT",
                                                       fun_name  = "read", fun_args = integer_to_list(length(SIP)),
                                                       query     = SQ,
                                                       in_params = SIP, bind_params = SP,
                                                       record    = SR, map = SM}, QD2A),

                  % UPDATE
                  UpdateSql = build_update_sql(Schema, Name, T),
                  {ok, UQ, UP, UIP, UR, UM} = proto_crudl_parse:parse_query(RecordName, UpdateSql, ColDict),
                  QD4 = orddict:store("update", #query{name      = "UPDATE",
                                                       fun_name  = "update", fun_args = "1",
                                                       query     = UQ,
                                                       in_params = UIP, bind_params = UP,
                                                       record    = UR, map = UM}, QD3),

                  % DELETE
                  DeleteSql = build_delete_sql(Schema, Name, T),
                  {ok, DQ, DP, DIP, DR, DM} = proto_crudl_parse:parse_query(RecordName, DeleteSql, ColDict),
                  orddict:store("delete", #query{name      = "DELETE",
                                                 fun_name  = "delete", fun_args = integer_to_list(length(DIP)),
                                                 query     = DQ,
                                                 in_params = DIP, bind_params = DP,
                                                 record    = DR, map = DM}, QD4)
          end,


    Fun = fun(#foreign_relation{constraint_name = ConstraintName, foreign_columns = FCols}, QD) ->
        UpdateFkeySql = build_update_fkey_sql(Schema, Name, T, FCols),
        {ok, UFQ, UFP, UFIP, UFR, UFM} = proto_crudl_parse:parse_query(RecordName, UpdateFkeySql, ColDict),
        CN = proto_crudl_utils:to_string(ConstraintName),
        orddict:store(ConstraintName, #query{name      = "UPDATE_" ++ string:to_upper(CN),
                                             fun_name  = "update_" ++ string:to_lower(CN), fun_args = integer_to_list(length(UFIP)),
                                             query     = UFQ,
                                             in_params = UFIP, bind_params = UFP,
                                             record    = UFR, map = UFM}, QD)
          end,
    QD6 = lists:foldl(Fun, QD5, T#table.relations),

    SelectLimitSql = build_select_with_limit_sql(Schema, Name, T),
    {ok, SLQ, SLP, SLIP, SLR, SLM} = proto_crudl_parse:parse_query(RecordName, SelectLimitSql, ColDict),
    QD7 = orddict:store("select_limit", #query{name     = "SELECT_LIMIT",
                                               fun_name = "list", fun_args = integer_to_list(length(SLIP)),
                                               query    = SLQ,
                                               in_params = SLIP, bind_params = SLP,
                                               record = SLR, map = SLM}, QD6),

    % LIST/LOOKUP
    QD8 = build_lookup_list_query(T, QD7),
    build_queries(Rest, [{Key, T#table{query_dict = QD8}} | Acc]).


-spec rewrite_xform_fun(update | insert, orddict:dict(), list(), binary()) -> string().
rewrite_xform_fun(Which, ColDict, ColumnList, Operator) ->
    case erl_scan:string(Operator) of
        {ok, Tokens, _} ->
            % The expected cases are: [$column, ...], func($column1, ...)], [func()], [func(func($column1, ...))]
            logger:info("Tokens=~0p, SelectList=~0p", [Tokens, ColumnList]),
            rewrite_func_params(Which, ColDict, ColumnList, Operator, Tokens);
        {error, Reason} ->
            io:format("ERROR: Failed to parse transform function. Fun=~p, Reason=~p", [Operator, Reason]),
            failed
    end.

-spec rewrite_func_params(update | insert, orddict:dict(), list(), string(), list()) -> string().
rewrite_func_params(_Which, _ColDict, _ColumnList, Fun, []) ->
    Fun;
rewrite_func_params(Which, ColDict, ColumnList, Fun, [{char, 1, Chr}, {atom, 1, Remaining} | Rest]) ->
    % We have the parameter name
    Name = proto_crudl_utils:to_binary(lists:flatten([Chr, atom_to_list(Remaining)])),
    case lists:member(Name, ColumnList) of
        true ->
            rewrite_func_params(Which, ColDict, ColumnList, Fun, Rest);
        false ->
            io:format("    ERROR: Failed to find ~p in column list ~p (~p)~n", [Name, ColumnList, Which]),
            erlang:error(failed_rewrite)
    end;
rewrite_func_params(Which, ColDict, ColumnList, Fun, [{char, 1, Chr} | Rest]) ->
    Name = proto_crudl_utils:to_binary(lists:flatten([Chr])),
    case lists:member(Name, ColumnList) of
        true ->
            rewrite_func_params(Which, ColDict, ColumnList, Fun, Rest);
        false ->
            io:format("    ERROR: Failed to find ~p in column list ~p (~p)~n", [Name, ColumnList, Which]),
            erlang:error(failed_rewrite)
    end;
rewrite_func_params(Which, ColDict, ColumnList, Fun, [_H | Rest]) ->
    rewrite_func_params(Which, ColDict, ColumnList, Fun, Rest).

maybe_expand_sql(#table{columns = ColDict, select_list = SelectList}, Query) ->
    case string:str(Query, "*") > 0 of
        true ->
            Clause1 = build_select_clause(SelectList, ColDict, []),
            Clause2 = build_select_xforms(SelectList, ColDict, []),
            Expanded = lists:join(", ", lists:append(Clause1, Clause2)),
            lists:flatten(string:replace(Query, "*", Expanded));
        _ ->
            Query
    end.

%% ---- INSERT / CREATE ----

-spec build_insert_sql(string(), string(), #table{}) -> InsertStmt :: string().
%% @doc This function will return the INSERT statement based on the applied excluded columns and transformations
build_insert_sql(S, N, Table) ->
    ColDict = Table#table.columns,
    SelectList = Table#table.select_list,
    InsertList = Table#table.insert_list,

    {Clause, Params} = build_insert_clause(InsertList, ColDict, [], []),

    % Now add the insert transforms
    ColumnList = orddict:fetch_keys(ColDict),

    {Clause1, Params1} = build_insert_xforms(ColDict, ColumnList, InsertList, [], []),

    Clause2 = lists:append(Clause, Clause1),
    Params2 = lists:append(Params, Params1),

    % We need our update to return the record...
    RetClause1 = build_select_clause(SelectList, ColDict, []),
    RetClause2 = build_select_xforms(SelectList, ColDict, []),

    % Now, we need our to add any transforms to the values. For example, the column geog would have an input
    % that would include the lat, lng and then the output of the geog with a function that references those inputs.

    logger:info("Clause=~p, Params=~p", [Clause2, Params2]),
    lists:flatten("INSERT INTO " ++ S ++ "." ++ N ++ " (" ++ lists:join(", ", Clause2) ++ ") VALUES (" ++ lists:join(", ", Params2) ++ ")" ++
                  " RETURNING " ++ lists:join(", ", lists:append(RetClause1, RetClause2))).


build_insert_clause([], _ColDict, ClauseAcc, ParamsAcc) ->
    {lists:reverse(ClauseAcc), lists:reverse(ParamsAcc)};
build_insert_clause([ColumnName | Rest], ColDict, CAcc, PAcc) ->
    Column = orddict:fetch(ColumnName, ColDict),
    case {Column#column.is_sequence, Column#column.data_type, Column#column.is_version} of
        {true, _, _} ->
            build_insert_clause(Rest, ColDict, CAcc, PAcc);
        {false, <<"virtual">>, _} ->
            build_insert_clause(Rest, ColDict, CAcc, PAcc);
        {false, _, true} ->
            build_insert_clause(Rest, ColDict, [proto_crudl_utils:to_string(ColumnName) | CAcc], ["0" | PAcc]);
        {false, _, _} ->
            Name = proto_crudl_utils:to_string(ColumnName),
            build_insert_clause(Rest, ColDict, [Name | CAcc], ["$" ++ Name | PAcc])
    end.


build_insert_xforms(_ColDict, [], _SelectList, Clause, Params) ->
    {lists:reverse(Clause), lists:reverse(Params)};
build_insert_xforms(ColDict, [ColumnName | Rest], SelectList, Clause, Params) ->
    Column = orddict:fetch(ColumnName, ColDict),
    case Column#column.insert_xform of
        undefined ->
            build_insert_xforms(ColDict, Rest, SelectList, Clause, Params);
        Operation ->
            % Need to map the columns to the select params
            NewFun = rewrite_xform_fun(insert, ColDict, SelectList, proto_crudl_utils:to_string(Operation)),
            build_insert_xforms(ColDict, Rest, SelectList,
                                [proto_crudl_utils:to_string(ColumnName) | Clause],
                                [NewFun | Params])
    end.


-spec build_upsert_sql(string(), string(), #table{}) -> UpsertStmt :: string().
%% @doc This function will return the UPSERT statement based on the applied excluded columns and transformations. If
%%      there are no upserts defined for this table, an empty string is returned.
%%
%%      We need to handle xforms on the UPDATE statement
%%
%%      -define(UPSERT, "INSERT INTO test_schema.address (address1, address2, city, state, country, postcode, geog, version)
%%              VALUES ($1, $2, $3, $4, $5, $6, geog = ST_POINT($7, $8)::geography, 0)
%%              ON CONFLICT ON CONSTRAINT unq_address
%%              DO UPDATE test_schema.address SET address1 = EXCLUDED.address1, address2 = EXCLUDED.address2,
%%                                                city = EXCLUDED.city, state = EXCLUDED.state, country = EXCLUDED.country,
%%                                                postcode = EXCLUDED.postcode, geog = EXCLUDED.geog,
%%                                                version = EXCLUDED.version + 1
%%              RETURNING address_id, address1, address2, city, state, country, postcode, version").
build_upsert_sql(S, N, Table) ->
    case Table#table.upsert_constraint of
        undefined ->
            "";
        {UpsertConst, Action} ->
            ColDict = Table#table.columns,
            SelectList = Table#table.select_list,
            InsertList = Table#table.insert_list,

            {Clause, Params} = build_insert_clause(InsertList, ColDict, [], []),

            % Now add the upsert transforms
            ColumnList = orddict:fetch_keys(ColDict),

            {Clause1, Params1} = build_insert_xforms(ColDict, ColumnList, InsertList, [], []),

            Clause2 = lists:append(Clause, Clause1),
            Params2 = lists:append(Params, Params1),

            % We need our update to return the record...
            RetClause1 = build_select_clause(SelectList, ColDict, []),
            RetClause2 = build_select_xforms(SelectList, ColDict, []),

            % Now, we need our to add any transforms to the values. For example, the column geog would have an input
            % that would include the lat, lng and then the output of the geog with a function that references those inputs.

            DoUpdateClause = build_upsert_doupdate_sql(Action),

            logger:info("Clause=~p, Params=~p", [Clause2, Params2]),
            lists:flatten("INSERT INTO " ++ S ++ "." ++ N ++ " (" ++ lists:join(", ", Clause2) ++ ") "
                            "VALUES (" ++ lists:join(", ", Params2) ++ ") " ++
                            "ON CONFLICT ON CONSTRAINT " ++ proto_crudl_utils:to_string(UpsertConst) ++ " " ++
                            "DO " ++ DoUpdateClause ++ " " ++
                            "RETURNING " ++ lists:join(", ", lists:append(RetClause1, RetClause2)))
    end.

build_upsert_doupdate_sql(do_nothing) ->
    "NOTHING";
build_upsert_doupdate_sql({do_update, ColList}) ->
    UpdateList = [proto_crudl_utils:to_string(ColName) || ColName <- ColList],
    lists:flatten("UPDATE SET " ++ lists:join(", ", [ColName ++ " = EXCLUDED." ++
                                    ColName || ColName <- UpdateList]) ++ ", version = EXCLUDED.version + 1").

-spec build_insert_defaults_sql(Schema :: binary(), Name :: binary(), Table :: #table{}) -> InsertStmt :: string().
%% @doc This function will return the INSERT_DEFAULTS statement based on the applied excluded columns and transformations
build_insert_defaults_sql(Schema, Name, #table{columns = ColDict,
                                               insert_list = InsertList,
                                               select_list = SelectList,
                                               default_list = DefaultList}) ->
    IList = [C || C <- InsertList, lists:member(C, DefaultList) == false],

    logger:info("Schema=~p, Table=~p, IList=~p", [Schema, Name, IList]),

    {Clause, Params} = build_insert_clause(IList, ColDict, [], []),

    % Now add the insert transforms
    ColumnList = orddict:fetch_keys(ColDict),
    {Clause1, Params1} = build_insert_xforms(ColDict, ColumnList, IList, [], []),

    Clause2 = lists:append(Clause, Clause1),
    Params2 = lists:append(Params, Params1),

    % We need our update to return the record...
    RetClause1 = build_select_clause(SelectList, ColDict, []),
    RetClause2 = build_select_xforms(SelectList, ColDict, []),

    % Now, we need our to add any transforms to the values. For example, the column geog would have an input
    % that would include the lat, lng and then the output of the geog with a function that references those inputs.

    lists:flatten("INSERT INTO " ++ Schema ++ "." ++ Name ++ " (" ++ lists:join(", ", Clause2) ++ ") VALUES (" ++ lists:join(", ", Params2) ++ ")" ++
                  " RETURNING " ++ lists:join(", ", lists:append(RetClause1, RetClause2))).

%% ---- SELECT / READ ----

-spec build_select_with_limit_sql(Schema :: string(), Name :: string(), Table :: #table{}) -> SelectStmt :: string().
%% @doc This function will return the SELECT WITH LIMIT statement based on the applied excluded columns and transformations
build_select_with_limit_sql(Schema, Name, #table{columns = ColDict, select_list = SelectList, comment = Comment}) ->
    logger:info("Schema=~p, Table=~p, Comment=~p", [Schema, Name, Comment]),
    C0 = proto_crudl_utils:to_string(Comment),
    OrderBy = case string:substr(C0, 1, 9) of
                  "+ORDER BY" ->
                      case string:split(string:substr(C0, 2), "\n") of
                          [] -> "";
                          [Head | _Rest] -> Head
                      end;
                  _ ->
                      ""
              end,
    io:format("--------- Table=~p, C0=~p, OrderBy=~p\n", [Name, C0, OrderBy]),

    % Now build our select clause using all the columns except those that are excluded
    Clause1 = build_select_clause(SelectList, ColDict, []),
    Clause2 = build_select_xforms(SelectList, ColDict, []),
    lists:flatten("SELECT " ++ lists:join(", ", lists:append(Clause1, Clause2)) ++
                  " FROM " ++ Schema ++ "." ++ Name ++ " " ++ OrderBy ++ " " ++ "LIMIT $limit OFFSET $offset").


-spec build_select_sql(Schema :: string(), Name :: string(), Table :: #table{}) -> SelectStmt :: string().
%% @doc This function will return the SELECT statement based on the applied excluded columns and transformations
build_select_sql(Schema, Name, Table) ->
    ColDict = Table#table.columns,
    SelectList = Table#table.select_list,
    logger:info("Schema=~p, Table=~p", [Schema, Name]),
    case Table#table.pkey_list of
        [] ->
            "";
        PKColumns ->
            PKColumns1 = build_bind_params(PKColumns, []),
            % Now build our select clause using all the columns except those that are excluded
            Clause1 = build_select_clause(SelectList, ColDict, []),
            Clause2 = build_select_xforms(SelectList, ColDict, []),
            lists:flatten("SELECT " ++ lists:join(", ", lists:append(Clause1, Clause2)) ++
                          " FROM " ++ Schema ++ "." ++ Name ++
                          " WHERE " ++ lists:join(" AND ", PKColumns1))
    end.

build_select_clause([], _ColDict, Acc) ->
    lists:reverse(Acc);
build_select_clause([ColumnName | Rest], ColDict, Acc) ->
    Column = orddict:fetch(ColumnName, ColDict),
    case Column#column.data_type of
        <<"virtual">> ->
            build_select_clause(Rest, ColDict, Acc);
        _ ->
            build_select_clause(Rest, ColDict, [proto_crudl_utils:to_string(ColumnName) | Acc])
    end.


build_select_xforms([], _ColDict, Acc) ->
    lists:reverse(Acc);
build_select_xforms([ColumnName | Rest], ColDict, Acc) ->
    Column = orddict:fetch(ColumnName, ColDict),
    case Column#column.select_xform of
        undefined ->
            build_select_xforms(Rest, ColDict, Acc);
        Operation ->
            build_select_xforms(Rest, ColDict,
                                [proto_crudl_utils:to_string(Operation) ++ " AS " ++ proto_crudl_utils:to_string(ColumnName) | Acc])
    end.

build_bind_params([], Acc) ->
    lists:reverse(Acc);
build_bind_params([Name | Rest], Acc) ->
    N = proto_crudl_utils:to_string(Name),
    build_bind_params(Rest, [N ++ " = $" ++ N | Acc]).


%% ---- UPDATE ----

build_update_fkey_sql(S, N, T, FCols) ->
    ColDict = T#table.columns,
    SelectList = T#table.select_list,
    BindParams = build_bind_params(T#table.pkey_list, []),

    Clause0 = [proto_crudl_utils:to_string(LN) ++ " = $" ++ proto_crudl_utils:to_string(LN) || #foreign_column{local_name = LN} <- FCols],

    Clause1 = case T#table.version_column of
        undefined ->
            Clause0;
        VersionColumn ->
            VC = proto_crudl_utils:to_string(VersionColumn),
            [VC ++ " = " ++ VC ++ " + 1" | Clause0]
    end,

    % We need our update to return the record...
    RetClause1 = build_select_clause(SelectList, ColDict, []),
    RetClause2 = build_select_xforms(SelectList, ColDict, []),
    lists:flatten("UPDATE " ++ S ++ "." ++ N ++ " SET " ++ lists:join(", ", Clause1) ++ " WHERE " ++ lists:join(" AND ", BindParams) ++
                  " RETURNING " ++ lists:join(", ", lists:append(RetClause1, RetClause2))).

-spec build_update_sql(Schema :: binary(), Name :: binary(), Table :: #table{}) -> UpdateStmt :: string().
%% @doc This function will return the UPDATE statement based on the applied excluded columns and transformations
build_update_sql(Schema, Name, Table) ->
    ColDict = Table#table.columns,
    SelectList = Table#table.select_list,
    UpdateList = Table#table.update_list,

    PKColumns1 = build_bind_params(Table#table.pkey_list, []),

    Clause = build_update_clause(UpdateList, ColDict, []),

    ColumnList = orddict:fetch_keys(ColDict),
    Clause1 = build_update_xforms(ColDict, ColumnList, SelectList, []),
    Clause2 = lists:append(Clause, Clause1),

    % We need our update to return the record...
    RetClause1 = build_select_clause(SelectList, ColDict, []),
    RetClause2 = build_select_xforms(SelectList, ColDict, []),

    WhereClause = case Table#table.version_column of
                      <<>> ->
                          PKColumns1;
                      VersionColumn ->
                          VC = proto_crudl_utils:to_string(VersionColumn),
                          lists:append(PKColumns1, [VC ++ " = $" ++ VC])
                  end,

    logger:info("Clause2=~p, PKColumns1=~p", [Clause2, PKColumns1]),
    lists:flatten("UPDATE " ++ Schema ++ "." ++ Name ++ " SET " ++ lists:join(", ", Clause2) ++ " WHERE " ++ lists:join(" AND ", WhereClause) ++
                  " RETURNING " ++ lists:join(", ", lists:append(RetClause1, RetClause2))).


build_update_clause([], _ColDict, ClauseAcc) ->
    lists:reverse(ClauseAcc);
build_update_clause([ColumnName | Rest], ColDict, CAcc) ->
    Cname = proto_crudl_utils:to_string(ColumnName),
    Column = orddict:fetch(ColumnName, ColDict),
    case {Column#column.is_sequence, Column#column.data_type, Column#column.is_pkey, Column#column.is_version} of
        {true, _, _, _} ->
            build_update_clause(Rest, ColDict, CAcc);
        {_, _, true, _} ->
            build_update_clause(Rest, ColDict, CAcc);
        {_, <<"virtual">>, _, _} ->
            build_update_clause(Rest, ColDict, CAcc);
        {_, _, _, true} ->
            build_update_clause(Rest, ColDict, [Cname ++ " = " ++ Cname ++ " + 1"  | CAcc]);
        {_, _, _, _} ->
            build_update_clause(Rest, ColDict, [Cname ++ " = $" ++ Cname | CAcc])
    end.

build_update_xforms(_ColDict, [], _ColumnList, Clause) ->
    lists:reverse(Clause);
build_update_xforms(ColDict, [ColumnName | Rest], ColumnList, Clause) ->
    Column = orddict:fetch(ColumnName, ColDict),
    case Column#column.update_xform of
        undefined ->
            build_update_xforms(ColDict, Rest, ColumnList, Clause);
        Operation ->
            % Need to map the columns to the select params
            NewFun = rewrite_xform_fun(update, ColDict, ColumnList, proto_crudl_utils:to_string(Operation)),
            Cname = proto_crudl_utils:to_string(ColumnName),
            build_update_xforms(ColDict, Rest, ColumnList, [Cname ++ " = " ++ NewFun])
    end.

%% ---- DELETE ----

-spec build_delete_sql(Schema :: binary(), Name :: binary(), Table :: #table{}) -> DeleteStmt :: string().
%% @doc This function will return the DELETE statement based on the applied excluded columns and transformations
build_delete_sql(Schema, Name, Table) ->
    logger:info("Schema=~p, Table=~p", [Schema, Name]),
    PKColumns = lists:reverse([Pkey || Pkey <- Table#table.pkey_list]),
    PKColumns1 = build_bind_params(PKColumns, []),
    lists:flatten("DELETE FROM " ++ Schema ++ "." ++ Name ++ " WHERE " ++ lists:join(" AND ", PKColumns1)).


%% ---- LIST ----

-spec build_lookup_list_query(Table :: #table{}, QueryDictIn :: dict:dict()) -> QueryDict::dict:dict().
%% @doc This function will return a list of LOOKUP statements based on the applied excluded columns and transformations
build_lookup_list_query(Table, QueryDictIn) ->
    IndexList = lists:reverse(Table#table.indexes),
    build_lookup_list_query(Table, IndexList, QueryDictIn).

build_lookup_list_query(_Table, [], QueryDict) ->
    QueryDict;
build_lookup_list_query(Table = #table{columns = ColDict}, [Index = #index{is_list = List, is_lookup = Lookup} | Rest], QueryDict) when List =:= true orelse Lookup =:= true  ->
    % Its a list index
    Schema = proto_crudl_utils:to_string(Index#index.table_schema),
    Name = proto_crudl_utils:to_string(Index#index.table_name),
    RecordName = Schema ++ "." ++ proto_crudl_utils:camel_case(Name),
    LookupName = proto_crudl_utils:to_string(Index#index.name),
    LookupSql = build_lookup_list_sql(Table, Index),
    {ok, Q, P, IP, R, M} = proto_crudl_parse:parse_query(RecordName, LookupSql, ColDict),
    Args = length(IP),
    Query = #query{name     = string:to_upper(LookupName),
                   fun_name = string:to_lower(LookupName),
                   fun_args = integer_to_list(Args),
                   query    = Q, record = R, map = M, bind_params = P, in_params = IP},
    build_lookup_list_query(Table, Rest, orddict:store(Index#index.name, Query, QueryDict));
build_lookup_list_query(Table, [_Index | Rest], QueryDict) ->
    build_lookup_list_query(Table, Rest, QueryDict).

build_lookup_list_sql(Table, Index = #index{is_lookup = IsLookup, comment = Comment}) ->
    Schema = proto_crudl_utils:to_string(Index#index.table_schema),
    Name = proto_crudl_utils:to_string(Index#index.table_name),
    ColDict = Table#table.columns,
    SelectList = Table#table.select_list,
    LookupColumns = Index#index.columns,

    C = proto_crudl_utils:to_string(Comment),
    OrderBy = case string:substr(C, 1, 9) of
                  "+ORDER BY" ->
                      case string:split(string:substr(C, 2), "\n") of
                          [] -> "";
                          [Head | _Rest] -> Head
                      end;
                  _ ->
                      ""
              end,

    LookupColumns1 = build_bind_params(LookupColumns, []),

    % Now build our select clause using all the columns except those that are excluded
    Clause1 = build_select_clause(SelectList, ColDict, []),
    Clause2 = build_select_xforms(SelectList, ColDict, []),

    LimitClause = case IsLookup of true -> ""; _ -> " LIMIT $limit OFFSET $offset" end,

    lists:flatten("SELECT " ++ lists:join(", ", lists:append(Clause1, Clause2)) ++
                  " FROM " ++ proto_crudl_utils:to_string(Schema) ++ "." ++ proto_crudl_utils:to_string(Name) ++
                  " WHERE " ++ lists:join(" AND ", LookupColumns1) ++ " " ++ OrderBy ++ " " ++ LimitClause).


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
    ok.

define_test() ->
    ?LOG_INFO("====================== define_test() START ======================"),

    {ok, C} = epgsql:connect(#{host => "localhost",
                               port => 5432,
                               username => "proto_crudl",
                               password => "proto_crudl",
                               database => "proto_crudl",
                               timeout => 4000}),

    {ok, Database} = proto_crudl_psql:read_database(C, [{schemas, ["public", "test_schema"]},
                                                        {excluded, ["public.excluded", "spatial_ref_sys"]}]),

    Configs = [
        {transforms, [
            {"test_schema.user", [
                % For the select transform, we need to know the datatype of the product of the transform. This is needed for
                % generating the protobufs
                {select, [{"lat", "decimal", "ST_Y(geog::geometry)"},
                          {"lon", "decimal", "ST_X(geog::geometry)"}]},
                {insert, [{"geog", "geography", "ST_POINT($lon, $lat)::geography"}]},
                {update, [{"geog", "geography", "ST_POINT($lon, $lat)::geography"}]}]},
            {"public.foo", [
                {select, [{"foobar", "integer", "1"}]}]}
        ]},
        {exclude_columns, [
            {"test_schema.user", ["pword_hash", "geog"]}
                          ]},
        {mapping, [
            {"test_schema.user", [
                {get_pword_hash, "SELECT pword_hash FROM test_schema.user WHERE email = $email"},
                {update_pword_hash, "UPDATE test_schema.user SET pword_hash = $pword_hash WHERE email = $email AND version = $version"},
                {reset_pword_hash, "UPDATE test_schema.user SET pword_hash = NULL WHERE email = $email AND version = $version"},
                {disable_user, "UPDATE test_schema.user SET enabled = false WHERE email = $email"},
                {enable_user, "UPDATE test_schema.user SET enabled = true WHERE email = $email"},
                {delete_user_by_email, "DELETE FROM test_schema.user WHERE email = $email"},
                {set_token, "UPDATE test_schema.user SET user_token = uuid_generate_v4() WHERE user_id = $user_id AND version = $version RETURNING user_token, version"},
                {find_nearest, "SELECT *, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon FROM test_schema.user "
                               "WHERE ST_DWithin(geog, Geography(ST_MakePoint($lon, $lat)), $radius) "
                               "ORDER BY geog <-> ST_POINT($lon, $lat)::geography"}
            ]}
                  ]}],

    {ok, Database1} = proto_crudl:process_configs(C, Configs, Database),
    TablesDict = Database1#database.tables,
    {ok, UserTable} = dict:find(<<"test_schema.user">>, TablesDict),

    S = proto_crudl_utils:to_string(UserTable#table.schema),
    N = proto_crudl_utils:to_string(UserTable#table.name),

    SelectList = UserTable#table.select_list,
    ?LOG_INFO("SelectList=~p", [SelectList]),
    ?assertEqual([<<"user_id">>,<<"first_name">>,<<"last_name">>,<<"email">>,
                  <<"user_token">>,<<"enabled">>,<<"aka_id">>,<<"my_array">>,
                  <<"user_type">>,<<"number_value">>,<<"created_on">>,
                  <<"updated_on">>,<<"due_date">>,<<"user_state">>,<<"user_state_type">>, <<"lat">>,<<"lon">>], SelectList),

    ColumnList = orddict:fetch_keys(UserTable#table.columns),
    ?LOG_INFO("ColumnList=~p", [ColumnList]),
    ?assertEqual([<<"aka_id">>,<<"created_on">>,<<"due_date">>,<<"email">>,
                  <<"enabled">>,<<"first_name">>,<<"geog">>,<<"last_name">>,
                  <<"lat">>,<<"lon">>,<<"my_array">>,<<"number_value">>,
                  <<"pword_hash">>,<<"updated_on">>,<<"user_id">>,
                  <<"user_state">>,<<"user_state_type">>, <<"user_token">>,<<"user_type">>], ColumnList),

    SelectOutput = build_select_sql(S, N, UserTable),
    ?LOG_INFO("SelectOutput=~p~n", [SelectOutput]),
    SelectAssert = "SELECT user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon FROM test_schema.user WHERE user_id = $user_id",
    ?assertEqual(SelectAssert, SelectOutput),

    InsertOutput = build_insert_sql(S, N, UserTable),
    ?LOG_INFO("InsertOutput=~p~n", [InsertOutput]),
    InsertAssert = "INSERT INTO test_schema.user (first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, geog) VALUES ($first_name, $last_name, $email, $user_token, $enabled, $aka_id, $my_array, $user_type, $number_value, $created_on, $updated_on, $due_date, $user_state, $user_state_type, ST_POINT($lon, $lat)::geography) RETURNING user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon",
    ?assertEqual(InsertAssert, InsertOutput),

    UpdateOutput = build_update_sql(S, N, UserTable),
    ?LOG_INFO("UpdateOutput=~p~n", [UpdateOutput]),
    UpdateAssert = "UPDATE test_schema.user SET first_name = $first_name, last_name = $last_name, email = $email, user_token = $user_token, enabled = $enabled, aka_id = $aka_id, my_array = $my_array, user_type = $user_type, number_value = $number_value, created_on = $created_on, updated_on = $updated_on, due_date = $due_date, user_state = $user_state, user_state_type = $user_state_type, geog = ST_POINT($lon, $lat)::geography WHERE user_id = $user_id RETURNING user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon",
    ?assertEqual(UpdateAssert, UpdateOutput),

    DeleteOutput = build_delete_sql(S, N, UserTable),
    ?LOG_INFO("DeleteOutput=~p~n", [DeleteOutput]),
    DeleteAssert = "DELETE FROM test_schema.user WHERE user_id = $user_id",
    ?assertEqual(DeleteAssert, DeleteOutput),

    ExpandedOutput = maybe_expand_sql(UserTable, "SELECT * FROM test_schema.user"),
    ?LOG_INFO("ExpandedOutput=~p", [ExpandedOutput]),
    ExpandedAssert = "SELECT user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon FROM test_schema.user",
    ?assertEqual(ExpandedAssert, ExpandedOutput),

    ok = epgsql:close(C),
    ?LOG_INFO("====================== define_test() END ======================"),
    ok.

% SIDE-EFFECTS FROM ALTERING A TABLE, RUN THIS NEXT TO LAST
inject_version_test() ->
    ?LOG_INFO("====================== inject_version_test() START ======================"),

    {ok, C} = epgsql:connect(#{host => "localhost",
                               port => 5432,
                               username => "proto_crudl",
                               password => "proto_crudl",
                               database => "proto_crudl",
                               timeout => 4000}),

    {ok, Database} = proto_crudl_psql:read_database(C, [{schemas, ["public", "test_schema"]},
                                                        {excluded, ["public.excluded", "spatial_ref_sys"]}]),

    Configs = [
        {options, [{version_column, "version"}, indexed_lookups, check_constraints_as_enums]},
        {transforms, [
            {"test_schema.user", [
                % For the select transform, we need to know the datatype of the product of the transform. This is needed for
                % generating the protobufs
                {select, [{"lat", "decimal", "ST_Y(geog::geometry)"},
                          {"lon", "decimal", "ST_X(geog::geometry)"}]},
                {insert, [{"geog", "geography", "ST_POINT($lon, $lat)::geography"}]},
                {update, [{"geog", "geography", "ST_POINT($lon, $lat)::geography"}]}]},
            {"public.foo", [{select, [{"foobar", "integer", "1"}]}]}
        ]},
        {exclude_columns, [{"test_schema.user", ["pword_hash", "geog"]}]}],

    {ok, Database1} = proto_crudl:process_configs(C, Configs, Database),
    TablesDict = Database1#database.tables,
    {ok, UserTable} = dict:find(<<"test_schema.user">>, TablesDict),

    Version = orddict:fetch(<<"version">>, UserTable#table.columns),
    ?assertEqual(true, Version#column.is_version),

    ?assertEqual(<<"version">>, UserTable#table.version_column),

    SelectList = UserTable#table.select_list,
    ?LOG_INFO("SelectList=~p", [SelectList]),
    ?assertEqual([<<"user_id">>,<<"first_name">>,<<"last_name">>,<<"email">>,
                  <<"user_token">>,<<"enabled">>,<<"aka_id">>,<<"my_array">>,
                  <<"user_type">>,<<"number_value">>,<<"created_on">>,
                  <<"updated_on">>,<<"due_date">>,<<"user_state">>,<<"user_state_type">>,
                  <<"version">>,<<"lat">>,<<"lon">>], SelectList),

    ColumnList = orddict:fetch_keys(UserTable#table.columns),
    ?LOG_INFO("ColumnList=~p", [ColumnList]),
    ?assertEqual([<<"aka_id">>,<<"created_on">>,<<"due_date">>,<<"email">>,
                  <<"enabled">>,<<"first_name">>,<<"geog">>,<<"last_name">>,
                  <<"lat">>,<<"lon">>,<<"my_array">>,<<"number_value">>,
                  <<"pword_hash">>,<<"updated_on">>,<<"user_id">>,
                  <<"user_state">>,<<"user_state_type">>,<<"user_token">>,<<"user_type">>,<<"version">>], ColumnList),

    Version = orddict:fetch(<<"version">>, UserTable#table.columns),
    ?assertEqual(true, Version#column.is_version),

    ?assertEqual(<<"version">>, UserTable#table.version_column),

    S = proto_crudl_utils:to_string(UserTable#table.schema),
    N = proto_crudl_utils:to_string(UserTable#table.name),

    InsertOutput = build_insert_sql(S, N, UserTable),
    ?LOG_INFO("InsertOutput=~p~n", [InsertOutput]),
    InsertAssert = "INSERT INTO test_schema.user (first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, version, geog) VALUES ($first_name, $last_name, $email, $user_token, $enabled, $aka_id, $my_array, $user_type, $number_value, $created_on, $updated_on, $due_date, $user_state, $user_state_type, 0, ST_POINT($lon, $lat)::geography) RETURNING user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, version, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon",
    ?assertEqual(InsertAssert, InsertOutput),

    UpdateOutput = build_update_sql(S, N, UserTable),
    ?LOG_INFO("UpdateOutput=~p~n", [UpdateOutput]),
    UpdateAssert = "UPDATE test_schema.user SET first_name = $first_name, last_name = $last_name, email = $email, user_token = $user_token, enabled = $enabled, aka_id = $aka_id, my_array = $my_array, user_type = $user_type, number_value = $number_value, created_on = $created_on, updated_on = $updated_on, due_date = $due_date, user_state = $user_state, user_state_type = $user_state_type, version = version + 1, geog = ST_POINT($lon, $lat)::geography WHERE user_id = $user_id AND version = $version RETURNING user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, version, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon",
    ?assertEqual(UpdateAssert, UpdateOutput),

    UpdateFkeyOutput = build_update_fkey_sql(S, N, UserTable, [#foreign_column{local_name = <<"aka_id">>}]),
    UpdateFkeyAssert = "UPDATE test_schema.user SET version = version + 1, aka_id = $aka_id WHERE user_id = $user_id RETURNING user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, user_state_type, version, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon",
    ?assertEqual(UpdateFkeyAssert, UpdateFkeyOutput),

    ok = epgsql:close(C),

    ?LOG_INFO("====================== inject_version_test() END ======================"),
    ok.

cleanup_version(_C, []) ->
    ok;
cleanup_version(C, [{_Key, T0} | Rest]) ->
    Alter = "ALTER TABLE " ++ binary_to_list(T0#table.schema) ++ "." ++ binary_to_list(T0#table.name) ++ " DROP COLUMN version",
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
