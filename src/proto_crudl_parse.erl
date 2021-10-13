%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 01. Oct 2021 6:12 PM
%%%-------------------------------------------------------------------
-module(proto_crudl_parse).
-author("bryan").

%% API
-export([parse_query/3]).

-include("proto_crudl.hrl").

-spec parse_query(RecordName :: string(), QueryIn :: string(), ColDict :: orddict:orddict()) ->
    {ok, QueryOut :: string(), BindParams :: string(), InParams :: string(), Record :: string(), Map :: string()} | {error, Reason :: any()}.
parse_query(RecordName, QueryIn, ColDict) ->
    case erl_scan:string(QueryIn) of
        {ok, Tokens, _} ->
            logger:debug("Tokens=~p", [Tokens]),
            {QueryOut, BindParams, InParams, Record, Map} = parse_placeholders(ColDict, RecordName, Tokens, dict:new(),
                                                                               [], [], [], [], 1, QueryIn),
            logger:debug("BindParams=~p", [BindParams]),
            {ok, QueryOut, BindParams, InParams, Record, Map};
        {error, Reason} ->
            io:format("ERROR: Failed to parse Query=~p, Reason=~p", [QueryIn, Reason]),
            {error, Reason}
    end.

parse_placeholders(_ColDict, RecordName, [], _PosDict, BAcc, IAcc, RAcc, MAcc, _Cnt, NewQuery) ->
    {lists:flatten(NewQuery),
     lists:reverse(BAcc),
     lists:reverse(IAcc),
     lists:flatten("#'" ++ RecordName ++ "'{" ++ lists:join(", ", lists:reverse(RAcc)) ++ "}"),
     lists:flatten("#{" ++ lists:join(", ", lists:reverse(MAcc)) ++ "}")};
parse_placeholders(ColDict, RecordName, [{char, N, Chr}, {_, N, Remaining} | Rest], PosDict, BAcc, IAcc, RAcc, MAcc, Cnt, Query) ->
    FN = proto_crudl_utils:to_binary(lists:flatten([Chr, atom_to_list(Remaining)])),
    logger:debug("Remaining=~p, FN=~p", [Remaining, FN]),
    BindVar = proto_crudl_utils:camel_case(FN),
    logger:info("BindVar=~p, FN=~p", [BindVar, FN]),
    do_parse_placeholders(dict:find(FN, PosDict), PosDict, BindVar, ColDict, RecordName, Rest, FN, BAcc, IAcc, RAcc, MAcc, Cnt, Query);
parse_placeholders(ColDict, RecordName, [_Token | Rest], PosDict, BAcc, IAcc, RAcc, MAcc, Cnt, Query) ->
    logger:debug("_Token=~p", [_Token]),
    parse_placeholders(ColDict, RecordName, Rest, PosDict, BAcc, IAcc, RAcc, MAcc, Cnt, Query).

do_parse_placeholders(error, PosDict, BindVar, ColDict, RecordName, Rest, FN, BAcc, IAcc, RAcc, MAcc, Cnt, Query) ->
    NewQuery = string:replace(Query, "$" ++ proto_crudl_utils:to_string(FN), "$" ++ integer_to_list(Cnt)),
    case orddict:find(FN, ColDict) of
        {ok, Column} ->
            Mapped = fixup_param_mapping(FN, Column),
            RecordAssign = lists:flatten(proto_crudl_utils:to_string(FN) ++ " = " ++ BindVar),
            MapAssign = lists:flatten(proto_crudl_utils:to_string(FN) ++ " := " ++ BindVar),
            parse_placeholders(ColDict, RecordName, Rest, dict:store(FN, Cnt, PosDict), [Mapped | BAcc], [BindVar | IAcc], [RecordAssign | RAcc],
                               [MapAssign | MAcc], Cnt + 1, NewQuery);
        _ ->
            parse_placeholders(ColDict, RecordName, Rest, dict:store(FN, Cnt, PosDict), [BindVar | BAcc], [BindVar | IAcc], RAcc, MAcc, Cnt + 1, NewQuery)
    end;
do_parse_placeholders({ok, Pos}, PosDict, _BindVar, ColDict, RecordName, Rest, FN, BAcc, IAcc, RAcc, MAcc, Cnt, Query) ->
    NewQuery = string:replace(Query, "$" ++ proto_crudl_utils:to_string(FN), "$" ++ integer_to_list(Pos)),
    parse_placeholders(ColDict, RecordName, Rest, PosDict, BAcc, IAcc, RAcc, MAcc, Cnt, NewQuery).

fixup_param_mapping(FN, #column{data_type = <<"ARRAY">>}) ->
    "ensure_array(" ++ proto_crudl_utils:camel_case(FN) ++ ")";
fixup_param_mapping(FN, #column{valid_values = VV}) when length(VV) > 0 ->
    proto_crudl_utils:to_string(FN) ++ "_value(" ++ proto_crudl_utils:camel_case(FN) ++ ")";
fixup_param_mapping(FN, #column{udt_name = Udt}) ->
    Name = proto_crudl_utils:camel_case(FN),
    case proto_crudl_psql:is_timestamp(Udt) of
        true ->
            "ts_decode(" ++ Name ++ ")";
        false ->
            case proto_crudl_psql:is_date(Udt) of
                true ->
                    "date_decode(" ++ Name ++ ")";
                false ->
                    Name
            end
    end;
fixup_param_mapping(FN, _) ->
    proto_crudl_utils:camel_case(FN).

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

bind_param_test() ->
    ?LOG_INFO("====================== bind_param_test() START ======================"),

    % The input
    QueryIn = "UPDATE test_schema.user SET last_name = $last_name, first_name = $first_name, email = $email, enabled = $enabled, my_array = $my_array, user_token = $user_token, user_type = $user_type, number_value = $number_value, due_date = $due_date, user_state = $user_state, user_state_type = $user_state_type, version = version + 1, geog = ST_POINT($lon, $lat)::geography WHERE user_id = $user_id AND version = $version",

    % The column dict
    ColDict = orddict:from_list([{<<"user_id">>, #column{is_pkey = true}},
                                 {<<"first_name">>, #column{}},
                                 {<<"last_name">>, #column{}},
                                 {<<"email">>, #column{}},
                                 {<<"user_token">>, #column{valid_values = [unknown, living, deceased]}},
                                 {<<"enabled">>, #column{}},
                                 {<<"aka_id">>, #column{}},
                                 {<<"my_array">>, #column{}},
                                 {<<"user_type">>, #column{}},
                                 {<<"number_value">>, #column{}},
                                 {<<"created_on">>, #column{udt_name = <<"timestamp">>}},
                                 {<<"updated_on">>, #column{udt_name = <<"timestamp">>}},
                                 {<<"due_date">>, #column{udt_name = <<"timestamp">>}},
                                 {<<"user_state">>, #column{}},
                                 {<<"user_state_type">>, #column{}},
                                 {<<"version">>, #column{}},
                                 {<<"lat">>, #column{is_virtual = true}},
                                 {<<"lon">>, #column{is_virtual = true}}]),

    {ok, Query, Params, InParams, Record, Map} = parse_query("test_schema.User", QueryIn, ColDict),

    % The output
    ParamsAssert = ["LastName","FirstName","Email","Enabled","MyArray", "user_token_value(UserToken)","UserType","NumberValue",
                    "ts_decode(DueDate)","UserState","UserStateType","Lon","Lat","UserId", "Version"],
    ?assertEqual(ParamsAssert, Params),

    InParamsAssert = ["LastName","FirstName","Email","Enabled","MyArray", "UserToken","UserType","NumberValue",
                      "DueDate","UserState","UserStateType","Lon","Lat","UserId", "Version"],
    ?assertEqual(InParamsAssert, InParams),

    RecordAssert = "#'test_schema.User'{last_name = LastName, first_name = FirstName, email = Email, enabled = Enabled, my_array = MyArray, user_token = UserToken, user_type = UserType, number_value = NumberValue, due_date = DueDate, user_state = UserState, user_state_type = UserStateType, lon = Lon, lat = Lat, user_id = UserId, version = Version}",
    ?assertEqual(RecordAssert, Record),

    MapAssert = "#{last_name := LastName, first_name := FirstName, email := Email, enabled := Enabled, my_array := MyArray, user_token := UserToken, user_type := UserType, number_value := NumberValue, due_date := DueDate, user_state := UserState, user_state_type := UserStateType, lon := Lon, lat := Lat, user_id := UserId, version := Version}",
    ?assertEqual(MapAssert, Map),

    QueryAssert = "UPDATE test_schema.user SET last_name = $1, first_name = $2, email = $3, enabled = $4, my_array = $5, user_token = $6, user_type = $7, number_value = $8, due_date = $9, user_state = $10, user_state_type = $11, version = version + 1, geog = ST_POINT($12, $13)::geography WHERE user_id = $14 AND version = $15",
    ?assertEqual(QueryAssert, Query),

    ?LOG_INFO("====================== bind_param_test() END ======================"),
    ok.

dup_param_test() ->
    ?LOG_INFO("====================== dup_param_test() START ======================"),

    % The input
    QueryIn = "SELECT user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, version, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon FROM test_schema.user WHERE ST_DWithin(geog, Geography(ST_MakePoint($lon, $lat)), $radius) ORDER BY geog <-> ST_POINT($lon, $lat)::geography",

    ColDict = orddict:from_list([{<<"user_id">>, #column{is_pkey = true}},
                                 {<<"first_name">>, #column{}},
                                 {<<"last_name">>, #column{}},
                                 {<<"email">>, #column{}},
                                 {<<"user_token">>, #column{valid_values = [unknown, living, deceased]}},
                                 {<<"enabled">>, #column{}},
                                 {<<"aka_id">>, #column{}},
                                 {<<"my_array">>, #column{}},
                                 {<<"user_type">>, #column{}},
                                 {<<"number_value">>, #column{}},
                                 {<<"created_on">>, #column{udt_name = <<"timestamp">>}},
                                 {<<"updated_on">>, #column{udt_name = <<"timestamp">>}},
                                 {<<"due_date">>, #column{udt_name = <<"timestamp">>}},
                                 {<<"user_state">>, #column{}},
                                 {<<"version">>, #column{is_version = true}},
                                 {<<"lat">>, #column{is_virtual = true}},
                                 {<<"lon">>, #column{is_virtual = true}}]),

    {ok, Query, Params, InParams, Record, Map} = parse_query("test_schema.User", QueryIn, ColDict),

    % The output
    ParamsAssert = ["Lon", "Lat", "Radius"],
    ?assertEqual(ParamsAssert, Params),

    InParamsAssert = ["Lon", "Lat", "Radius"],
    ?assertEqual(InParamsAssert, InParams),

    RecordAssert = "#'test_schema.User'{lon = Lon, lat = Lat}",
    ?assertEqual(RecordAssert, Record),

    MapAssert = "#{lon := Lon, lat := Lat}",
    ?assertEqual(MapAssert, Map),

    QueryAssert = "SELECT user_id, first_name, last_name, email, user_token, enabled, aka_id, my_array, user_type, number_value, created_on, updated_on, due_date, user_state, version, ST_Y(geog::geometry) AS lat, ST_X(geog::geometry) AS lon FROM test_schema.user WHERE ST_DWithin(geog, Geography(ST_MakePoint($1, $2)), $3) ORDER BY geog <-> ST_POINT($1, $2)::geography",
    ?assertEqual(QueryAssert, Query),

    ?LOG_INFO("====================== dup_param_test() END ======================"),
    ok.

bug_param_test() ->
    ?LOG_INFO("====================== bug_param_test() START ======================"),

    % The input
    QueryIn = "INSERT INTO public.example_b (column_a, column_b1, column_1, test_id, p_bar, t_bar, version) VALUES ($column_a, $column_b1, $column_1, $test_id, $p_bar, $t_bar, 0) RETURNING column_a, column_b1, column_1, test_id, p_bar, t_bar, version",

    % The column dict
    ColDict = orddict:from_list([{<<"column_a">>, #column{is_pkey = true}},
                                 {<<"column_b1">>, #column{}},
                                 {<<"column_1">>, #column{}},
                                 {<<"test_id">>, #column{}},
                                 {<<"p_bar">>, #column{}},
                                 {<<"t_bar">>, #column{}},
                                 {<<"version">>, #column{is_version = true}},
                                 {<<"lat">>, #column{is_virtual = true}},
                                 {<<"lon">>, #column{is_virtual = true}}]),

    {ok, Query, Params, InParams, Record, Map} = parse_query("public.Product", QueryIn, ColDict),

    % The output
    ParamsAssert = ["ColumnA", "ColumnB1", "Column1", "TestId", "PBar", "TBar"],
    ?assertEqual(ParamsAssert, Params),

    InParamsAssert = ["ColumnA", "ColumnB1", "Column1", "TestId", "PBar", "TBar"],
    ?assertEqual(InParamsAssert, InParams),

    RecordAssert = "#'public.Product'{column_a = ColumnA, column_b1 = ColumnB1, column_1 = Column1, test_id = TestId, p_bar = PBar, t_bar = TBar}",
    ?assertEqual(RecordAssert, Record),

    MapAssert = "#{column_a := ColumnA, column_b1 := ColumnB1, column_1 := Column1, test_id := TestId, p_bar := PBar, t_bar := TBar}",
    ?assertEqual(MapAssert, Map),

    QueryAssert = "INSERT INTO public.example_b (column_a, column_b1, column_1, test_id, p_bar, t_bar, version) VALUES ($1, $2, $3, $4, $5, $6, 0) RETURNING column_a, column_b1, column_1, test_id, p_bar, t_bar, version",
    ?assertEqual(QueryAssert, Query),

    ?LOG_INFO("====================== bug_param_test() END ======================"),
    ok.

-endif.
