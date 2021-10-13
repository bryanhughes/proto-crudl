%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 17. Feb 2021 7:40 PM
%%%-------------------------------------------------------------------
-module(example_tests).
-author("bryan").

-include_lib("eunit/include/eunit.hrl").
-include_lib("kernel/include/logger.hrl").

-define(LAT_0, 38.470763).
-define(LON_0, -123.022557).
-define(LAT_1, 38.476541).      % 1,767 feet to LAT_0
-define(LON_1, -123.024377).
-define(LAT_2, 38.465016).      % 4,307 feet to LAT_0. 1.12 miles to LAT_1
-define(LON_2, -123.009292).

first_test() ->
    application:ensure_all_started(logger),
    logger:set_primary_config(level, info),
    application:ensure_all_started(pgo),
    ?LOG_INFO("====================== first_test() Starting Database ======================"),
    application:set_env(pg_types, uuid_format, string),
    pgo:start_pool(default, #{pool_size => 10,
                              host => "127.0.0.1",
                              database => "proto_crudl",
                              user => "proto_crudl",
                              password => "proto_crudl"}),
    ok.

-define(records, true).

-ifdef(maps).

crudl_proto_maps_test() ->
    ?LOG_INFO("====================== crudl_proto_maps_test() START ======================"),
    ?LOG_INFO("Deleting from test_schema.user"),

    Query = "DELETE FROM test_schema.user",
    case pgo:query(Query, []) of
        #{command := delete} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end,

    % Do a good create
    {ok, Bryan} = test_schema_user_db:create(<<"Bryan">>, <<"Hughes">>, <<"hughesb@gmail.com">>, undefined,
                                             [1, 2, 3], 'BIG_SHOT', 100, undefined, {{2021, 2, 23}, {10, 23, 23.5}},
                                             undefined, undefined, undefined),
    ?LOG_INFO("Bryan=~p", [Bryan]),
    ?assertEqual(<<"hughesb@gmail.com">>, maps:get(email, Bryan)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan)),
    ?assertEqual([1, 2, 3], maps:get(my_array, Bryan)),
    ?assertEqual('BIG_SHOT', maps:get(user_type, Bryan)),
    ?assertEqual(0, maps:get(version, Bryan)),
    ?assertEqual({{2021, 2, 23}, {10, 23, 23.5}}, test_schema_user_db:ts_decode(maps:get(created_on, Bryan))),

    % The code was generated with {use_defaults, true}, while non-foreign keys are set to a default or empty value
    % because of the proto3 code generation, foreign key columns are still null
    ?assertEqual(null, maps:get(aka_id, Bryan)),

    BryanId = maps:get(user_id, Bryan),
    ?assert(BryanId > 0),

    {ok, Tom} = test_schema_user_db:create(<<"Tom">>, undefined, <<"tombagby@gmail.com">>, undefined, [100, 200, 300],
                                           '_123FUN', 100, undefined, {{2021, 2, 23}, {0, 0, 0}}, undefined, ?LON_0, ?LAT_0),

    ?LOG_INFO("Tom=~p", [Tom]),
    ?assertEqual(<<"tombagby@gmail.com">>, maps:get(email, Tom)),
    ?assertEqual(<<"Tom">>, maps:get(first_name, Tom)),
    ?assertEqual(null, maps:get(last_name, Tom)),

    TomId = maps:get(user_id, Tom),
    ?assert(TomId > BryanId),

    ?LOG_INFO("Reading Bryan back, user_id=~p", [BryanId]),

    % The internal utility is to convert the results into a list of maps where each map is a record. Even though a read
    % will always return a single record, it still uses the same implementation, versus just {ok, Map}.
    {ok, Bryan3} = test_schema_user_db:read(BryanId),
    ?LOG_INFO("Bryan3=~p", [Bryan3]),

    ?assertEqual(<<"hughesb@gmail.com">>, maps:get(email, Bryan3)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan3)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan3)),
    ?assertEqual(true, maps:get(enabled, Bryan3)),

    ?LOG_INFO("Updating Bryan..."),
    Bryan4 = Bryan3#{user_token => <<"123345">>, enabled => false, email => <<"foo@gmail.com">>},
    ?LOG_INFO("Bryan4=~p", [Bryan4]),
    {ok, Bryan4a} = test_schema_user_db:update(Bryan4),
    ?assertEqual(1, maps:get(version, Bryan4a)),

    {ok, Bryan5} = test_schema_user_db:read(BryanId),
    ?LOG_INFO("Bryan5=~p", [Bryan5]),

    ?assertEqual(<<"foo@gmail.com">>, maps:get(email, Bryan5)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan5)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan5)),
    ?assertEqual(<<"00000000-0000-0000-0000-000000123345">>, maps:get(user_token, Bryan5)),
    ?assertEqual(false, maps:get(enabled, Bryan5)),
    ?assertEqual(1, maps:get(version, Bryan5)),

    {ok, Tom3} = test_schema_user_db:read(TomId),
    ?LOG_INFO("Tom3=~p", [Tom3]),

    ?assertEqual(<<"tombagby@gmail.com">>, maps:get(email, Tom3)),
    ?assertEqual(<<"Tom">>, maps:get(first_name, Tom3)),
    ?assertEqual(null, maps:get(last_name, Tom3)),

    % Test our default value set in the schema
    ?assert(maps:get(user_token, Tom3) =/= null),

    % Now test a bad id
    {error, invalid_map} = test_schema_user_db:update(#{user_id => 0}),

    ?LOG_INFO("Looking up..."),
    {ok, 1, [Bryan5]} = test_schema_user_db:lookup_email(#{email => <<"foo@gmail.com">>}),

    ?LOG_INFO("Listing users..."),
    {ok, 2, Users} = test_schema_user_db:list(10, 0),
    ?LOG_INFO("Users=~p", [Users]),

    {ok, 1, _} = test_schema_user_db:list(1, 0),
    {ok, 1, _} = test_schema_user_db:list(1, 1),
    {ok, 0, _} = test_schema_user_db:list(10, 2),

    ?LOG_INFO("Exercising custom functions..."),
    {ok, 1, []} = test_schema_user_db:disable_user(<<"foo@gmail.com">>),

    {ok, Bryan6} = test_schema_user_db:read(BryanId),
    ?LOG_INFO("Bryan6=~p", [Bryan6]),

    ?assertEqual(<<"foo@gmail.com">>, maps:get(email, Bryan6)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan6)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan6)),

    UserToken = maps:get(user_token, Bryan6),
    ?assertEqual(<<"00000000-0000-0000-0000-000000123345">>, UserToken),
    ?assertEqual(false, maps:get(enabled, Bryan6)),

    {ok, 1, [Result]} = test_schema_user_db:set_token(BryanId, maps:get(version, Bryan6)),
    SetToken = maps:get(user_token, Result),
    Version = maps:get(version, Result),
    ?LOG_INFO("SetToken=~p, Version=~p", [SetToken, Version]),

    {ok, Bryan7} = test_schema_user_db:read(BryanId),
    ?LOG_INFO("Bryan7=~p", [Bryan7]),

    ?assertEqual(<<"foo@gmail.com">>, maps:get(email, Bryan7)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan7)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan7)),
    ?assertEqual(false, maps:get(enabled, Bryan7)),

    UserToken1 = maps:get(user_token, Bryan7),
    ?assertEqual(SetToken, UserToken1),

    ?LOG_INFO("Set UserToken=~p, Old UserToken=~p", [UserToken, UserToken1]),
    ?assert(UserToken =/= UserToken1),

    Enum = user_pb:enum_value_by_symbol('test_schema.User.UserType', maps:get(user_type, Bryan7)),
    ?assertEqual(4, Enum),

    % Test our version
    Bryan8 = Bryan7#{last_name => <<"Bagby">>},
    {ok, Bryan9} = test_schema_user_db:update(Bryan8),
    ?assertEqual(<<"Bagby">>, maps:get(last_name, Bryan9)),

    % Try to update it again, should fail by returning no results because the version has changed
    notfound = test_schema_user_db:update(Bryan8),

    ?LOG_INFO("Bryan7=~p", [Bryan7]),

    %% Do some encoding/decoding
    Encoded = user_pb:encode_msg(Bryan7, 'test_schema.User'),
    ?assert(size(Encoded) > 1),
    Decoded = user_pb:decode_msg(Encoded, 'test_schema.User'),
    ?LOG_INFO("Decoded=~0p", [Decoded]),

    ?assertEqual(Bryan7, test_schema_user_db:from_proto(Decoded)),

    {ok, 1, []} = test_schema_user_db:delete_user_by_email(<<"foo@gmail.com">>),
    notfound = test_schema_user_db:read(BryanId),

    ok = test_schema_user_db:delete(TomId),
    notfound = test_schema_user_db:read(TomId).

custom_query_map_test() ->
    ?LOG_INFO("====================== custom_query_test() START ======================"),
    ?LOG_INFO("Deleting from test_schema.user"),

    Query = "DELETE FROM test_schema.user",
    case pgo:query(Query, []) of
        #{command := delete} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end,

    {ok, Bryan} = test_schema_user_db:create(<<"Bryan">>, <<"Hughes">>, <<"hughesb@gmail.com">>, undefined,
                                             [1, 2, 3], 'BIG_SHOT', 100, undefined, {{2021, 2, 23}, {10, 23, 23.5}},
                                             undefined, ?LON_0, ?LAT_0),
    ?LOG_INFO("Bryan=~p", [Bryan]),
    ?assertEqual(?LAT_0, maps:get(lat, Bryan)),
    ?assertEqual(?LON_0, maps:get(lon, Bryan)),

    {ok, Tom} = test_schema_user_db:create(<<"Tom">>, undefined, <<"tombagby@gmail.com">>, undefined, [100, 200, 300],
                                           '_123FUN', 100, undefined, {{2021, 2, 23}, {0, 0, 0}}, undefined,
                                           ?LON_1, ?LAT_1),

    ?LOG_INFO("Tom=~p", [Tom]),
    ?assertEqual(?LAT_1, maps:get(lat, Tom)),
    ?assertEqual(?LON_1, maps:get(lon, Tom)),

    Result0 = test_schema_user_db:find_nearest(?LON_0, ?LAT_0, 10),
    ?LOG_INFO("Result0=~p", [Result0]),
    {ok, 1, [Row]} = Result0,
    ?assertEqual(<<"hughesb@gmail.com">>, maps:get(email, Row)),
    ?assertEqual(?LAT_0, maps:get(lat, Row)),
    ?assertEqual(?LON_0, maps:get(lon, Row)),
    ?assertEqual(0, maps:get(version, Row)).


-else.

-include("user_pb.hrl").
-record(bad_record, {badbadbad}).

crudl_proto_records_test() ->
    ?LOG_INFO("====================== crudl_proto_records_test() START ======================"),
    ?LOG_INFO("Deleting from test_schema.user"),

    Query = "DELETE FROM test_schema.user",
    case pgo:query(Query, []) of
        #{command := delete} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end,

    % Do a good create
    {ok, Bryan} = test_schema_user_db:create(<<"Bryan">>, <<"Hughes">>, <<"hughesb@gmail.com">>, undefined,
                                             [1, 2, 3], 'BIG_SHOT', 100, {{2021, 2, 23}, {10, 23, 23.5}}, {2021, 2, 23},
                                             'living', undefined, undefined),
    ?LOG_INFO("Bryan=~p", [Bryan]),
    ?assertEqual(<<"hughesb@gmail.com">>, Bryan#'test_schema.User'.email),
    ?assertEqual(<<"Bryan">>, Bryan#'test_schema.User'.first_name),
    ?assertEqual(<<"Hughes">>, Bryan#'test_schema.User'.last_name),
    ?assertEqual([1, 2, 3], Bryan#'test_schema.User'.my_array),
    ?assertEqual('BIG_SHOT', Bryan#'test_schema.User'.user_type),
    ?assertEqual('living', Bryan#'test_schema.User'.user_state),
    ?assertEqual(0, Bryan#'test_schema.User'.version),
    ?assertEqual({'google.protobuf.Timestamp',1614075803,500000}, Bryan#'test_schema.User'.updated_on),
    ?assertEqual({'google.protobuf.Timestamp',1614038400,0}, Bryan#'test_schema.User'.due_date),

    Decoded0 = test_schema_user_db:ts_decode(Bryan#'test_schema.User'.updated_on),
    ?LOG_INFO("Decoded0=~p", [Decoded0]),
    ?assertEqual({{2021, 2, 23}, {10, 23, 23.5}}, Decoded0),

    Decoded1 = test_schema_user_db:date_decode(Bryan#'test_schema.User'.due_date),
    ?assertEqual({2021, 2, 23}, Decoded1),

    ?LOG_INFO("created_on=~p", [Bryan#'test_schema.User'.created_on]),

    % The code was generated with {use_defaults, true}, while non-foreign keys are set to a default or empty value
    % because of the proto3 code generation, foreign key columns are still null
    ?assertEqual(undefined, Bryan#'test_schema.User'.aka_id),

    BryanId = Bryan#'test_schema.User'.user_id,
    ?assert(BryanId > 0),

    {ok, Tom} = test_schema_user_db:create(<<"Tom">>, undefined, <<"tombagby@gmail.com">>, undefined, [100, 200, 300],
                                           '_123FUN', 100, undefined, {2021, 2, 23}, 'living', ?LON_0, ?LAT_0),

    ?LOG_INFO("Tom=~p", [Tom]),
    ?assertEqual(<<"tombagby@gmail.com">>, Tom#'test_schema.User'.email),
    ?assertEqual(<<"Tom">>, Tom#'test_schema.User'.first_name),
    ?assertEqual(undefined, Tom#'test_schema.User'.last_name),
    ?assertEqual('_123FUN', Tom#'test_schema.User'.user_type),
    ?assertEqual('living', Tom#'test_schema.User'.user_state),

    TomId = Tom#'test_schema.User'.user_id,
    ?assert(TomId > BryanId),

    ?LOG_INFO("Reading Bryan back=~p", [BryanId]),

    % The internal utility is to convert the results into a list of maps where each map is a record. Even though a read
    % will always return a single record, it still uses the same implementation, versus just {ok, Map}.
    {ok, Bryan3} = test_schema_user_db:read(BryanId),
    ?LOG_INFO("Bryan3=~p", [Bryan3]),

    ?assertEqual(<<"hughesb@gmail.com">>, Bryan3#'test_schema.User'.email),
    ?assertEqual(<<"Bryan">>, Bryan3#'test_schema.User'.first_name),
    ?assertEqual(<<"Hughes">>, Bryan3#'test_schema.User'.last_name),
    ?assertEqual(true, Bryan3#'test_schema.User'.enabled),

    ?LOG_INFO("Updating Bryan..."),
    Bryan4 = Bryan3#'test_schema.User'{user_token = <<"123345">>, enabled = false, email = <<"foo@gmail.com">>},
    ?LOG_INFO("Bryan4=~p", [Bryan4]),
    {ok, Bryan4a} = test_schema_user_db:update(Bryan4),
    ?assertEqual(1, Bryan4a#'test_schema.User'.version),

    {ok, Bryan5} = test_schema_user_db:read(BryanId),
    ?LOG_INFO("Bryan5=~p", [Bryan5]),

    ?assertEqual(<<"foo@gmail.com">>, Bryan5#'test_schema.User'.email),
    ?assertEqual(<<"Bryan">>, Bryan5#'test_schema.User'.first_name),
    ?assertEqual(<<"Hughes">>, Bryan5#'test_schema.User'.last_name),
    ?assertEqual(<<"00000000-0000-0000-0000-000000123345">>, Bryan5#'test_schema.User'.user_token),
    ?assertEqual(false, Bryan5#'test_schema.User'.enabled),
    ?assertEqual(1, Bryan5#'test_schema.User'.version),

    {ok, Tom3} = test_schema_user_db:read(TomId),
    ?LOG_INFO("Tom3=~p", [Tom3]),

    ?assertEqual(<<"tombagby@gmail.com">>, Tom3#'test_schema.User'.email),
    ?assertEqual(<<"Tom">>, Tom3#'test_schema.User'.first_name),
    ?assertEqual(undefined, Tom3#'test_schema.User'.last_name),

    % Test our default value set in the schema
    ?assert(Tom3#'test_schema.User'.user_token =/= undefined),

    % Now test a bad record
    {error, invalid_record} = test_schema_user_db:update(#bad_record{badbadbad = 0}),

    ?LOG_INFO("Looking up..."),
    {ok, 1, [Bryan5]} = test_schema_user_db:lookup_email(#'test_schema.User'{email = <<"foo@gmail.com">>}),

    ?LOG_INFO("Listing users..."),
    {ok, 2, Users} = test_schema_user_db:list(10, 0),
    ?LOG_INFO("Users=~p", [Users]),

    {ok, 1, _} = test_schema_user_db:list(1, 0),
    {ok, 1, _} = test_schema_user_db:list(1, 1),
    {ok, 0, _} = test_schema_user_db:list(10, 2),

    ?LOG_INFO("Exercising custom functions..."),
    {ok, 1, []} = test_schema_user_db:disable_user(<<"foo@gmail.com">>),

    {ok, Bryan6} = test_schema_user_db:read(BryanId),
    ?LOG_INFO("Bryan6=~p", [Bryan6]),

    ?assertEqual(<<"foo@gmail.com">>, Bryan6#'test_schema.User'.email),
    ?assertEqual(<<"Bryan">>, Bryan6#'test_schema.User'.first_name),
    ?assertEqual(<<"Hughes">>, Bryan6#'test_schema.User'.last_name),

    UserToken = Bryan6#'test_schema.User'.user_token,
    ?assertEqual(<<"00000000-0000-0000-0000-000000123345">>, UserToken),
    ?assertEqual(false, Bryan6#'test_schema.User'.enabled),

    {ok, 1, [Result]} = test_schema_user_db:set_token(BryanId, Bryan6#'test_schema.User'.version),
    ?LOG_INFO("Result=~p", [Result]),
    SetToken = Result#'test_schema.SetToken'.user_token,
    Version = Result#'test_schema.SetToken'.version,
    ?LOG_INFO("SetToken=~p, Version=~p", [SetToken, Version]),

    {ok, Bryan7} = test_schema_user_db:read(BryanId),
    ?LOG_INFO("Bryan7=~p", [Bryan7]),

    ?assertEqual(<<"foo@gmail.com">>, Bryan7#'test_schema.User'.email),
    ?assertEqual(<<"Bryan">>, Bryan7#'test_schema.User'.first_name),
    ?assertEqual(<<"Hughes">>, Bryan7#'test_schema.User'.last_name),
    ?assertEqual('BIG_SHOT', Bryan7#'test_schema.User'.user_type),
    ?assertEqual(false, Bryan7#'test_schema.User'.enabled),

    UserToken1 = Bryan7#'test_schema.User'.user_token,
    ?assertEqual(SetToken, UserToken1),

    ?LOG_INFO("Set UserToken=~p, Old UserToken=~p", [UserToken, UserToken1]),
    ?assert(UserToken =/= UserToken1),

    Enum = user_pb:enum_value_by_symbol('test_schema.User.UserType', Bryan7#'test_schema.User'.user_type),
    ?LOG_INFO("Enum=~0p", [Enum]),
    ?assertEqual(0, Enum),

    % Test our version
    Bryan8 = Bryan7#'test_schema.User'{last_name = <<"Bagby">>},
    {ok, Bryan9} = test_schema_user_db:update(Bryan8),
    ?assertEqual(<<"Bagby">>, Bryan9#'test_schema.User'.last_name),

    % Try to update it again, should fail by returning no results because the version has changed
    notfound = test_schema_user_db:update(Bryan8),

    ?LOG_INFO("Encoding. Bryan9=~p", [Bryan9]),

    % While proto_crudl is all about mapping protobuffers to relational tables in Erlang, how timestamps are handled
    % are disjointed. Erlang handles dates, time, and datetime as
    % datetime() = {date(), time()}
    % date() = {year(), month(), day()}
    % year() = integer() >= 0
    % while protobuffers are treated as timestamps as {seconds, nanoseconds}. Call the helper functions to_proto/1 and
    % from_proto/1 to convert datetime to timestamps.

    ?assertEqual({'google.protobuf.Timestamp',1614075803,500000}, Bryan9#'test_schema.User'.updated_on),
    ?assertEqual({'google.protobuf.Timestamp',1614038400,0}, Bryan9#'test_schema.User'.due_date),

    ToProto = test_schema_user_db:to_proto(Bryan9),
    ?LOG_INFO("ToProto=~0p", [ToProto]),

    ?assertEqual({'google.protobuf.Timestamp',1614075803,500000}, ToProto#'test_schema.User'.updated_on),
    ?assertEqual({'google.protobuf.Timestamp',1614038400,0}, ToProto#'test_schema.User'.due_date),

    Encoded = user_pb:encode_msg(ToProto, 'test_schema.User'),
    ?assert(size(Encoded) > 1),
    Decoded = user_pb:decode_msg(Encoded, 'test_schema.User'),
    ?LOG_INFO("Decoded=~0p", [Decoded]),
    FromProto = test_schema_user_db:from_proto(Decoded),
    ?LOG_INFO("FromProto=~0p", [FromProto]),

    ?assertEqual({{2021, 2, 23}, {10, 23, 23.5}}, FromProto#'test_schema.User'.updated_on),
    ?assertEqual({2021, 2, 23}, FromProto#'test_schema.User'.due_date),

    {ok, 1, []} = test_schema_user_db:delete_user_by_email(<<"foo@gmail.com">>),
    notfound = test_schema_user_db:read(BryanId),

    ok = test_schema_user_db:delete(TomId),
    notfound = test_schema_user_db:read(TomId).

custom_query_record_test() ->
    ?LOG_INFO("====================== custom_query_record_test() START ======================"),
    ?LOG_INFO("Deleting from test_schema.user"),

    Query = "DELETE FROM test_schema.user",
    case pgo:query(Query, []) of
        #{command := delete} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end,
    {ok, Bryan} = test_schema_user_db:create(<<"Bryan">>, <<"Hughes">>, <<"hughesb@gmail.com">>, undefined,
                                             [1, 2, 3], 'BIG_SHOT', 100, {{2021, 2, 23}, {10, 23, 23.5}}, {2021, 2, 23},
                                             'living', ?LON_0, ?LAT_0),
    ?LOG_INFO("Bryan=~p", [Bryan]),
    ?assertEqual(?LAT_0, Bryan#'test_schema.User'.lat),
    ?assertEqual(?LON_0, Bryan#'test_schema.User'.lon),

    {ok, Tom} = test_schema_user_db:create(<<"Tom">>, undefined, <<"tombagby@gmail.com">>, undefined, [100, 200, 300],
                                           '_123FUN', 100, undefined, undefined, undefined, ?LON_1, ?LAT_1),

    ?LOG_INFO("Tom=~p", [Tom]),
    ?assertEqual(?LAT_1, Tom#'test_schema.User'.lat),
    ?assertEqual(?LON_1, Tom#'test_schema.User'.lon),

    Result0 = test_schema_user_db:find_nearest(?LON_0, ?LAT_0, 10),
    ?LOG_INFO("Result0=~p", [Result0]),
    {ok, 1, [Row]} = Result0,
    ?assertEqual(<<"hughesb@gmail.com">>, Row#'test_schema.FindNearest'.email),
    ?assertEqual(?LAT_0, Row#'test_schema.FindNearest'.lat),
    ?assertEqual(?LON_0, Row#'test_schema.FindNearest'.lon),
    ?assertEqual(0, Row#'test_schema.FindNearest'.version).

update_fkey_record_test() ->
    ?LOG_INFO("====================== update_fkey_record_test() START ======================"),
    ?LOG_INFO("Deleting from test_schema.user"),

    Query = "DELETE FROM test_schema.user",
    case pgo:query(Query, []) of
        #{command := delete} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end,
    {ok, Bryan} = test_schema_user_db:create(<<"Bryan">>, <<"Hughes">>, <<"hughesb@gmail.com">>, undefined,
                                             [1, 2, 3], 'BIG_SHOT', 100, {{2021, 2, 23}, {10, 23, 23.5}}, {2021, 2, 23},
                                             'living', ?LON_0, ?LAT_0),
    ?LOG_INFO("Bryan=~p", [Bryan]),
    ?assertEqual(?LAT_0, Bryan#'test_schema.User'.lat),
    ?assertEqual(?LON_0, Bryan#'test_schema.User'.lon),

    {ok, Tom} = test_schema_user_db:create(<<"Tom">>, undefined, <<"tombagby@gmail.com">>, undefined, undefined,
                                           '_123FUN', 100, undefined, undefined, undefined, ?LON_1, ?LAT_1),
    % The generated code will make sure the undefined and null are empty lists
    ?assertEqual([], Tom#'test_schema.User'.my_array),

    ?LOG_INFO("Tom (before update)=~p", [Tom]),

    % Now Update Tom and try to update the aka_id directly, this should not work
    {ok, Tom1} = test_schema_user_db:update(Tom#'test_schema.User'{aka_id = 100000, user_type = 'BUSY_GUY'}),
    ?LOG_INFO("Tom1 (after update #1)=~p", [Tom1]),

    ?assertNotEqual(Bryan#'test_schema.User'.user_id, Tom1#'test_schema.User'.aka_id),
    ?assertEqual('BUSY_GUY', Tom1#'test_schema.User'.user_type),

    % Now do the update on the foreign key

    {ok, Tom2} = test_schema_user_db:update_fk_user_user(Bryan#'test_schema.User'.user_id, Tom#'test_schema.User'.user_id),
    ?LOG_INFO("Tom2 (after update #2)=~p", [Tom2]),

    ?assertEqual(Bryan#'test_schema.User'.user_id, Tom2#'test_schema.User'.aka_id),

    ok.

-endif.
