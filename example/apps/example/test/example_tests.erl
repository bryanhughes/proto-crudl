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

first_test() ->
    logger:set_primary_config(level, info),
    application:ensure_all_started(pgo),
    ?LOG_INFO("====================== first_test() Starting Database ======================"),
    application:set_env(pg_types, uuid_format, string),
    pgo:start_pool(default, #{pool_size => 10,
                              host => "127.0.0.1",
                              database => "erl_crudl",
                              user => "erl_crudl",
                              password => "erl_crudl"}),
    ok.

crudl_proto_test() ->
    ?LOG_INFO("====================== crudl_proto_test() START ======================"),
    ?LOG_INFO("Deleting from test_schema.user"),

    Query = "DELETE FROM test_schema.user",
    case pgo:query(Query, []) of
        #{command := delete} ->
            ok;
        {error, Reason} ->
            erlang:error(Reason)
    end,

    % Do some bad tests against our enum/check constraint
    BadBryan = (test_schema_user_db:new())#{first_name => <<"Bryan">>, last_name => <<"Hughes">>,
                                            email => <<"hughesb@gmail.com">>, my_array => [1, 2, 3]},
    ?LOG_INFO("Creating bad user=~p", [BadBryan]),
    {error, Reason0} = test_schema_user_db:create(BadBryan),
    ?LOG_INFO("Reason=~p", [Reason0]),

    BadBryan1 = (test_schema_user_db:new())#{first_name => <<"Bryan">>, last_name => <<"Hughes">>,
                                             email => <<"hughesb@gmail.com">>, user_type => 'FOOBAR',
                                             number_value => 100, created_on => 0, my_array => [1, 2, 3]},
    ?LOG_INFO("Creating bad user=~p", [BadBryan1]),
    {error, Reason1} = test_schema_user_db:create(BadBryan1),
    ?LOG_INFO("Reason=~p", [Reason1]),


    % Do a good create
    Bryan = (test_schema_user_db:new())#{first_name => <<"Bryan">>, last_name => <<"Hughes">>,
                                         email => <<"hughesb@gmail.com">>, user_type => 'BIG_SHOT',
                                         number_value => 100, created_on => {{2021,2,23},{10,23,23.5}},
                                         my_array => [1, 2, 3]},
    ?LOG_INFO("Bryan=~p", [Bryan]),
    {ok, Bryan1} = test_schema_user_db:create(Bryan),
    ?LOG_INFO("Bryan1=~p", [Bryan1]),
    ?assertEqual(<<"hughesb@gmail.com">>, maps:get(email, Bryan1)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan1)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan1)),
    ?assertEqual([1,2,3], maps:get(my_array, Bryan1)),
    ?assertEqual('BIG_SHOT', maps:get(user_type, Bryan1)),
    ?assertEqual({{2021,2,23},{10,23,23.5}}, test_schema_user_db:ts_decode_map(maps:get(created_on, Bryan1))),

    % The code was generated with {use_defaults, true}, while non-foreign keys are set to a default or empty value
    % because of the proto3 code generation, foreign key columns are still null
    ?assertEqual(null, maps:get(aka_id, Bryan1)),

    BryanId = maps:get(user_id, Bryan1),
    ?assert(BryanId > 0),

    Tom1 = (test_schema_user_db:new())#{first_name => <<"Tom">>, email => <<"tombagby@gmail.com">>,
                                        user_type => '_123FUN',
                                        number_value => 100, created_on => {{2021,2,23},{0,0,0}},
                                        my_array => [100, 200, 300]},
    ?LOG_INFO("Creating user=~p", [Tom1]),
    {ok, Tom2} = test_schema_user_db:create(Tom1),

    ?LOG_INFO("Tom2=~p", [Tom2]),
    ?assertEqual(<<"tombagby@gmail.com">>, maps:get(email, Tom2)),
    ?assertEqual(<<"Tom">>, maps:get(first_name, Tom2)),
    ?assertEqual(null, maps:get(last_name, Tom2)),

    TomId = maps:get(user_id, Tom2),
    ?assert(TomId > BryanId),

    ?LOG_INFO("Reading Bryan back=~p", [BryanId]),

    % The internal utility is to convert the results into a list of maps where each map is a record. Even though a read
    % will always return a single record, it still uses the same implementation, versus just {ok, Map}.
    {ok, Bryan3} = test_schema_user_db:read(#{user_id => BryanId}),
    ?LOG_INFO("Bryan3=~p", [Bryan3]),

    ?assertEqual(<<"hughesb@gmail.com">>, maps:get(email, Bryan3)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan3)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan3)),
    ?assertEqual(true, maps:get(enabled, Bryan3)),

    ?LOG_INFO("Updating Bryan..."),
    Bryan4 = Bryan3#{user_token => <<"123345">>, enabled => false, email => <<"foo@gmail.com">>},
    {ok, _} = test_schema_user_db:update(Bryan4),

    {ok, Bryan5} = test_schema_user_db:read(Bryan1),
    ?LOG_INFO("Bryan5=~p", [Bryan5]),

    ?assertEqual(<<"foo@gmail.com">>, maps:get(email, Bryan5)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan5)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan5)),
    ?assertEqual(<<"00000000-0000-0000-0000-000000123345">>, maps:get(user_token, Bryan5)),
    ?assertEqual(false, maps:get(enabled, Bryan5)),

    {ok, Tom3} = test_schema_user_db:read(#{user_id => TomId}),
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

    {ok, Bryan6} = test_schema_user_db:read(#{user_id => BryanId}),
    ?LOG_INFO("Bryan6=~p", [Bryan6]),

    ?assertEqual(<<"foo@gmail.com">>, maps:get(email, Bryan6)),
    ?assertEqual(<<"Bryan">>, maps:get(first_name, Bryan6)),
    ?assertEqual(<<"Hughes">>, maps:get(last_name, Bryan6)),

    UserToken = maps:get(user_token, Bryan6),
    ?assertEqual(<<"00000000-0000-0000-0000-000000123345">>, UserToken),
    ?assertEqual(false, maps:get(enabled, Bryan6)),

    {ok, 1, [Result]} = test_schema_user_db:set_token(BryanId),
    SetToken = maps:get(user_token, Result),

    {ok, Bryan7} = test_schema_user_db:read(#{user_id => BryanId}),
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

    %% Do some encoding/decoding
    Encoded = user_pb:encode_msg(test_schema_user_db:to_proto(Bryan7), 'test_schema.User'),
    ?assert(size(Encoded) > 1),
    Decoded = user_pb:decode_msg(Encoded, 'test_schema.User'),
    ?LOG_INFO("Decoded=~0p", [Decoded]),

    ?assertEqual(Bryan7, test_schema_user_db:from_proto(Decoded)),

    {ok, 1, []} = test_schema_user_db:delete_user_by_email(<<"foo@gmail.com">>),
    not_found = test_schema_user_db:read(#{user_id => BryanId}),

    ok = test_schema_user_db:delete(#{user_id => TomId}),
    not_found = test_schema_user_db:read(#{user_id => TomId}),

    ?LOG_INFO("====================== crudl_proto_test() END ======================"),
    ok.



