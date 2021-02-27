%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Feb 2021 2:12 PM
%%%-------------------------------------------------------------------
-module(example_utils).
-author("bryan").

%% API
-compile(export_all).

-spec secs() -> Seconds::non_neg_integer().
%% @doc Returns whole seconds
secs() ->
    erlang:system_time(seconds).

-spec get_server() -> [string()].
get_server() ->
    Protocol = application:get_env(example_service, protocol, "http"),
    Server = application:get_env(example_service, server, "localhost"),
    [Protocol, "://", Server].

-spec get_serverport() -> non_neg_integer().
get_serverport() ->
    application:get_env(example_service, port, 8080).

-spec compare_maps(Map0::map(), Map1::map()) -> true | false.
compare_maps(Map0, Map1) ->
    Keys0 = maps:keys(Map0),
    compare_maps_(Keys0, Map0, Map1).
compare_maps_([], _Map0, _Map1) ->
    true;
compare_maps_([Key | Rest], Map0, Map1) ->
    case maps:find(Key, Map0) of
        {ok, null} ->
            compare_maps_(Rest, Map0, Map1);
        {ok, V0} ->
            case maps:find(Key, Map1) of
                {ok, null} ->
                    compare_maps_(Rest, Map0, Map1);
                {ok, V0} ->
                    compare_maps_(Rest, Map0, Map1);
                {ok, V} ->
                    logger:info("Expected ~p but got ~p for key ~p in map 1 ~p", [V0, V, Key, Map1]),
                    false;
                error ->
                    logger:info("Expected ~p but not present for key ~p in map 1 ~p", [V0, Key, Map1]),
                    false
            end;
        error ->
            logger:info("Expected value but not present for key ~p in map 0 ~p", [Key, Map0]),
            false
    end.

-spec to_string(any()) -> string().
to_string(X) when is_atom(X) -> atom_to_list(X);
to_string(X) when is_float(X) -> [L] = io_lib:format("~.3f", [X]), L;
to_string(X) when is_integer(X) -> integer_to_list(X);
to_string(X) when is_binary(X) -> binary_to_list(X);
to_string(X) -> X.

-spec to_binary(any()) -> binary().
to_binary(undefined) ->
    <<>>;
to_binary([]) ->
    <<>>;
to_binary(L) when is_list(L) ->
    list_to_binary(L);
to_binary(I) when is_integer(I) ->
    integer_to_binary(I);
to_binary(F) when is_float(F) ->
    [L] = io_lib:format("~.3f", [F]),
    list_to_binary(L);
to_binary(A) when is_atom(A) ->
    atom_to_binary(A, utf8);
to_binary(B) when is_binary(B)->
    B;
to_binary(B)->
    list_to_binary(io_lib:format("~p", [B])).

-spec to_list(any()) -> list().
to_list(undefined) ->
    [];
to_list(Term) when is_integer(Term) ->
    integer_to_list(Term);
to_list(Term) when is_binary(Term) ->
    binary_to_list(Term);
to_list(Term) when is_list(Term) ->
    Term;
to_list(Term) when is_float(Term) ->
    [L] = io_lib:format("~.3f", [Term]),
    L;
to_list(Term) ->
    Term.

-spec to_integer(any()) -> integer().
to_integer(undefined) ->
    0;
to_integer(true) ->
    1;
to_integer(false) ->
    0;
to_integer(<<"true">>) ->
    1;
to_integer(<<"false">>) ->
    0;
to_integer("true") ->
    1;
to_integer("false") ->
    0;
to_integer([]) ->
    0;
to_integer(<<>>) ->
    0;
to_integer(Term) when is_integer(Term) ->
    Term;
to_integer(Term) when is_binary(Term) ->
    binary_to_integer(Term);
to_integer(Term) when is_list(Term) ->
    list_to_integer(Term);
to_integer(Term) when is_float(Term) ->
    round(Term);
to_integer(_) ->
    0.

-spec to_float(any()) -> float().
to_float(undefined) ->
    0.0;
to_float([]) ->
    0.0;
to_float(Term) when is_float(Term) ->
    Term;
to_float(Term) when is_binary(Term) ->
    binary_to_float(Term);
to_float(Term) when is_list(Term) ->
    list_to_float(Term);
to_float(Term) when is_integer(Term) ->
    float(Term);
to_float(_) ->
    0.0.

-spec to_boolean(any()) -> atom().
to_boolean(undefined) ->
    false;
to_boolean(Term) when is_atom(Term) ->
    Term;
to_boolean(1) ->
    true;
to_boolean(0) ->
    false;
to_boolean("true") ->
    true;
to_boolean("false") ->
    false;
to_boolean(<<"true">>) ->
    true;
to_boolean(<<"false">>) ->
    false.

-spec to_atom(any()) -> atom().
to_atom(Term) when is_atom(Term) ->
    Term;
to_atom(Term) when is_list(Term) ->
    list_to_atom(Term);
to_atom(Term) when is_binary(Term) ->
    binary_to_atom(Term, utf8);
to_atom(_Term) ->
    erlang:error(unable_to_convert).
