%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2019, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 12. Jun 2019 12:02 PM
%%%-------------------------------------------------------------------
-module(proto_crudl_utils).
-author("bryan").

%% API
-export([camel_case/1,
         binary_join/2,
         to_binary/1,
         to_list/1,
         to_integer/1,
         to_float/1,
         to_boolean/1,
         to_atom/1,
         to_string/1,
         count_chr/2,
         nullify/1,
         get_values/1,
         spaces/1,
         capitalize/1]).

spaces(Cnt) ->
    lists:flatten([" " || _Cnt <- lists:seq(0, Cnt)]).

-spec get_values([{any(), any()}]) -> [any()].
get_values(KeyValueList) ->
    get_values(KeyValueList, []).

get_values([], Acc) ->
    Acc;
get_values([{_Key, Value} | Rest], Acc) ->
    get_values(Rest, [Value | Acc]).


-spec nullify(list()) -> list().
nullify(Params) ->
    nullify(Params, []).

nullify([], Acc) ->
    lists:reverse(Acc);
nullify([undefined | Rest], Acc) ->
    nullify(Rest, [null | Acc]);
nullify([Param | Rest], Acc) ->
    nullify(Rest, [Param | Acc]).

-spec binary_join([binary()], binary()) -> binary().
binary_join([], _Sep) ->
    <<>>;
binary_join([Part], _Sep) ->
    Part;
binary_join([Head|Tail], Sep) ->
    lists:foldl(fun (Value, Acc) -> <<Acc/binary, Sep/binary, Value/binary>> end, Head, Tail).

-spec camel_case(atom()) -> string().
camel_case(Name) when is_binary(Name) ->
    camel_case(binary_to_list(Name));
camel_case(Name) when is_atom(Name) ->
    camel_case(atom_to_list(Name));
camel_case(Name) when is_list(Name) ->
    %% Convert 'foo_bar' to "FooBar"
    Parts = string:split(Name, "_", all),
    lists:flatten([capitalize(B) || B <- Parts]).

capitalize([C | Rest]) when C >= $a andalso C =< $z ->
    [(C - $a + $A) | Rest];
capitalize(B) ->
    B.

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

count_chr(String, Chr) ->
    F = fun(X, N) when X =:= Chr -> N + 1;
           (_, N) -> N
        end,
    lists:foldl(F, 0, String).

%%
%% Tests
%%
-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

studlycaps_test() ->
    A = camel_case(foo_bar_baz),
    logger:info("A=~p", [A]),
    ?assertEqual("FooBarBaz", A),

    B = camel_case("foo_bar1"),
    logger:info("B=~p", [B]),
    ?assertEqual("FooBar1", B),

    ?assertEqual("FooBar2", camel_case(<<"foo_bar2">>)).

-endif.