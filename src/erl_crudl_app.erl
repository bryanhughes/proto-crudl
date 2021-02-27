%%%-------------------------------------------------------------------
%% @doc erl_crudl public API
%% @end
%%%-------------------------------------------------------------------

-module(erl_crudl_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    erl_crudl_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
