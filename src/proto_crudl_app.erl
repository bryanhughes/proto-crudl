%%%-------------------------------------------------------------------
%% @doc proto_crudl public API
%% @end
%%%-------------------------------------------------------------------

-module(proto_crudl_app).

-behaviour(application).

-export([start/2, stop/1]).

start(_StartType, _StartArgs) ->
    proto_crudl_sup:start_link().

stop(_State) ->
    ok.

%% internal functions
