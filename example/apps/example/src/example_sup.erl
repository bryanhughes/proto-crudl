%%%-------------------------------------------------------------------
%% @doc example top level supervisor.
%% @end
%%%-------------------------------------------------------------------

-module(example_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

%% sup_flags() = #{strategy => strategy(),         % optional
%%                 intensity => non_neg_integer(), % optional
%%                 period => pos_integer()}        % optional
%% child_spec() = #{id => child_id(),       % mandatory
%%                  start => mfargs(),      % mandatory
%%                  restart => restart(),   % optional
%%                  shutdown => shutdown(), % optional
%%                  type => worker(),       % optional
%%                  modules => modules()}   % optional
init([]) ->
    logger:info("\n--------------------------------------------- New Launch ---------------------------------------------"),
    logger:info("Starting example supervisor."),
    logger:info("Console log level is currently: ~p", [logger:get_module_level()]),

    SSL = application:get_env(example, ssl, []),
    SSLOptions = {ssl_opts, proplists:get_value(ssl_opts, SSL)},
    logger:info("SSL options: ~p from ~p", [SSLOptions, SSL]),

    Port = application:get_env(example, listen_port, 8080),
    Protocol = application:get_env(example, protocol, "http"),

    logger:info("Starting ~p Listener on port: ~p", [Protocol, Port]),
    HTTPServer = case Protocol of
                     "https" ->
                         web_specs(example_http, example_http, Port, [{ssl, true}, SSLOptions]);
                     "http" ->
                         web_specs(example_http, example_http, Port, [])
                 end,

    SupFlags = #{strategy => one_for_all,
                 intensity => 0,
                 period => 1},
    ChildSpecs = [HTTPServer],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions

web_specs(_Mod, Name, Port, _SSLOptions) ->
    {Name, {elli, start_link, [[{callback, elli_middleware},
                                {callback_args, [{mods, [%% {oc_elli_middleware, []},
                                                         {elli_date, []},
                                                         {example_http, []}]}]},
                                {port, Port},
                                {request_timeout, 65000}]]},
     permanent, 5000, worker, dynamic}.