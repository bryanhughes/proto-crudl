%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Feb 2021 2:11 PM
%%%-------------------------------------------------------------------
-module(example_http).
-author("bryan").

%% API
-export([handle/2]).

-include("example_http.hrl").
-include_lib("elli/include/elli.hrl").

handle(Req, _Args) ->
    logger:info("Handling ~0p request: ~0p", [Req#req.method, Req#req.raw_path]),
    DocRoot = list_to_binary(filename:join(code:priv_dir(example), "www")),
    try
        handle_request(Req#req.method, Req, elli_request:peer(Req), DocRoot)
    catch
        Class:What:Stacktrace ->
            HttpReq = #{<<"method">> => example_utils:to_binary(Req#req.method),
                        <<"url">> => Req#req.raw_path,
                        <<"responseStatusCode">> => 500},
            logger:error("~0p:~0p - HttpReq=~0p~nStacktrace:~s", [Class, What, HttpReq, Stacktrace]),
            serve_500(DocRoot)
    end.

serve_500(DocRoot) ->
    Path = filename:join(DocRoot, "500.html"),
    case elli_util:file_size(Path) of
        {error, _Reason} ->
            {500, [{<<"Content-Type">>, <<"text/plain">>}], <<"Handled exception.">>};
        Size ->
            {ok, Content} = file:read_file(Path),
            {500, [{<<"Content-Length">>, Size},
                   {<<"Content-Type">>, ?TYPE_HTML}], Content}
    end.

serve_404(DocRoot) ->
    Path = filename:join(DocRoot, "404.html"),
    case elli_util:file_size(Path) of
        {error, _Reason} ->
            {404, [], <<"Not Found">>};
        Size ->
            {ok, Content} = file:read_file(Path),
            {404, [{<<"Content-Length">>, Size},
                   {<<"Content-Type">>, ?TYPE_HTML}], Content}
    end.

-spec handle_request('GET' | 'POST' | 'PUT' | 'DELETE' | 'OPTIONS', elli_req(), peer(), document_root()) -> elli_resp().
%% @doc This function will handle the standard get and post requests and server the necessary resources.
%%      NOTE: an elli middleware will look for and server a 404.html file from the doc root if the path is not found.
handle_request('OPTIONS', _Req, _Peer, _DocRoot) ->
    {ok, APIVersion} = application:get_key(example_service, vsn),
    {200, [{<<"Access-Control-Allow-Origin">>, <<"*">>},
           {<<"Access-Control-Allow-Methods">>, <<"POST,PUT">>},
           {<<"Access-Control-Allow-Headers">>, ?HEADER_ALLOWED},
           {<<"Access-Control-Expose-Headers">>, ?HEADER_EXPOSED},
           {?HEADER_MAXAGE, ?HEADER_MAXAGE_VALUE},
           {?HEADER_APIVERSION, list_to_binary(APIVersion)}], <<"">>};
handle_request(Method, Req, _Peer, DocRoot) ->
    Path = elli_request:path(Req),
    RawPath = elli_request:raw_path(Req),
    Args = elli_request:get_args(Req),
    logger:info("Handling request. Method=~0p, DocRoot=~0p, Path=~0p, RawPath=~0p, Args=~0p",
                [Method, DocRoot, Path, RawPath, Args]),
    serve_request(Method, Req, RawPath, DocRoot, Path, Args).

serve_request(_Method, _Req, _RawPath, DocRoot, [], _Args) ->
    Index = filename:join(DocRoot, "index.html"),
    logger:info("Serving ~p", [Index]),
    case elli_util:file_size(Index) of
        {error, _Reason} ->
            serve_404(DocRoot);
        Size ->
            {ok, [{<<"Content-Length">>, Size}], {file, Index}}
    end;
serve_request('PUT', Req, Path, DocRoot, [M, F], Args) ->
    %% Otherwise, serve up the file from the document root, check to see if there is a
    %% template that should be served
    Host = elli_request:get_header(<<"Host">>, Req),
    logger:info("Serving request. Path=~0p, M=~0p, F=~0p, Host=~0p, Req=~0p, Args=~0p",
                [DocRoot, Path, M, F, Host, Req, Args]),
    make_response(route(M, F, Req, Args));
serve_request(_Method, _Req, RawPath, DocRoot, PathParts, Args) ->
    logger:info("Serving regular resource. DocRoot=~0p, Path=~0p, Args=~0p", [DocRoot, RawPath, Args]),
    Path = filename:join([DocRoot | PathParts]),
    case elli_util:file_size(Path) of
        {error, _Reason} ->
            serve_404(DocRoot);
        Size ->
            ContentType = content_type(iolist_to_binary(Path)),
            {ok, [{<<"Content-Length">>, Size},
                  {<<"Content-Type">>, ContentType}], {file, Path}}
    end.

route(Module, Function, _Req, Args) ->
    logger:info("Routing service call. Module=~0p, Function=~0p, Args=~0p", [Module, Function, Args]),
    {ok, <<"text/plain">>, <<"POST!">>}.

-spec content_type(binary()) -> binary().
content_type(Filename) ->
    case filename:extension(Filename) of
        <<>> ->
            <<"text/plain">>;
        <<$., Ext/binary>> ->
            mimerl:extension(Ext)
    end.


-spec make_response({status_code(), response_content_type(), response_content()}) -> elli:response().
make_response({Status, ContentType, Response}) ->
    {ok, APIVersion} = application:get_key(example, vsn),
    {Status, [{<<"Content-Type">>, ContentType},
              {?HEADER_APIVERSION, APIVersion},
              {<<"Access-Control-Allow-Origin">>, <<"*">>},
              {<<"Access-Control-Allow-Methods">>, <<"PUT,POST,GET,OPTIONS">>},
              {<<"Access-Control-Allow-Headers">>, ?HEADER_ALLOWED},
              {<<"Access-Control-Expose-Headers">>, ?HEADER_EXPOSED},
              {?HEADER_MAXAGE, ?HEADER_MAXAGE_VALUE}], Response}.