%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 25. Feb 2021 2:07 PM
%%%-------------------------------------------------------------------
-author("bryan").

-ifndef(EXAMPLE_HTTP_HRL).
-define(EXAMPLE_HTTP_HRL, true).

-type header_key() :: atom() | string() | binary().
-type header_value() :: atom() | string() | binary() | integer().
-type document_root() :: file:name_all().
-type query_string() :: [string()].
-type server_return() :: string().

-type elli_req() :: any().
-type elli_resp() :: ignore | {status_code(), [tuple()], binary()} | {ok, [tuple()], binary()}.

-type http_headers() :: [{atom(), binary()}].
-type peer() :: binary().
-type response_content_type() :: string().
-type response_content() :: any().
-type status_code() :: non_neg_integer().
-type service_response() :: {status_code(), response_content()}.

%%
%% Headers
%%
-define(HEADER_APIVERSION, <<"X-Example-Api-Version">>).
-define(HEADER_PEER, <<"X-Example-Peer">>).
-define(HEADER_APIKEY, <<"X-Example-Api-Key">>).
-define(HEADER_APITOKEN, <<"X-Example-Api-Token">>).
-define(HEADER_ACCOUNT_ID, <<"X-Example-Account-Id">>).
-define(HEADER_ACCOUNT_TOKEN, <<"X-Example-Account-Token">>).
-define(HEADER_USER_EMAIL, <<"X-Example-User-Email">>).
-define(HEADER_USERID, <<"X-Example-User-Id">>).
-define(HEADER_USER_TOKEN, <<"X-Example-User-Token">>).
-define(HEADER_USER_PASSWORD, <<"X-Example-User-Password">>).
-define(HEADER_ALLOWED, <<"Origin,Content-Type,Content-Disposition,Accept,"
                          "X-Example-Api-Key,"
                          "X-Example-Api-Token,"
                          "X-Example-User-Id,"
                          "X-Example-User-Token,"
                          "X-Example-User-Password">>).
-define(HEADER_EXPOSED, <<"X-Example-Api-Version">>).
-define(HEADER_MAXAGE, <<"Access-Control-Max-Age">>).
-define(HEADER_MAXAGE_VALUE, <<"600">>).

%%
%% Content Types
%%
-define(TYPE_MULTIPART, <<"multipart/related">>).
-define(TYPE_JSON, <<"application/json">>).
-define(TYPE_IMAGE_JPEG, <<"image/jpeg">>).
-define(TYPE_IMAGE_PNG, <<"image/png">>).
-define(TYPE_HTML, <<"text/html">>).
-define(TYPE_PLAIN, <<"text/plain">>).
-define(TYPE_CSV, <<"text/csv">>).
-define(TYPE_FILE, <<"text/file">>).
-define(TYPE_DRAW, <<"application/dwg">>).
-define(TYPE_OCTECT_STREAM, <<"application/octet-stream">>).
-define(TYPE_X_PROTOBUF, <<"application/x-protobuf">>).
-define(TYPE_FORM_URLENCODED, <<"application/x-www-form-urlencoded">>).

-define(MAX_RECV_BODY, ((1024*1024) * 30)).

-endif.
