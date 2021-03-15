%%%-------------------------------------------------------------------
%%% @author bryan
%%% @copyright (C) 2021, <COMPANY>
%%% @doc
%%%
%%% @end
%%% Created : 11. Jan 2021 3:58 PM
%%%-------------------------------------------------------------------
-author("bryan").

-type relation_type() :: zero_one_or_more | many_to_many.
-type index_type() :: primary_key | unique | non_unique.

-record(column, {table_name :: binary() | undefined,
                 table_schema :: binary() | undefined,
                 name :: binary() | undefined,
                 ordinal_position = 0 :: non_neg_integer(),
                 data_type :: binary() | undefined,
                 udt_name :: binary() | undefined,
                 default :: binary() | undefined | null,
                 is_nullable = false :: boolean(),
                 is_sequence = false :: boolean(),
                 is_pkey = false :: boolean(),
                 is_virtual = false :: boolean(),
                 is_excluded = false :: boolean(),
                 is_input = false :: boolean(),
                 is_version = false :: boolean(),
                 select_xform :: binary() | undefined,
                 insert_xform :: binary() | undefined,
                 update_xform :: binary() | undefined,
                 as :: binary() | undefined,
                 valid_values = [] :: list()
}).

%% @doc Describes the return statement
-record(return_value, {name :: binary() | undefined,
                       data_type :: binary() | undefined}).

%% @doc Defines the clause in a statement, such as the SELECT or WHERE clause of a SELECT STATEMENT
-record(clause, {columns = [] :: [#column{}]}).

%% @doc Defines the SQL statement which is made up of clauses, inputs and returning columns
-record(statement, {inputs = [] :: [binary()],
                    clauses = dict:new() :: dict:dict(),                 % Keyed by clause_type() value is #clause{}
                    returning = [] :: [binary()]}).

%% @doc Describes a parsed constraint where Name and Expression are an array of literals
-record(constraint, {name = [] :: [binary()],
                     operator :: binary() | undefined,
                     expressions = [] :: [binary()]}).

%% @doc Describes a function argument as defined by $<name>[::type]
-record(argument, {name :: binary() | undefined,
                   data_type :: binary() | undefined}).

-record(index, {table_name :: binary() | undefined,
                table_schema :: binary() | undefined,
                name :: binary() | undefined,
                type :: index_type() | undefined,
                columns = [] :: [binary()],
                is_list = false :: boolean(),
                is_lookup = false :: boolean(),
                comment :: binary() | undefined}).

%% @doc Describes the column mapping between the foreign and local table
-record(foreign_column, {foreign_name :: binary() | undefined,
                         local_name :: binary() | undefined,
                         ordinal_position = 0 :: non_neg_integer()}).

%% @doc Describes the foreign table relationship
-record(foreign_relation, {foreign_schema :: binary() | undefined,
                           foreign_table :: binary() | undefined,
                           foreign_columns = [] :: [#foreign_column{}],
                           relation_type :: relation_type() | undefined}).

%% @doc Describes the proto field to database column mapping.
-record(proto_map, {field_name :: binary(), column :: #column{}, relation :: #foreign_relation{}}).

-record(table, {name :: binary() | undefined,
                schema :: binary() | undefined,
                columns = orddict:new() :: orddict:orddict(),   % Keyed by column_name
                proto_fields = [] :: [#proto_map{}],            % List of proto_map records
                proto_extension :: string() | undefined,        % Extensions option when generating protobuffers
                indexes = [] :: list(),                         % Keyed by index_name
                relations = [] :: [#foreign_relation{}],
                mappings = orddict:new() :: orddict:orddict(),  % Keyed by query_name
                last_ordinal = 0 :: non_neg_integer(),
                statements = dict:new() :: dict:dict(),         % Keyed by clause_type
                has_valid_values = false :: boolean(),
                has_timestamps = false :: boolean(),
                version_column = <<>> :: binary(),
                sequence :: binary() | undefined,
                select_list = [] :: [binary()],
                insert_list = [] :: [binary()],
                update_list = [] :: [binary()],
                pkey_list = [] :: [binary()],
                default_list = [] :: [binary()]}).

-record(database, {tables = dict:new() :: dict:dict()}).          % If there are multiple schema's then code will be
% generated with the module names prepended with the
% schema name, so test_schema.user will become
% test_schema_user.


