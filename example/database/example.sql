CREATE SCHEMA IF NOT EXISTS "public";

CREATE SCHEMA IF NOT EXISTS test_schema;

CREATE  TABLE "public".example_a ( 
	column_a             varchar  NOT NULL  ,
	column_b             varchar  NOT NULL  ,
	column_c             integer    ,
	column_d             integer[]    ,
	column_e             varchar[]    ,
	column_f             text[]    ,
	column_g             bool[]    ,
	column_h             json[]    ,
	column_i             varchar  NOT NULL  ,
	CONSTRAINT pk_example_a PRIMARY KEY ( column_a, column_b ),
	CONSTRAINT unq_example_a UNIQUE ( column_i ) 
 );

CREATE INDEX idx_example_a ON "public".example_a  ( column_c );

CREATE  TABLE "public".excluded ( 
	column1              integer  NOT NULL  ,
	column2              integer  NOT NULL  ,
	column3              varchar    ,
	CONSTRAINT pk_public_excluded PRIMARY KEY ( column1, column2 )
 );

CREATE  TABLE "public".foo ( 
	bar                  varchar  NOT NULL  ,
	baz                  varchar    ,
	ignore_me            timestamp    ,
	CONSTRAINT pk_test_schema_foo_bar PRIMARY KEY ( bar )
 );

CREATE  TABLE "public".part ( 
	part_id              uuid DEFAULT uuid_generate_v1mc() NOT NULL  ,
	part_name            varchar(100)    ,
	CONSTRAINT pk_public_parts_part_id PRIMARY KEY ( part_id )
 );

CREATE  TABLE "public".part_part ( 
	part_id              uuid  NOT NULL  ,
	child_part_id        uuid  NOT NULL  ,
	CONSTRAINT pk_public_part_part PRIMARY KEY ( part_id, child_part_id )
 );

CREATE  TABLE "public".product ( 
	product_id           integer  NOT NULL GENERATED ALWAYS AS IDENTITY ,
	product_name         varchar(100)    ,
	sku                  varchar(100)    ,
	produced             timestamptz  NOT NULL  ,
	id                   int    ,
	modified             timestamp    ,
	CONSTRAINT pk_public_table_id PRIMARY KEY ( product_id ),
	CONSTRAINT lookup_sku UNIQUE ( sku ) 
 );

CREATE INDEX idx_product ON "public".product  ( id );

CREATE  TABLE "public".product_parts ( 
	product_id           int  NOT NULL  ,
	part_id              uuid  NOT NULL  ,
	CONSTRAINT pk_public_product_parts PRIMARY KEY ( product_id, part_id )
 );

CREATE  TABLE "public".example_b ( 
	column_a             varchar  NOT NULL  ,
	column_b1            varchar  NOT NULL  ,
	column_1             integer  NOT NULL  ,
	test_id              integer    ,
	p_bar                varchar    ,
	t_bar                varchar  NOT NULL  ,
	CONSTRAINT pk_example_b_column_1 PRIMARY KEY ( column_1 ),
	CONSTRAINT unq_example_b_column_a UNIQUE ( column_a ) 
 );

CREATE INDEX list_by_public_foo ON "public".example_b  ( p_bar );

CREATE  TABLE "public".example_c ( 
	column_aa            integer  NOT NULL GENERATED BY DEFAULT AS IDENTITY ,
	column_a             varchar  NOT NULL  ,
	CONSTRAINT pk_example_c_column_aa PRIMARY KEY ( column_aa )
 );

CREATE  TABLE test_schema.address ( 
	address_id           integer  NOT NULL GENERATED ALWAYS AS IDENTITY ,
	address1             varchar(100)    ,
	address2             varchar(100)    ,
	city                 varchar    ,
	"state"              varchar    ,
	country              char(2)    ,
	postcode             varchar    ,
	notes                text    ,
	CONSTRAINT pk_address_address_d PRIMARY KEY ( address_id ),
	CONSTRAINT unq_address UNIQUE ( address1, address2, city, "state", postcode, country ) 
 );

CREATE  TABLE test_schema.foo ( 
	bar                  varchar  NOT NULL  ,
	baz                  varchar    ,
	CONSTRAINT pk_test_schema_foo_bar PRIMARY KEY ( bar )
 );

CREATE  TABLE test_schema.test_table_no_pkey ( 
	bigint_col           bigint    ,
	bigint_array_col     bigint[]    ,
	big_serial_col       bigserial    ,
	bool_col             boolean    ,
	bytea_col            bytea    ,
	char_col             char(100)    ,
	cidr_col             cidr    ,
	date_col             date DEFAULT current_date   ,
	float8_col           double precision    ,
	inet_col             inet    ,
	integer_col          integer    ,
	integer_array_col    integer[]    ,
	json_col             json    ,
	numeric_precision_col numeric(9,4)    ,
	numeric_col          numeric    ,
	real_col             real    ,
	serial_col           serial    ,
	smallint_col         smallint    ,
	smallint_array_col   smallint[]    ,
	smallserial_col      smallserial    ,
	text_col             text    ,
	time_col             time DEFAULT current_time   ,
	timestamp_col        timestamp DEFAULT current_timestamp   ,
	timestampz_col       timestamptz DEFAULT current_timestamp   ,
	uuid_col             uuid    ,
	varchar_col          varchar    ,
	varchar_length_col   varchar(256)    ,
	xml_col              xml    ,
	int_col              int    ,
	decimal_col          decimal    ,
	jsonb_col            jsonb  NOT NULL  
 );

CREATE  TABLE test_schema.test_table_pkey ( 
	bigint_col           bigint    ,
	bigint_array_col     bigint[]    ,
	big_serial_col       bigserial    ,
	bool_col             boolean    ,
	bytea_col            bytea    ,
	char_col             char(100)    ,
	cidr_col             cidr    ,
	date_col             date DEFAULT current_date   ,
	float8_col           double precision    ,
	inet_col             inet    ,
	integer_col          integer    ,
	integer_array_col    integer[]    ,
	json_col             json    ,
	numeric_precision_col numeric(9,4)    ,
	numeric_col          numeric    ,
	real_col             real    ,
	serial_col           serial    ,
	smallint_col         smallint    ,
	smallint_array_col   smallint[]    ,
	smallserial_col      smallserial    ,
	text_col             text    ,
	time_col             time DEFAULT current_time   ,
	timestamp_col        timestamp DEFAULT current_timestamp   ,
	timestampz_col       timestamptz DEFAULT current_timestamp   ,
	uuid_col             uuid    ,
	varchar_col          varchar    ,
	varchar_length_col   varchar(256)    ,
	xml_col              xml    ,
	int_col              int    ,
	decimal_col          decimal    ,
	id                   integer  NOT NULL GENERATED ALWAYS AS IDENTITY ,
	jsonb_col            jsonb    ,
	text_array_col       text[]    ,
	bool_array_col       bool[]    ,
	CONSTRAINT pk_test_table_no_pkey_0_id PRIMARY KEY ( id )
 );

CREATE  TABLE test_schema."user" ( 
	user_id              bigint  NOT NULL GENERATED BY DEFAULT AS IDENTITY ,
	first_name           varchar(100)    ,
	last_name            varchar(100)    ,
	email                varchar  NOT NULL  ,
	geog                 geography(point)    ,
	pword_hash           bytea    ,
	user_token           uuid DEFAULT uuid_generate_v1()   ,
	enabled              boolean DEFAULT true NOT NULL  ,
	aka_id               bigint    ,
	my_array             integer[]  NOT NULL  ,
	user_type            varchar  NOT NULL  ,
	number_value         integer  NOT NULL  ,
	created_on           timestamp DEFAULT current_timestamp NOT NULL  ,
	updated_on           timestamp    ,
	due_date             date    ,
	user_state           varchar    ,
	user_state_type      boolean DEFAULT true   ,
	CONSTRAINT lookup_email UNIQUE ( email ) ,
	CONSTRAINT pk_user PRIMARY KEY ( user_id )
 );

ALTER TABLE test_schema."user" ADD CONSTRAINT check_user_type CHECK ( user_type IN ('BIG SHOT', 'LITTLE-SHOT', 'BUSY_GUY', 'BUSYGAL', '123FUN') );

ALTER TABLE test_schema."user" ADD CONSTRAINT check_user_number_value CHECK ( number_value > 1 );

ALTER TABLE test_schema."user" ADD CONSTRAINT check_user_state CHECK ( user_state IN ('unknown', 'living', 'deceased') );

CREATE INDEX list_by_name ON test_schema."user"  ( first_name, last_name );

CREATE INDEX list_by_enabled ON test_schema."user"  ( enabled );

CREATE INDEX list_by_user_type_enabled ON test_schema."user"  ( enabled, user_type );

CREATE  TABLE test_schema.user_product_part ( 
	user_id              bigint  NOT NULL  ,
	product_id           integer  NOT NULL  ,
	part_id              uuid  NOT NULL  ,
	inserted_on          bigint  NOT NULL  ,
	CONSTRAINT pk_user_product_part PRIMARY KEY ( user_id, product_id, part_id ),
	CONSTRAINT unq_user_product_part_product_id UNIQUE ( product_id ) ,
	CONSTRAINT unq_user_product_part_user_id UNIQUE ( user_id ) 
 );

ALTER TABLE "public".example_b ADD CONSTRAINT fk_example_b_example_a FOREIGN KEY ( column_a, column_b1 ) REFERENCES "public".example_a( column_a, column_b );

ALTER TABLE "public".example_b ADD CONSTRAINT fk_example_b_test_table_pkey FOREIGN KEY ( test_id ) REFERENCES test_schema.test_table_pkey( id );

ALTER TABLE "public".example_b ADD CONSTRAINT fk_example_b_pfoo FOREIGN KEY ( p_bar ) REFERENCES "public".foo( bar );

ALTER TABLE "public".example_b ADD CONSTRAINT fk_example_b_tfoo FOREIGN KEY ( t_bar ) REFERENCES test_schema.foo( bar );

ALTER TABLE "public".example_c ADD CONSTRAINT fk_example_c_example_b FOREIGN KEY ( column_a ) REFERENCES "public".example_b( column_a );

ALTER TABLE "public".part_part ADD CONSTRAINT fk_public_part_part_public_part FOREIGN KEY ( part_id ) REFERENCES "public".part( part_id );

ALTER TABLE "public".part_part ADD CONSTRAINT fk_public_part_part_public_part_0 FOREIGN KEY ( child_part_id ) REFERENCES "public".part( part_id );

ALTER TABLE "public".product ADD CONSTRAINT fk_public_product_test_table_pkey FOREIGN KEY ( id ) REFERENCES test_schema.test_table_pkey( id );

ALTER TABLE "public".product_parts ADD CONSTRAINT fk_parts_product FOREIGN KEY ( product_id ) REFERENCES "public".product( product_id );

ALTER TABLE "public".product_parts ADD CONSTRAINT fk_product_parts_part FOREIGN KEY ( part_id ) REFERENCES "public".part( part_id );

ALTER TABLE test_schema."user" ADD CONSTRAINT fk_user_user FOREIGN KEY ( aka_id ) REFERENCES test_schema."user"( user_id );

ALTER TABLE test_schema.user_product_part ADD CONSTRAINT fk_user_product_part_product FOREIGN KEY ( product_id ) REFERENCES "public".product( product_id );

ALTER TABLE test_schema.user_product_part ADD CONSTRAINT fk_user_product_part_user FOREIGN KEY ( user_id ) REFERENCES test_schema."user"( user_id );

ALTER TABLE test_schema.user_product_part ADD CONSTRAINT fk_user_product_part_part FOREIGN KEY ( part_id ) REFERENCES "public".part( part_id );

COMMENT ON TABLE "public".example_a IS 'If generated by DBSchema, then SQL file needs to be fixed up. The ''[]'' needs to be part of the datatype (no spaces)';

COMMENT ON TABLE "public".part_part IS 'This table represents a many to many nested / recursive relationship. This should result in a repeated message.';

COMMENT ON INDEX "public".idx_product IS 'This index will not generate an accessor method.';

COMMENT ON CONSTRAINT lookup_sku ON "public".product IS 'Any index that is prefixed with ''lookup_'' will become an accessor methodin the code generated by go_dbmap.';

COMMENT ON TABLE "public".product IS 'This table represents a top level entity with an identifying primary key.\n\n#service:product';

COMMENT ON TABLE "public".product_parts IS 'This table represents a many to many relationship between two different entities.\n\n#service: product';

COMMENT ON COLUMN "public".example_c.column_a IS 'This shows a weird corner case where a column that is part of a composite foreign key is referencing another table as a single column';

COMMENT ON COLUMN test_schema.address.notes IS 'This is NOT included in the upsert constraint and should be the only field that gets updated on an UPSERT';

COMMENT ON TABLE test_schema.test_table_no_pkey IS 'This table will enumerate every postgres data type as of 9.5';

COMMENT ON TABLE test_schema.test_table_pkey IS 'This table will enumerate every postgres data type as of 9.5 with a primary key';

COMMENT ON CONSTRAINT lookup_email ON test_schema."user" IS 'Any index that is prefixed with ''lookup_'' will become an accessor methodin the code generated by go_dbmap.';

COMMENT ON INDEX test_schema.list_by_user_type_enabled IS '+ORDER BY enabled DESC';

COMMENT ON TABLE test_schema."user" IS '+ORDER BY last_name DESC\n\nThis table will help test secondary lookup via email.\n\n#service: user';

COMMENT ON TABLE test_schema.user_product_part IS '#service: user';

COMMENT ON COLUMN test_schema.user_product_part.inserted_on IS 'The UNIX epoch';

