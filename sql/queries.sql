/* Get referencing tables and keys - these are all the parent tables we are referencing */
SELECT
	rel_kcu.table_schema as primary_schema,
	rel_kcu.table_name as primary_table,
	kcu.column_name as fk_column,
	kcu.constraint_name,
	(SELECT
		CASE WHEN count(*) > 1 THEN true
			 ELSE false
		END
	 FROM
		information_schema.key_column_usage kcu1
	 WHERE
		kcu1.table_schema = kcu.table_schema AND
		kcu1.table_name = kcu.table_name AND
		kcu1.constraint_name = kcu.constraint_name) is_composite
FROM
	information_schema.table_constraints tco
	JOIN information_schema.key_column_usage kcu
		  ON tco.constraint_schema = kcu.constraint_schema
		  AND tco.constraint_name = kcu.constraint_name
	JOIN information_schema.referential_constraints rco
		  ON tco.constraint_schema = rco.constraint_schema
		  AND tco.constraint_name = rco.constraint_name
	JOIN information_schema.key_column_usage rel_kcu
		  ON rco.unique_constraint_schema = rel_kcu.constraint_schema
		  AND rco.unique_constraint_name = rel_kcu.constraint_name
		  AND kcu.ordinal_position = rel_kcu.ordinal_position
WHERE
	tco.constraint_type = 'FOREIGN KEY' AND
	kcu.table_schema = 'public' AND
	kcu.table_name = 'part_part'
ORDER BY primary_schema, primary_table, rel_kcu.column_name, kcu.constraint_name;

/* Get referenced by tables - these are all the child tables that are referencing us */
SELECT
	tc.constraint_schema reference_schema,
	tc.table_name reference_table,
	ftc.constraint_schema referencing_schema,
	ftc.table_name referencing_table
FROM
	information_schema.referential_constraints rc,
	information_schema.table_constraints tc,
	information_schema.table_constraints ftc
WHERE
	tc.constraint_type = 'PRIMARY KEY' AND
	tc.constraint_name = rc.unique_constraint_name AND
	tc.constraint_schema = rc.unique_constraint_schema AND
	ftc.constraint_type = 'FOREIGN KEY' AND
	ftc.constraint_name = rc.constraint_name AND
	ftc.constraint_schema = rc.constraint_schema;

/* Get referenced tables - these are all the child tables that are referencing us and note those that are join tables */
SELECT
	tc.constraint_schema reference_schema,
	tc.table_name reference_table,
	ftc.constraint_schema referencing_schema,
	ftc.table_name referencing_table,
	(SELECT 
		CASE WHEN count(p_kcu.column_name) = count(f_kcu.column_name) AND count(p_kcu.column_name) > 0 THEN true
			 ELSE false
		END
	FROM
		information_schema.key_column_usage p_kcu,
		information_schema.table_constraints p_tc,
		information_schema.key_column_usage f_kcu,
		information_schema.table_constraints f_tc
	WHERE
	 	p_tc.constraint_schema = ftc.constraint_schema AND
		p_tc.constraint_type = 'PRIMARY KEY' AND
		p_tc.constraint_name = p_kcu.constraint_name AND
		p_tc.table_schema = p_kcu.table_schema AND
		p_tc.table_name = p_kcu.table_name AND
		p_tc.table_name = ftc.table_name AND
		f_tc.constraint_type = 'FOREIGN KEY' AND
		f_tc.constraint_name = f_kcu.constraint_name AND
		f_tc.table_schema = f_kcu.table_schema AND
		f_tc.table_name = f_kcu.table_name AND
		f_tc.table_name = ftc.table_name AND
		p_kcu.column_name = f_kcu.column_name) is_join
FROM
	information_schema.referential_constraints rc,
	information_schema.table_constraints tc,
	information_schema.table_constraints ftc
WHERE
	tc.constraint_type = 'PRIMARY KEY' AND
	tc.constraint_name = rc.unique_constraint_name AND
	tc.constraint_schema = rc.unique_constraint_schema AND
	ftc.constraint_type = 'FOREIGN KEY' AND
	ftc.constraint_name = rc.constraint_name AND
	ftc.constraint_schema = rc.constraint_schema AND
	tc.table_schema = 'public' AND
	tc.table_name = 'product'
	