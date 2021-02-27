/* Get the join tables for many to many relationships starting with a left table */
SELECT DISTINCT
	j_tc.constraint_schema join_schema,
	j_tc.table_name join_table
FROM
	-- The left table primary key constraints
	information_schema.table_constraints l_tc
	JOIN information_schema.key_column_usage l_kcu ON 
		-- The left table key column usage
		l_kcu.table_name = l_tc.table_name AND
		l_kcu.table_schema = l_tc.table_schema AND
		l_kcu.constraint_name = l_tc.constraint_name
	JOIN information_schema.referential_constraints l_rc ON  
		-- Bridges left table to join table
		l_rc.unique_constraint_name = l_tc.constraint_name AND
		l_rc.unique_constraint_schema = l_tc.constraint_schema
	JOIN information_schema.table_constraints j_tc ON
		-- The join table foreign key constraints 
		j_tc.constraint_type = 'FOREIGN KEY' AND
		j_tc.constraint_name = l_rc.constraint_name AND
		j_tc.constraint_schema = l_rc.constraint_schema
	JOIN information_schema.table_constraints jp_tc ON
		-- The join table primary key constraint
		jp_tc.constraint_schema = j_tc.constraint_schema AND
		jp_tc.constraint_type = 'PRIMARY KEY' AND
		jp_tc.table_name = j_tc.table_name 
 	JOIN information_schema.key_column_usage j_kcu ON 
		-- The join table key column usage
		j_kcu.constraint_name = jp_tc.constraint_name AND
		j_kcu.table_schema = jp_tc.table_schema AND
		j_kcu.table_name = jp_tc.table_name
	JOIN information_schema.table_constraints r_tc ON
		-- Thr join table foreign key constraints
		r_tc.constraint_type = 'FOREIGN KEY' AND
		r_tc.table_name = j_tc.table_name AND
		r_tc.table_schema = j_tc.table_schema
	JOIN information_schema.key_column_usage r_kcu ON
		-- The right table foreign key constraint name
		r_tc.constraint_schema = r_kcu.constraint_schema AND
		r_tc.constraint_name = r_kcu.constraint_name
	JOIN information_schema.referential_constraints r_rc ON
		-- Rith right table foreign key referential constraint
		r_tc.constraint_schema = r_rc.constraint_schema AND
		r_tc.constraint_name = r_rc.constraint_name
	JOIN information_schema.key_column_usage r_rel_kcu ON
		r_rc.unique_constraint_schema = r_rel_kcu.constraint_schema AND
		r_rc.unique_constraint_name = r_rel_kcu.constraint_name AND
		r_kcu.ordinal_position = r_rel_kcu.ordinal_position
WHERE
	l_tc.constraint_type = 'PRIMARY KEY'
	AND l_tc.table_schema = 'public'
	AND l_tc.table_name = 'product'
	AND j_kcu.column_name = r_rel_kcu.column_name
ORDER BY
	join_schema, join_table
	
	