\set VERBOSITY terse
SET search_path = 'public','_gogu';
CREATE EXTENSION gogudb;



/*
 * Test COPY
 */
CREATE SCHEMA copy_stmt_hooking;

CREATE TABLE copy_stmt_hooking.test(
	val int not null,
	comment text,
	c3 int,
	c4 int);
INSERT INTO copy_stmt_hooking.test SELECT generate_series(1, 20), 'comment';
CREATE INDEX ON copy_stmt_hooking.test(val);


/* test for RANGE partitioning */
SELECT create_range_partitions('copy_stmt_hooking.test', 'val', 1, 5);

/* perform VACUUM */
VACUUM FULL copy_stmt_hooking.test;
VACUUM FULL gogudb_partition_table._copy_stmt_hooking_1_test;
VACUUM FULL gogudb_partition_table._copy_stmt_hooking_2_test;
VACUUM FULL gogudb_partition_table._copy_stmt_hooking_3_test;
VACUUM FULL gogudb_partition_table._copy_stmt_hooking_4_test;

/* DELETE ROWS, COPY FROM */
DELETE FROM copy_stmt_hooking.test;
COPY copy_stmt_hooking.test FROM stdin;
1	test_1	0	0
6	test_2	0	0
7	test_2	0	0
11	test_3	0	0
16	test_4	0	0
\.
SELECT count(*) FROM ONLY copy_stmt_hooking.test;
SELECT *, tableoid::REGCLASS FROM copy_stmt_hooking.test ORDER BY val;

/* perform VACUUM */
VACUUM FULL copy_stmt_hooking.test;
VACUUM FULL gogudb_partition_table._copy_stmt_hooking_1_test;
VACUUM FULL gogudb_partition_table._copy_stmt_hooking_2_test;
VACUUM FULL gogudb_partition_table._copy_stmt_hooking_3_test;
VACUUM FULL gogudb_partition_table._copy_stmt_hooking_4_test;

/* COPY TO */
COPY copy_stmt_hooking.test TO stdout;			/* not ok */
COPY copy_stmt_hooking.test (val) TO stdout;	/* not ok */
COPY (SELECT * FROM copy_stmt_hooking.test) TO stdout;
COPY (SELECT * FROM copy_stmt_hooking.test) TO stdout (FORMAT CSV);
\copy (SELECT * FROM copy_stmt_hooking.test) TO stdout

/* COPY FROM (partition does not exist, NOT allowed to create partitions) */
SET gogudb.enable_auto_partition = OFF;
COPY copy_stmt_hooking.test FROM stdin;
21	test_no_part	0	0
\.
SELECT * FROM copy_stmt_hooking.test WHERE val > 20;

/* COPY FROM (partition does not exist, allowed to create partitions) */
SET gogudb.enable_auto_partition = ON;
COPY copy_stmt_hooking.test FROM stdin;
21	test_no_part	0	0
\.
SELECT * FROM copy_stmt_hooking.test WHERE val > 20;

/* COPY FROM (partitioned column is not specified) */
COPY copy_stmt_hooking.test(comment) FROM stdin;
test_no_part
\.

/* COPY FROM (we don't support FREEZE) */
COPY copy_stmt_hooking.test FROM stdin WITH (FREEZE);


/* Drop column (make use of 'tuple_map') */
ALTER TABLE copy_stmt_hooking.test DROP COLUMN comment;


/* create new partition */
SELECT get_number_of_partitions('copy_stmt_hooking.test');
INSERT INTO copy_stmt_hooking.test (val, c3, c4) VALUES (26, 1, 2);
SELECT get_number_of_partitions('copy_stmt_hooking.test');

/* check number of columns in 'test' */
SELECT count(*) FROM pg_attribute
WHERE attnum > 0 AND attrelid = 'copy_stmt_hooking.test'::REGCLASS;

/* check number of columns in 'test_6' */
SELECT count(*) FROM pg_attribute
WHERE attnum > 0 AND attrelid = 'gogudb_partition_table._copy_stmt_hooking_6_test'::REGCLASS;

/* test transformed tuples */
COPY (SELECT * FROM copy_stmt_hooking.test) TO stdout;


/* COPY FROM (insert into table with dropped column) */
COPY copy_stmt_hooking.test(val, c3, c4) FROM stdin;
2	1	2
\.

/* COPY FROM (insert into table without dropped column) */
COPY copy_stmt_hooking.test(val, c3, c4) FROM stdin;
27	1	2
\.

/* check tuples from last partition (without dropped column) */
SELECT *, tableoid::REGCLASS FROM copy_stmt_hooking.test ORDER BY val;


/* drop modified table */
DROP TABLE copy_stmt_hooking.test CASCADE;


/* create table again */
CREATE TABLE copy_stmt_hooking.test(
	val int not null,
	comment text,
	c3 int,
	c4 int);
CREATE INDEX ON copy_stmt_hooking.test(val);


/* test for HASH partitioning */
SELECT create_hash_partitions('copy_stmt_hooking.test', 'val', 5);

/* DELETE ROWS, COPY FROM */
DELETE FROM copy_stmt_hooking.test;
COPY copy_stmt_hooking.test FROM stdin;
1	hash_1	0	0
6	hash_2	0	0
\.
SELECT count(*) FROM ONLY copy_stmt_hooking.test;
SELECT * FROM copy_stmt_hooking.test ORDER BY val;

/* Check dropped colums before partitioning */
CREATE TABLE copy_stmt_hooking.test2 (
	a varchar(50),
	b varchar(50),
	t timestamp without time zone not null
);
ALTER TABLE copy_stmt_hooking.test2 DROP COLUMN a;
SELECT create_range_partitions('copy_stmt_hooking.test2',
	't',
	'2017-01-01 00:00:00'::timestamp,
	interval '1 hour', 5, false
);
COPY copy_stmt_hooking.test2(t) FROM stdin;
2017-02-02 20:00:00
\.
SELECT COUNT(*) FROM copy_stmt_hooking.test2;

DROP SCHEMA copy_stmt_hooking CASCADE;



/*
 * Test auto check constraint renaming
 */
CREATE SCHEMA rename;


/*
 * Check that auto naming sequence is renamed
 */
CREATE TABLE rename.parent(id int not null);
SELECT create_range_partitions('rename.parent', 'id', 1, 2, 2);
SELECT 'rename.parent'::regclass;		/* parent is OK */
SELECT 'rename.parent_seq'::regclass;	/* sequence is OK */
ALTER TABLE rename.parent RENAME TO parent_renamed;
SELECT 'rename.parent_renamed'::regclass;		/* parent is OK */
SELECT 'rename.parent_renamed_seq'::regclass;	/* sequence is OK */
SELECT append_range_partition('rename.parent_renamed'); /* can append */
DROP SEQUENCE rename.parent_renamed_seq;
ALTER TABLE rename.parent_renamed RENAME TO parent;
SELECT 'rename.parent'::regclass;		/* parent is OK */

/*
 * Check that partitioning constraints are renamed
 */
CREATE TABLE rename.test(a serial, b int);
SELECT create_hash_partitions('rename.test', 'a', 3);
ALTER TABLE gogudb_partition_table._rename_0_test RENAME TO test_one;
/* We expect to find check constraint renamed as well */
SELECT r.conname, pg_get_constraintdef(r.oid, true)
FROM pg_constraint r
WHERE r.conrelid = 'gogudb_partition_table.test_one'::regclass AND r.contype = 'c';

/* Generates check constraint for relation */
CREATE OR REPLACE FUNCTION add_constraint(rel regclass)
RETURNS VOID AS $$
declare
	constraint_name text := build_check_constraint_name(rel);
BEGIN
	EXECUTE format('ALTER TABLE %s ADD CONSTRAINT %s CHECK (a < 100);',
				   rel, constraint_name);
END
$$
LANGUAGE plpgsql;

/*
 * Check that it doesn't affect regular inherited
 * tables that aren't managed by gogudb
 */
CREATE TABLE rename.test_inh (LIKE rename.test INCLUDING ALL);
CREATE TABLE rename.test_inh_1 (LIKE rename.test INCLUDING ALL);
ALTER TABLE rename.test_inh_1 INHERIT rename.test_inh;
SELECT add_constraint('rename.test_inh_1');
ALTER TABLE rename.test_inh_1 RENAME TO test_inh_one;
/* Show check constraints of rename.test_inh_one */
SELECT r.conname, pg_get_constraintdef(r.oid, true)
FROM pg_constraint r
WHERE r.conrelid = 'rename.test_inh_one'::regclass AND r.contype = 'c';

/*
 * Check that plain tables are not affected too
 */
CREATE TABLE rename.plain_test(a serial, b int);
ALTER TABLE rename.plain_test RENAME TO plain_test_renamed;
SELECT add_constraint('rename.plain_test_renamed');
/* Show check constraints of rename.plain_test_renamed */
SELECT r.conname, pg_get_constraintdef(r.oid, true)
FROM pg_constraint r
WHERE r.conrelid = 'rename.plain_test_renamed'::regclass AND r.contype = 'c';

ALTER TABLE rename.plain_test_renamed RENAME TO plain_test;
/* ... and check constraints of rename.plain_test */
SELECT r.conname, pg_get_constraintdef(r.oid, true)
FROM pg_constraint r
WHERE r.conrelid = 'rename.plain_test'::regclass AND r.contype = 'c';


DROP SCHEMA rename CASCADE;



/*
 * Test DROP INDEX CONCURRENTLY (test snapshots)
 */
CREATE SCHEMA drop_index;

CREATE TABLE drop_index.test (val INT4 NOT NULL);
CREATE INDEX ON drop_index.test (val);
SELECT create_hash_partitions('drop_index.test', 'val', 2);
DROP INDEX CONCURRENTLY drop_index.test_0_val_idx;

DROP SCHEMA drop_index CASCADE;



DROP EXTENSION gogudb;


