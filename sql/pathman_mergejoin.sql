\set VERBOSITY terse

SET search_path = 'public','_gogu';
CREATE EXTENSION gogudb ;
CREATE SCHEMA test;


CREATE TABLE test.range_rel (
	id	SERIAL PRIMARY KEY,
	dt	TIMESTAMP NOT NULL,
	txt	TEXT);
CREATE INDEX ON test.range_rel (dt);

INSERT INTO test.range_rel (dt, txt)
SELECT g, md5(g::TEXT) FROM generate_series('2015-01-01', '2015-04-30', '1 day'::interval) as g;

SELECT _gogu.create_range_partitions('test.range_rel', 'DT', '2015-01-01'::DATE, '1 month'::INTERVAL);

CREATE TABLE test.num_range_rel (
	id	SERIAL PRIMARY KEY,
	txt	TEXT);

INSERT INTO test.num_range_rel SELECT g, md5(g::TEXT) FROM generate_series(1, 3000) as g;

SELECT _gogu.create_range_partitions('test.num_range_rel', 'id', 0, 1000, 4);

/*
 * Merge join between 3 partitioned tables
 *
 * test case for the fix of sorting, merge append and index scan issues
 * details in commit 54dd0486fc55b2d25cf7d095f83dee6ff4adee06
 */
SET enable_hashjoin = OFF;
SET enable_nestloop = OFF;
SET enable_mergejoin = ON;

EXPLAIN (COSTS OFF)
SELECT * FROM test.range_rel j1
JOIN test.range_rel j2 on j2.id = j1.id
JOIN test.num_range_rel j3 on j3.id = j1.id
WHERE j1.dt < '2015-03-01' AND j2.dt >= '2015-02-01' ORDER BY j2.dt;

SET enable_hashjoin = ON;
SET enable_nestloop = ON;


DROP SCHEMA test CASCADE;
DROP EXTENSION gogudb;

