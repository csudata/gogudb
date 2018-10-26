\set VERBOSITY terse

SET search_path = 'public','_gogu';
CREATE EXTENSION gogudb;
CREATE SCHEMA gaps;



CREATE TABLE gaps.test_1(val INT8 NOT NULL);
SELECT create_range_partitions('gaps.test_1', 'val', 1, 10, 3);
DROP TABLE  gogudb_partition_table._gaps_2_test_1;

CREATE TABLE gaps.test_2(val INT8 NOT NULL);
SELECT create_range_partitions('gaps.test_2', 'val', 1, 10, 5);
DROP TABLE  gogudb_partition_table._gaps_3_test_2;

CREATE TABLE gaps.test_3(val INT8 NOT NULL);
SELECT create_range_partitions('gaps.test_3', 'val', 1, 10, 8);
DROP TABLE gogudb_partition_table._gaps_4_test_3;

CREATE TABLE gaps.test_4(val INT8 NOT NULL);
SELECT create_range_partitions('gaps.test_4', 'val', 1, 10, 11);
DROP TABLE gogudb_partition_table._gaps_4_test_4;
DROP TABLE gogudb_partition_table._gaps_5_test_4;



/* Check existing partitions */
SELECT * FROM gogudb_partition_list ORDER BY parent, partition;



/* Pivot values */
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val =  11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val =  16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val =  21;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <  11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <= 11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <  16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <= 16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val <= 21;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >  11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >= 11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >  16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >= 16;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_1 WHERE val >= 21;


/* Pivot values */
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val =  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val =  26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val =  31;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <= 21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <  26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <= 26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val <= 41;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >  11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >= 11;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >= 21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >  26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >= 26;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_2 WHERE val >= 31;


/* Pivot values */
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val =  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val =  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val =  41;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <= 36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <= 41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <  51;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val <= 51;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >= 21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >= 36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_3 WHERE val >= 41;


/* Pivot values */
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val =  51;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  51;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 51;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <  61;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val <= 61;

EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 21;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 31;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 36;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 41;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 46;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >  51;
EXPLAIN (COSTS OFF) SELECT * FROM gaps.test_4 WHERE val >= 51;



DROP SCHEMA gaps CASCADE;

DROP EXTENSION gogudb;


