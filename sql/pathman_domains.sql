\set VERBOSITY terse

SET search_path = 'public','_gogu';
CREATE EXTENSION gogudb;
CREATE SCHEMA domains;

CREATE DOMAIN domains.dom_test AS numeric CHECK (value < 1200);

CREATE TABLE domains.dom_table(val domains.dom_test NOT NULL);
INSERT INTO domains.dom_table SELECT generate_series(1, 999);

SELECT create_range_partitions('domains.dom_table', 'val', 1, 100);

EXPLAIN (COSTS OFF)
SELECT * FROM domains.dom_table
WHERE val < 250;

INSERT INTO domains.dom_table VALUES(1500);
INSERT INTO domains.dom_table VALUES(-10);

SELECT append_range_partition('domains.dom_table');
SELECT prepend_range_partition('domains.dom_table');
SELECT merge_range_partitions('gogudb_partition_table._domains_1_dom_table', 'gogudb_partition_table._domains_2_dom_table');
SELECT split_range_partition('gogudb_partition_table._domains_1_dom_table', 50);

INSERT INTO domains.dom_table VALUES(1101);

EXPLAIN (COSTS OFF)
SELECT * FROM domains.dom_table
WHERE val < 450;


SELECT * FROM gogudb_partition_list
ORDER BY range_min::INT, range_max::INT;


SELECT drop_partitions('domains.dom_table');
SELECT create_hash_partitions('domains.dom_table', 'val', 5);

SELECT * FROM gogudb_partition_list
ORDER BY "partition"::TEXT;


DROP SCHEMA domains CASCADE;
DROP EXTENSION gogudb CASCADE;

