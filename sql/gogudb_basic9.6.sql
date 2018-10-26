SET search_path = 'public','_gogu';
CREATE EXTENSION gogudb;
DO $d$
    BEGIN
        EXECUTE $$CREATE SERVER server_remote1 FOREIGN DATA WRAPPER gogudb_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
        EXECUTE $$CREATE SERVER server_remote2 FOREIGN DATA WRAPPER gogudb_fdw
            OPTIONS (dbname '$$||current_database()||$$',
                     port '$$||current_setting('port')||$$'
            )$$;
    END;
$d$;

CREATE USER MAPPING FOR CURRENT_USER SERVER server_remote1;
CREATE USER MAPPING FOR CURRENT_USER SERVER server_remote2;

insert into server_map values('server_remote1', 0, 64), ('server_remote2', 64, 128);
select reload_range_server_set();

/* expect warning, but still can create hash partition on server_remote1 and server_remote2 */
insert into server_map values('err_server_remote3', 0, 64), ('err_server_remote4', 64, 128);
select reload_range_server_set();

/* support range partitioned by time column */
insert into  table_partition_rule(schema_name ,table_name ,part_expr ,part_type ,range_interval ,
				range_start ,part_dist, remote_schema) 
	values('public', 'part_range_time_test', 'crt_time', 2, '2 month','2018-1-1 00:00:0', 6, 'public');

/* support range partitioned by time column */
insert into table_partition_rule(schema_name, table_name, part_expr, part_type, part_dist, remote_schema,
				range_interval,range_start) 
	values('public', 'part_range_num_test', 'id', 2, 4, 'public', '100', '0');

/* support hash partitioned */
insert into table_partition_rule(schema_name, table_name, part_expr, part_type, part_dist, remote_schema)
	 values('public', 'part_hash_test', 'id', 1,4,'public');

CREATE TABLE part_range_time_test(id int, info text, crt_time timestamp not null);
CREATE TABLE part_range_num_test ( id integer NOT NULL, k integer DEFAULT 0 NOT NULL);
CREATE TABLE part_hash_test(id INT NOT NULL, payload REAL);
\d+ part_range_num_test
\d+ part_range_time_test
\d+ part_hash_test

CREATE INDEX crt_time_index ON part_range_time_test(crt_time);
CREATE INDEX k_index ON part_range_num_test(k);
CREATE INDEX id_index ON part_hash_test(id);

INSERT INTO part_range_time_test SELECT id,md5(random()::text),clock_timestamp() + (id||' hour')::interval 
			FROM generate_series(1,10000) t(id);
INSERT INTO part_range_num_test SELECT id, random() FROM generate_series(1,399) t(id);
INSERT INTO part_hash_test SELECT id, random() FROM generate_series(1,100) t(id);
SELECT COUNT(*) FROM part_range_time_test;
SELECT COUNT(*) FROM part_range_num_test;
SELECT COUNT(*) FROM part_hash_test;

SELECT count(*) FROM gogudb_partition_table._public_0_part_hash_test;
SELECT count(*) FROM gogudb_partition_table._public_1_part_hash_test;
SELECT count(*) FROM gogudb_partition_table._public_2_part_hash_test;
SELECT count(*) FROM gogudb_partition_table._public_3_part_hash_test;

explain (COSTS OFF) select * from part_range_time_test;
explain (COSTS OFF) select * from part_range_num_test;
explain (COSTS OFF) select * from part_hash_test;
explain (COSTS OFF) select * from part_hash_test where id = 1;
explain (COSTS OFF) select * from part_hash_test where id = 2;
explain (COSTS OFF) select * from part_hash_test where id = 3;
explain (COSTS OFF) select * from part_hash_test where id = 4;
explain (COSTS OFF) select * from part_hash_test where id = 5;
explain (COSTS OFF) select * from part_hash_test where id = 6;
explain (COSTS OFF) select * from part_hash_test where id = 7;
explain (COSTS OFF) select * from part_hash_test where id = 8;
explain (COSTS OFF) select * from part_hash_test where id > 9;
explain (COSTS OFF) select * from part_hash_test where id < 10;
explain (COSTS OFF) select * from part_hash_test where id != 11;
explain (COSTS OFF) select * from part_hash_test where id >= 9;
explain (COSTS OFF) select * from part_hash_test where id <= 10;
explain (COSTS OFF) select * from part_hash_test where id >= 20 and id <= 100;
explain (COSTS OFF) select * from part_hash_test where id <= 10 or id >= 50;

PREPARE q1(int) AS
	select id from part_hash_test where id = $1;

EXECUTE q1(1);
EXECUTE q1(2);
EXECUTE q1(3);
EXECUTE q1(4);
EXECUTE q1(5);
EXECUTE q1(6);

CLUSTER part_range_time_test USING crt_time_index;
CLUSTER part_range_num_test USING k_index;
CLUSTER part_hash_test USING id_index;

REINDEX TABLE part_range_num_test;
REINDEX INDEX crt_time_index ;
REINDEX INDEX k_index ;
REINDEX INDEX id_index ;


truncate part_range_time_test;
truncate part_range_num_test;
truncate ONLY part_hash_test;

VACUUM FULL part_range_time_test;
VACUUM FULL part_range_num_test;
VACUUM FULL part_hash_test;

SELECT COUNT(*) FROM part_range_time_test; 

alter table part_range_time_test add column name text;
alter table part_range_num_test add column name text;
alter table part_hash_test add column name text;
\d+ part_range_time_test
\d+ part_range_num_test
\d+ part_hash_test
alter table part_range_time_test alter column name type varchar(30);
alter table part_range_num_test alter column name type varchar(30);
alter table part_hash_test alter column name type varchar(30);
\d+ part_range_time_test
\d+ part_range_num_test
\d+ part_hash_test

alter table part_range_time_test drop column name ;
alter table part_range_num_test drop column name ;
alter table part_hash_test drop column name ;
\d+ part_range_time_test
\d+ part_range_num_test
\d+ part_hash_test

/* Following expect error */
/* For rename */
alter schema public  rename to my_schema;
alter foreign table gogudb_partition_table._public_0_part_hash_test rename to new_name;
alter table part_hash_test rename to new_name;
alter index crt_time_index rename to my_index;
alter table part_hash_test rename id to my_id;

/* For creat temp table */
insert into table_partition_rule(schema_name, table_name, part_expr, part_type, part_dist, remote_schema)
	 values('public', 'part_hash_test_xx', 'id', 1,4,'public');

create temp table part_hash_test_xx(id int);
create table part_hash_test_xx as select * from part_hash_test;
/* For alter foreign table */
alter foreign table gogudb_partition_table._public_0_part_hash_test drop column id;
alter foreign table gogudb_partition_table._public_0_part_hash_test add column new_col text;
/* For alter table schema */
alter table part_hash_test set schema gogudb_partition_table;
/* For drop foreign table */
drop foreign table gogudb_partition_table._public_0_part_hash_test;
CREATE TABLE other_test(id INT NOT NULL, payload REAL);
drop table other_test, part_range_num_test;
drop table other_test;

/* OK, clean it and quit */
DROP INDEX crt_time_index ;
DROP INDEX k_index ;
DROP INDEX id_index ;

drop table part_range_time_test, part_range_num_test;
drop table part_hash_test cascade;

DROP EXTENSION gogudb cascade;

