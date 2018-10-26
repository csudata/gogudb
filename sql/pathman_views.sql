/*
 * -------------------------------------------
 *  NOTE: This test behaves differenly on 9.5
 * -------------------------------------------
 */

\set VERBOSITY terse

SET search_path = 'public','_gogu';
CREATE EXTENSION gogudb;
CREATE SCHEMA views;



/* create a partitioned table */
create table views._abc(id int4 not null);
select create_hash_partitions('views._abc', 'id', 10);
insert into views._abc select generate_series(1, 100);


/* create a facade view */
create view views.abc as select * from views._abc;

create or replace function views.disable_modification()
returns trigger as
$$
BEGIN
  RAISE EXCEPTION '%', TG_OP;
  RETURN NULL;
END;
$$
language 'plpgsql';

create trigger abc_mod_tr
instead of insert or update or delete
on views.abc for each row
execute procedure views.disable_modification();


/* Test SELECT */
explain (costs off) select * from views.abc;
explain (costs off) select * from views.abc where id = 1;
explain (costs off) select * from views.abc where id = 1 for update;
select * from views.abc where id = 1 for update;
select count (*) from views.abc;


/* Test INSERT */
explain (costs off) insert into views.abc values (1);
insert into views.abc values (1);


/* Test UPDATE */
explain (costs off) update views.abc set id = 2 where id = 1 or id = 2;
update views.abc set id = 2 where id = 1 or id = 2;


/* Test DELETE */
explain (costs off) delete from views.abc where id = 1 or id = 2;
delete from views.abc where id = 1 or id = 2;



DROP SCHEMA views CASCADE;
DROP EXTENSION gogudb;

