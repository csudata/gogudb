* 测试表结构
```
postgres=# \d+ partitioned_table
                              Table "public.partitioned_table"
 Column  |  Type   | Collation | Nullable | Default | Storage  | Stats target | Description
---------+---------+-----------+----------+---------+----------+--------------+-------------
 id      | integer |           | not null |         | plain    |              |
 payload | real    |           |          |         | plain    |              |
 name    | text    |           |          |         | extended |              |
Child tables: partitioned_table_0,
              partitioned_table_1,
              partitioned_table_2,
              partitioned_table_3
```
* 测试SQL通过性列表

|SQL|是否通过|
|-|-|
|alter table partitioned_table add column name text;|是|
|alter table partitioned_table alter column name type varchar(30);|是|
|select max(id) from partitioned_table;|是|
|select min(payload) from partitioned_table where id<2;|是|
|select * from partitioned_table where name !='';|是|
|create index CONCURRENTLY index_parttion_id on partitioned_table(id);|是|
|drop index "index_parttion_id";|是|
|select id,count(*) from partitioned_table group by id order by id limit 6;|是|
|select *  from partitioned_table a left join  gwtest01 b on a.id=b.id;|是|
|select *  from partitioned_table a left join  gwtest01 b on a.id=b.id where a.id=6;|是|
|select avg(payload) from partitioned_table where id <2;|是|
|select count(distinct(id)) from partitioned_table where id=3;|是|
|select count(distinct(id)) from partitioned_table;|是|
|select id,count(*) from partitioned_table group by id having true;|是|
|select count(*) from partitioned_table where id <2;|是|
|select * from partitioned_table limit 10;|是|
|select count(*) from partitioned_table;|是|
|delete from partitioned_table where name='hello';|是|
|insert into partitioned_table values(11,0.35,'cstech');|是|
|copy (select * from partitioned_table) to '/tmp/test.csv';|是|
|copy partitioned_table from '/tmp/a.txt';|否|
|SELECT * FROM part_hash_test WHERE id>(select avg(id) from part_hash_test);|是|
