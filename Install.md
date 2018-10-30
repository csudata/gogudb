# gogudb 使用手册 1.1 版
这是1.1版本的用户手册，主要内容包括：安装部署、使用接口以及使用案例三部分。
## 安装部署
### 安装前提
在安装gogudb之前，需要安装PG，安装PG的方式不限，但是需要的PG大版本是9.6或是10。目前gogudb支持的OS版本是centOS 7以上，并且该系统上安装了make这个工具（centos上默认自带了这个工具）。
在安装PG之后，需要设置环境变量，将pg的bin目录加入到path中来，使得shell能直接使用pg_config命令
### 安装二进制包
* 获取最新的二进制包，根据当前的OS、PG版本获取。
* 使用tar命令将其解压缩
* 解压之后，进入二进制包的所在目录，使用相应的权限执行make install命令，改命令会将so文件、sql文件、control复制到对应目录。注意：如果PG的版本和gogudb要求的PG版本不匹配，会报错，从而安装失败
* 详细源码安装请参照https://github.com/hangzhou-cstech/gogudb/blob/master/gogudb_source_install.md
### 配置gogudb
#### 编辑PG的配置文件
vi /path_to/postgresql.conf 在配置文件中增加一行：

```
shared_preload_libraries='gogudb'
```
在修改了配置文件之后，需要重启该数据库的实例。

### 创建gogudb
在gogudb的数据库启动之后，运行 create externsion gogudb，即可完成gogudb的创建

## 使用接口
gogudb向用户提供了两大功能：一，创建远程数据源；二，配置分表规则；三，配置远程数据源的hash值区间。用户需要首先借助gogudb提供的gogudb_fdw来创建远程数据源，创建之后，用户在建表之前根据分表规则，需要向配置表里设置表的分表规则。当用户创建表的时候，gogudb会根据分表规则创建若干分片，并均分到各个远程数据源上。当创建基于HASH的分区表之前，用户需要配置远程数据源的hash值区间，创建之后，调用相应函数生效，然后就可以创建HASH方式的分区表了，否则会报错。
### 创建远程数据源
* 使用create server语句创建数据源，例如：

```
CREATE SERVER server_remote1 FOREIGN DATA WRAPPER gogudb_fdw OPTIONS(host '192.168.2.2',port '5432',dbname 'postgres');
```

创建了一个名为server_remote1的数据源，远程数据源的IP是192.168.2.2，端口是5432，数据库名是postgres
* 使用create user mapping语句来配置数据源的访问用户信息，例如：

```
create user mapping for current_user    server server_remote1 options(user 'pgsql',password '');
```

设置当前用户访问server_remote1时，使用的登录用户是pgsql，密码为空。

### 配置表的分表规则
gogudb中有一张表 table_partition_rule（1.0版本在pg_catalog这个schema下，而1.1移到了\_gogu这个schema下）定义了用户分表的规则，在创建分区表之前，用户需要在表中插入数据。在创建表的时候，gogudb会根据改变预先定义的规则，来创建分区表。该表主要包括下列字段：
* schema_name 类型TEXT NOT NULL，指定即将创建的父表所在的schema
* table_name 类型TEXT NOT NULL，指定即将创建的父表的名称，
* part_expr  类型TEXTTEXT NOT NULL，指定分表时使用的表达式（最简单的就是列名）
* part_type  类型INTEGER NOT NULL，分区类型，只能选择1或是2,1表示hash分区，2表示range分区
* range_interval  类型TEXT DEFAULT NULL， range分区时使用的间隔。
* range_start  类型TEXT DEFAULT NULL，range分区时使用起始值。
* part_dist  类型INTEGER，子表的总数量，最终创建远程子表时，子表会逐一分布到每个远程数据源上数量，尽量保证每个数据源上的子表数据量均匀一致。
* remote_schema 类型 TEXT DEFAULT NULL，子表在远程数据源上的schema，默认为public
* servers 类型TEXT[] DEFAULT NULL，子表分布的远程数据源名称列表，默认是系统内所有使用gogudb_fdw的远程的数据源列表。

### 配置远程数据源的HASH值区间
gogudb中有一张表 server_map（1.0版本在pg_catalog这个schema下，而1.1移到了\_gogu这个schema下）定义了做hash值的范围和远程数据源的关系，这张表主要有下面三个字段：
* server_name, TEXT NOT NULL类型，子表分布的远程数据源名称列表，默认是系统内所有使用gogudb_fdw的远程的数据源列表。
* range_start，smallint NOT NULL类型，hash值范围的起始值（包括该值），最小为0；
* range_end，smallint NOT NULL类型，hash值范围的结束值（不包括该值），不小于range_start，不大于128；
用户向 pg_catalog.server_map插入数据源以及范围之后，需要执行

```
select reload_range_server_set()
```
来重新加载server_map表的数据，使之生效。
## 使用案例

### 准备测试环境的数据库实例
环境如下：

<table>
 <thead> <tr> <th>主机名</th><th>IP</th> <th>数据库端口</th> <th>数据库数据目录</th></tr></thead>
  <tbody>
  <tr><td>gogu01</td><td>192.168.3.46</td><td>5432</td><td>/home/postgres/pgdaa</td></tr>
  <tr><td>pg01</td><td>192.168.3.41</td><td>5432</td><td>/home/postgres/pgdata</td></tr>
  <tr><td>pg02</td><td>192.168.3.42</td><td>5432</td><td>/home/postgres/pgdata</td></tr>
</tbody>
</table>

其中gogu01作为googudb的运行实例，pg01和pg02作为gogudb的远程数据源

#### 配置gogudb
编辑gogu01机器上（即gogudb）数据库的配置文件： vi /data/pgdata/postgresql.conf 在配置文件中增加一行：

```
shared_preload_libraries='gogudb'
```


### 安装gogudb
使用前面描述的方法，获得gogudb的二进制包之后使用make install安装
### 启动数据库：
启动gogudb数据库：

```
pg_ctl start -D /data/pgdata
```

### 创建extension

需要在gogudb的数据库中创建gogudb的extension
* 连接gogudb数据库：
psql -d postgres
* 在连接会话中创建gogudb:

```
create extension gogudb;
```

### 创建远程数据源
创建两个数据源，分别指向拍pg01和pg02:
* 连接gogudb数据库： psql -d postgres
* 创建名为server_remote1的数据源：CREATE SERVER server_remote1 FOREIGN DATA WRAPPER gogudb_fdw OPTIONS(host '192.168.3.41',port '5432',dbname 'postgres');
* 创建名为server_remote2的数据源：CREATE SERVER server_remote2 FOREIGN DATA WRAPPER gogudb_fdw OPTIONS(host '192.168.3.42',port '5432',dbname 'postgres');
* 为server_remote1设置用户名密码：create user mapping for current_user    server server_remote1 options(user 'pgsql',password '');
* 为server_remote2设置用户名密码：create user mapping for current_user    server server_remote1 options(user 'pgsql',password '');

### 配置远程数据源的hash值分区
执行下列操作：

```
insert into _gogu.server_map values('server_remote1', 0, 64), ('server_remote2', 64, 128);
select _gogu.reload_range_server_set();
```
这就配置并生效了两台远程数据源server_remote1和server_remote2，server_remote1接受的hash值范围是[0,64), server_remote2接受的范围是[64,128)。

### 使用hash方式创建分区表
主要是先在table_partition_rule中插入表的分区规则，然后使用普通SQL来创建表。
* 连接gogudb数据库： psql -d postgres
* 插入分区规则：

```
insert into _gogu.table_partition_rule(schema_name, table_name, part_expr, part_type, part_dist, remote_schema) values('public', 'part_hash_test', 'id', 1,4,'public');
```
**注意对于1.0版本，上面的表名前不需要加“\_gogu.”**

插入的记录指定了：将会在public的schema下创建一张表，表的分布字段是id，采用hash分区，总共4个分片，分片将分布到所有的gogudb_fdw的远程的数据源列表上，所在的schema为public
* 创建分区表：

```
postgres=#  CREATE TABLE part_hash_test(id INT NOT NULL, payload REAL);
CREATE TABLE
```

*可以查看生成的分区表：
```
postgres=# \d+ part_hash_test 
                               Table "public.part_hash_test"
 Column  |  Type   | Collation | Nullable | Default | Storage | Stats target | Description 
---------+---------+-----------+----------+---------+---------+--------------+-------------
 id      | integer |           | not null |         | plain   |              | 
 payload | real    |           |          |         | plain   |              | 
Child tables: gogudb_partition_table._public_0_part_hash_test,
              gogudb_partition_table._public_1_part_hash_test,
              gogudb_partition_table._public_2_part_hash_test,
              gogudb_partition_table._public_3_part_hash_test
postgres=# \dES gogudb_partition_table.*
                            List of relations
         Schema         |          Name          |     Type      | Owner 
------------------------+------------------------+---------------+-------
 gogudb_partition_table | _public_0_part_hash_test | foreign table | pgsql
 gogudb_partition_table | _public_1_part_hash_test | foreign table | pgsql
 gogudb_partition_table | _public_2_part_hash_test | foreign table | pgsql
 gogudb_partition_table | _public_3_part_hash_test | foreign table | pgsql
```
*删除表：
```
postgres=# drop table part_hash_test cascade;
NOTICE:  drop cascades to 4 other objects
DETAIL:  drop cascades to foreign table gogudb_partition_table._public_0_part_hash_test
drop cascades to foreign table gogudb_partition_table._public_1_part_hash_test
drop cascades to foreign table gogudb_partition_table._public_2_part_hash_test
drop cascades to foreign table gogudb_partition_table._public_3_part_hash_test
DROP TABLE
```
### 使用range方式创建基于时间类型分区表
主要的步骤也是先向配置表中插入数据，然后使用普通的SQL来建表。
* 向分区配置表插入数据：
```
insert into  _gogu.table_partition_rule(schema_name ,table_name ,part_expr ,part_type ,range_interval ,range_start ,part_dist, remote_schema) values('public', 'part_range_test', 'crt_time', 2, '2 month','2018-1-1 00:00:0', 6, 'public');
```

这个指定即将创建表的schema是pulic，表名是part_range_test，分表会使用的表达式是'crt_time，实际将会是一个timestamp类型的字段，分区的类型是range方式，分区间隔是'2 month'，起始值是'2018-1-1 00:00:0'，会创建6个分片，分片位于所有数据源上，schema为public。
*创建表：

```
postgres=# create table part_range_test(id int, info text, crt_time timestamp not null);
CREATE TABLE
```
*可以查看这个表由6个子表组成，实际是外部表，均分到2个外部数据源上：

```
postgres=# \d+  part_range_test
                                         Table "public.part_range_test"
  Column  |            Type             | Collation | Nullable | Default | Storage  | Stats target | Description 
----------+-----------------------------+-----------+----------+---------+----------+--------------+-------------
 id       | integer                     |           |          |         | plain    |              | 
 info     | text                        |           |          |         | extended |              | 
 crt_time | timestamp without time zone |           | not null |         | plain    |              | 
Child tables: gogudb_partition_table._public_1_part_range_test,
              gogudb_partition_table._public_2_part_range_test,
              gogudb_partition_table._public_3_part_range_test,
              gogudb_partition_table._public_4_part_range_test,
              gogudb_partition_table._public_5_part_range_test,
              gogudb_partition_table._public_6_part_range_test

postgres=# \dES gogudb_partition_table.*
                            List of relations
         Schema         |          Name           |     Type      | Owner 
------------------------+-------------------------+---------------+-------
 gogudb_partition_table | _public_1_part_range_test | foreign table | pgsql
 gogudb_partition_table | _public_2_part_range_test | foreign table | pgsql
 gogudb_partition_table | _public_3_part_range_test | foreign table | pgsql
 gogudb_partition_table | _public_4_part_range_test | foreign table | pgsql
 gogudb_partition_table | _public_5_part_range_test | foreign table | pgsql
 gogudb_partition_table | _public_6_part_range_test | foreign table | pgsql
(6 rows)
```

*使用普通SQL来删除表：

```
postgres=# drop table part_range_test cascade;
NOTICE:  drop cascades to 7 other objects
DETAIL:  drop cascades to sequence part_range_test_seq
drop cascades to foreign table gogudb_partition_table._public_1_part_range_test
drop cascades to foreign table gogudb_partition_table._public_2_part_range_test
drop cascades to foreign table gogudb_partition_table._public_3_part_range_test
drop cascades to foreign table gogudb_partition_table._public_4_part_range_test
drop cascades to foreign table gogudb_partition_table._public_5_part_range_test
drop cascades to foreign table gogudb_partition_table._public_6_part_range_test
DROP TABLE
```

### 使用range方式创建基于数值类型分区表
* 向分区配置表插入数据：

```
insert into _gogu.table_partition_rule(schema_name, table_name, part_expr, part_type, part_dist, remote_schema, range_interval,range_start) values('public','part_range_num_test', 'id', 2, 4, 'public', '100', '0');
```

指定即将创建表的schema是pulic，表名是part_range_num_test，分表会使用的表达式是id，实际将会是一个int类型的字段，分区的类型是range方式，分区间隔是'100'，起始值是'0'，会创建4个分片，分片位于所有数据源上，schema为public。

* 创建表：

```
postgres=# CREATE TABLE part_range_num_test ( id integer NOT NULL, k integer DEFAULT 0 NOT NULL);、;
CREATE TABLE
```

* 可以查看这个表由4个子表组成：

```
postgres=# \d+  part_range_num_test
                            Table "public.part_range_num_test"
 Column |  Type   | Collation | Nullable | Default | Storage | Stats target | Description
--------+---------+-----------+----------+---------+---------+--------------+-------------
 id     | integer |           | not null |         | plain   |              |
 k      | integer |           | not null | 0       | plain   |              |
Child tables: gogudb_partition_table._public_1_part_range_num_test,
              gogudb_partition_table._public_2_part_range_num_test,
              gogudb_partition_table._public_3_part_range_num_test,
              gogudb_partition_table._public_4_part_range_num_test
```

* 删除表

```
postgres=# drop table part_range_num_test cascade;
NOTICE:  drop cascades to 5 other objects
DETAIL:  drop cascades to sequence part_range_num_test_seq
drop cascades to foreign table gogudb_partition_table._public_1_part_range_num_test
drop cascades to foreign table gogudb_partition_table._public_2_part_range_num_test
drop cascades to foreign table gogudb_partition_table._public_3_part_range_num_test
drop cascades to foreign table gogudb_partition_table._public_4_part_range_num_test
drop table part_hash_test cascade;
```

## 注意事项
目前可以在父表执行的操作有：create index，drop index，vacuum，reindex， cluster，truncate only等，不能执行的操作有：rename 父表，rename 子表，rename子表使用的远程表，不能rename partition_table_rule中使用的schema，不能修改子表、或是远程表中表名，不能drop 子表或是远程表。此外，也不支持分区的合并与分裂、以及读写分离。
