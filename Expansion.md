### 一、需求
* 假设当前server_remote1对应的服务器性能遇到瓶颈，需要将该服务器上的部分子表迁移到其他服务器
### 二、具体思路
假设当前test表有4个子表，分别对应node1、node2节点上的表t1、t2、t3、t4，目前需要将表t1迁移到其他服务器
1. 为node1节点搭建主从

 ![Image](https://github.com/hangzhou-cstech/gogudb/blob/master/image/e1.png)
2. node1与node3同步差不多时，在业务低峰期将应用端挂起，保证数据完全同步后，将主从断开，然后修改t1与server的映射关系让t1指向server3，然后将应用端服务恢复，可以在低峰期再将多于的数据清掉完成扩容

 ![Image](https://github.com/hangzhou-cstech/gogudb/blob/master/image/e2.png)
注：自动扩容功能将在后续版本添加
### 三、环境介绍
```
server_remote1 OPTIONS(host '192.168.0.13',port '5432',dbname
'postgres');
server_remote2 OPTIONS(host '192.168.0.14',port '5432',dbname
'postgres');
table part_hash_test(id INT NOT NULL, payload REAL); 
```

### 四、具体操作
#### 假设要对server_remote1进行扩容，其包含的分片表为
```
cs_remote_public_0_part_hash_test
cs_remote_public_1_part_hash_test 
#分片表不确定的话可以到server_remote1对应的服务器的数据库中查看
```
#### 这里以将分片表cs_remote_public_1_part_hash_test进行迁移为例
 1.将新服务器与server_remote1对应的服务器搭一个主从，所以就要求新服务器的存储空间不能小于server_remote1对应服务器的存储空间

操作略

 2.在gogudb中\d+ part_hash_test可以看到该表所包含的子表

```
postgres=# \d+ part_hash_test
Table "public.part_hash_test"
Column | Type | Collation | Nullable | Default | Storage | Stats
target | Description
‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐
‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐
id | integer | | not null | | plain |
|
payload | real | | | | plain |
|
Child tables: gogudb_partition_table._public_0_part_hash_test,
gogudb_partition_table._public_1_part_hash_test,
gogudb_partition_table._public_2_part_hash_test,
gogudb_partition_table._public_3_part_hash_test
```

 3.可以使用\d+ gogudb_partition_table._public_1_part_hash_test查看该子表对应的外部表

```
postgres=# \d+ gogudb_partition_table._public_1_part_hash_test
Foreign table
"gogudb_partition_table._public_1_part_hash_test"
Column | Type | Collation | Nullable | Default | FDW options | Storage
| Stats target | Description
‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐
‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐
id | integer | | not null | | | plain |
|
payload | real | | | | | plain |
|
Check constraints:
"gogudb__public_1_part_hash_test_check" CHECK
(get_hash_part_idx(hashint4(id), 128) >= 32 AND
get_hash_part_idx(hashint4(id), 128) < 64)
Server: server_remote1
FDW options: (schema_name 'public', table_name
'cs_remote_public_1_part_hash_test')
Inherits: part_hash_test
#可以看到子表gogudb_partition_table._public_1_part_hash_test对应的外部分片表为cs_remote_public_1_part_hash_test
```
4.添加新的server
```
CREATE SERVER server_remote3 FOREIGN DATA WRAPPER gogudb_fdw OPTIONS(host
'192.168.0.15',port '5432',dbname 'postgres');
```
  5.修改pg_foreign_server表中对应信息
```
#查看server的oid
postgres=# select oid, * from pg_foreign_server;
oid | srvname | srvowner | srvfdw | srvtype | srvversion |
srvacl | srvoptions
‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐
‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐
16548 | server_remote1 | 10 | 16511 | | |
| {host=192.168.0.11,port=5432,dbname=postgres}
16549 | server_remote2 | 10 | 16511 | | |
| {host=192.168.0.12,port=5432,dbname=postgres}
16655 | server_remote3 | 10 | 16511 | | |
| {host=192.168.0.15,port=5432,dbname=postgres}

#根据server的oid查看目标表（cs_remote_public_1_part_hash_test）的ftserver 
postgres=# select * from pg_foreign_table where ftserver=16548;
| ftrelid | ftserver | ftoptions
‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐
‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐‐
| 16595 | 16548 |
{schema_name=public,table_name=cs_remote_public_0_part_hash_test}
| 16599 | 16548 |
{schema_name=public,table_name=cs_remote_public_1_part_hash_test}

#在业务低峰期且主从数据基本同步后，暂停一下业务，然后将目标表的server替换成新的server 
postgres=# update pg_foreign_table set ftserver = 16655 where ftserver =
16548 and ftrelid=16599; 
```
### 五、验证是否修改成功
```
postgres=# \d+ gogudb_partition_table._public_1_part_hash_test
Foreign table
"gogudb_partition_table._public_1_part_hash_test"
Column | Type | Collation | Nullable | Default | FDW options | Storage
| Stats target | Description
‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐
‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐‐+‐‐‐‐‐‐‐‐‐‐‐‐‐
id | integer | | not null | | | plain |
|
payload | real | | | | | plain |
|
Check constraints:
"gogudb__public_1_part_hash_test_check" CHECK
(get_hash_part_idx(hashint4(id), 128) >= 32 AND
get_hash_part_idx(hashint4(id), 128) < 64)
Server: server_remote3
FDW options: (schema_name 'public', table_name
'cs_remote_public_1_part_hash_test')
Inherits: part_hash_test
#可以看到目前已修改成功
```
### 六、断开主从，开启服务，将多余数据删除，完成扩容

操作略
 
 
 
 
 
 
 
 
 
 
