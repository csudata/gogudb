# 开源分布式数据库GoGuDB
>当下分布式数据库架构百花齐放，我们一直所追求一种架构使用起来与单机无太大差异，GoGuDB是基于PostgreSQL的分布式数据库，在SQL语句上能支持所有SQL语句查询性能上我们做了大量优化工作，下一步工作是实现自动扩容、和分布式事务
> 项目中我们借助了pg_pathman实现高效分区:https://github.com/postgrespro/pg_pathman



## 有何优势？
结合之前公司项目经验及对市面上分布式产品(包括中间件)调研，常常有以下几个问题:
* 复杂SQL、跨库Join不支持或者结果是错误的
* SQL语句不能下推(将计算能力下推到数据节点)
* 中间件节点不具有查询优化器功能
* 实现数据库协议层，随着原生数据库新功能发布需要合并代码或放弃新功能
 
我们先看一下当下比较流行的中间件是如何做到的：首先，中间件通过一个全局表，用来解决跨节点数据聚合的问题，实现方法是在每一个分片上面，都创建这样的全局表，它的定义是不怎么修改，查询比较频繁表可以定义为全局表，这样的话在每一个分片节点上，只要用到这个表，就可以实现本地查询连接等操作，是可以解决部分问题，但问题是如果分片多的话（假如分片100个），如何保证数据一致性？这么多节点的XA事务影响是什么？如果出现不一致或者访问错误，引起的问题就是数据结果错误，这样的结果肯定不是业务想要看到的吧。
相比传统中间件方式,GoguDB的实现方法则可靠许多。因为GoguDB本身就是部署在数据库上面，所以，面对跨节点join的需求，GoguDB会把需要join的表的信息先从底层数据库中取出并临时存放在GoguDB所在的数据库中，在GoguDB节点上进行join操作，然后将结果返回给上层应用。相比于传统中间件,GoguDB不需要对数据库集群搞一个特殊的全局表，所以，传统中间件在跨节点join存在的种种问题，在GoguDB中自然不复存在。
 
### GoGuDB架构图
![Image text](https://github.com/hangzhou-cstech/gogudb/blob/master/image/gogudb_01.jpg)

GoguDB本质上是一个插件，部署起来仅需要安装并简单配置一下，相比于传统的“重量级”中间件，GoguDB显得灵活许多。
区别于传统的中间件GoguDB将分片的配置信息存放在GoguDB所在数据节点的一些表中，在配置分片信息时，只需要配置几条SQL就可以了，而且相关配置表结构简单，关系明朗，很容易理解并上手。

## 快速上手

* 安装配置手册:https://github.com/hangzhou-cstech/gogudb/blob/master/Install.md
* 获取二进制包:https://github.com/hangzhou-cstech/gogudb_binary_download

## 功能性测试实例
https://github.com/hangzhou-cstech/gogudb/blob/master/Functional_Test.md
