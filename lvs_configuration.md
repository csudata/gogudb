## 一、环境准备
|主机|ip|vip|
|-|-|-|
|lvs负载均衡器|192.168.2.203|192.168.2.17|
|服务器RS1|192.168.2.204| |
|服务器RS2|192.168.2.204| |
## 二、安装软件
* 可以编译安装也可以yum安装，编译安装需要创建一个超链接：ln -s /usr/src/kernels/2.6.18-238.el5-i686 /usr/src/linux
这里选择yum方式安装

```

[root@bogon ~]# yum -y install ipvsadm

已加载插件：fastestmirror

设置安装进程

Determining fastest mirrors

... ...

已安装:

  ipvsadm.x86_64 0:1.26-4.el6                                                                                          



作为依赖被安装:

  libnl.x86_64 0:1.1.4-2.el6                                                                                           



完毕！

```

* 查看ipvs模块是否加载

```

[root@bogon ~]# lsmod | grep ip_vs
[root@bogon ~]#
```

* 因为此时系统还没有把ipvs模块加载进系统，需要我们执行ipvsadm命令才会加载进去或者modprobe ip_vs

```

[root@bogon ~]# ipvsadm

IP Virtual Server version 1.2.1 (size=4096)

Prot LocalAddress:Port Scheduler Flags

  -> RemoteAddress:Port Forward Weight ActiveConn InActConn

[root@bogon ~]# lsmod | grep ip_vs

ip_vs 126897 0 

libcrc32c 1246 1 ip_vs

ipv6 336282 270 ip_vs,ip6t_REJECT,nf_conntrack_ipv6,nf_defrag_ipv6

[root@bogon ~]#

```

## 三、手动配置LVS负载均衡器

```

[root@bogon ~]# ifconfig eth1:1 192.168.2.17 netmask 255.255.255.0
[root@bogon ~]# route add -host 192.168.2.17 dev eth1
```

* ipvsadm命令参数：

```

-A    

-A --add-service 添加一个带选项的虚拟服务。

Add a virtual service. A serviceaddress is uniquely defined by a triplet: IP address, portnumber, and protocol. Alternatively a virtualservice may be defined by a firewall-mark.

-t 指定虚拟服务器的IP地址和端口

-s -s,--scheduler scheduling-method 调度算法

-p 会话保持按秒计算

-a    

-a在对应的VIP下添加RS节点

-g 指定此LVS的工作模式为-g -g为DR模式

-l    

指定LVS的工作模式为-l -l为tunnel模式

-m 指定LVS的工作模式为NAT模式

-w 指定RS节点的权重

-D    

删除虚拟服务

格式：ipvsadm-D -t|u|f service-address

Delete a virtual service, alongwith any associated real servers.

-C

-C, --clear Clear the virtual server table清空lvs原有的配置。

-set 设置tcp tcpfn udp 的连接超时时间（一般来说高并发的时候小一点点。)

```

* ipvsadm添加lvs服务

```

[root@bogon ~]# ipvsadm -C

[root@bogon ~]# ipvsadm -A -t 192.168.2.17:80 -s rr　　#添加虚拟服务指定VIP

[root@bogon ~]# 

[root@bogon ~]# ipvsadm -a -t 192.168.2.17:80 -r 192.168.2.204:80 -g　　#针对虚拟服务添加RS节点

[root@bogon ~]# ipvsadm -a -t 192.168.2.17:80 -r 192.168.2.205:80 -g

[root@bogon ~]# ipvsadm -L -n　　#查看VIP和RS是否已经配置成功

IP Virtual Server version 1.2.1 (size=4096)

Prot LocalAddress:Port Scheduler Flags

  -> RemoteAddress:Port Forward Weight ActiveConn InActConn

TCP 192.168.2.17:80 rr

  -> 192.168.2.204:80 Route 1 0 0         

  -> 192.168.2.205:80 Route 1 0 0

```

* LB上删除虚拟服务

```

ipvsadm -D -t 192.168.2.17:80 

```

# 四、RS节点服务器手动配置

* 添加lo端口的VIP&路由

```

[root@bogon ~]# ifconfig lo:0 192.168.2.17 netmask 255.255.255.255　　(由于RS的VIP不是用来通讯，并且这里一定要设置24位掩码）
[root@bogon ~]# route add -host 192.168.2.17 dev lo
```

* 配置ARP抑制

```

[root@bogon ~]# echo "1" > /proc/sys/net/ipv4/conf/lo/arp_ignore 
[root@bogon ~]# echo "2" > /proc/sys/net/ipv4/conf/lo/arp_announce 
[root@bogon ~]# echo "1" > /proc/sys/net/ipv4/conf/all/arp_announce 
[root@bogon ~]# echo "2" > /proc/sys/net/ipv4/conf/all/arp_ignore
```

