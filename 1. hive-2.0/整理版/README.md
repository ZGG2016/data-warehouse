
[TOC]

## 1 用户行为数据采集

### 1.1 生产原始数据

```
[logfile] -->  flume --> kafka --> flume --> hdfs
```

通过执行如下命令修改系统时间，以生产不同日期的数据

```sh
# dt.sh
date -s "2022-09-22 10:00:00"
```

如果系统时间修改后不生效，那么[执行如下命令](https://blog.csdn.net/StanleyWm_/article/details/101704707)

```sh
service ntp stop
timedatectl set-ntp no
```

集群的话注意同步各服务器的时间

执行如下命令，先生产5天的数据

```sh
# lg.sh
java -jar log-generator-1.0-SNAPSHOT-jar-with-dependencies.jar >/dev/null
```

### 1.2 flume采集原始数据

```
logfile -->  [flume] --> kafka --> flume --> hdfs
```

执行如下命令采集数据

```sh
# f1.sh
nohup /opt/flume-1.7.0/bin/flume-ng agent --conf-file /opt/flume-1.7.0/conf/file-flume-kafka.conf --name a1 -Dflume.root.logger=INFO,LOGFILE >/opt/flume-1.7.0/test1 2>&1 &
```

### 1.3 启动kafka

```
logfile -->  flume --> [kafka] --> flume --> hdfs
```

启动

```sh
# 先启动zookeeper
bin/kafka-server-start.sh config/server.properties >logs/kafka.log 2>1 &
```

创建topic

```sh
bin/kafka-topics.sh --bootstrap-server bigdata101:9092 --create --replication-factor 1 --partitions 1 --topic topic_start

bin/kafka-topics.sh --bootstrap-server bigdata101:9092 --create --replication-factor 1 --partitions 1 --topic topic_event
```

查看

```sh
bin/kafka-topics.sh --bootstrap-server bigdata101:9092 --list
```

删除

```sh
bin/kafka-topics.sh --delete --bootstrap-server bigdata101:9092 --topic topic_start
```

### 1.4 flume消费数据到hdfs


```
logfile --> flume --> kafka --> [flume] --> hdfs
```

```sh
# f2.sh
nohup /opt/flume-1.7.0/bin/flume-ng agent --conf-file /opt/flume-1.7.0/conf/kafka-flume-hdfs.conf --name a2 -Dflume.root.logger=INFO,LOGFILE >/opt/flume-1.7.0/test1 2>&1 &
```

### 1.5 用户行为数据采集通道

将前面的脚本和命令准备好之后，先逐个组件的测试，保证每个环节是通的。

测试通过后，开两个窗口（不加nohup），一个执行生产数据脚本(f1.sh)，一个执行消费数据脚本(f2.sh)，逐个执行每天的数据，并随时查看日志输出内容。

注意，执行每天的数据前，先修改系统时间，和日志数据的时间一致。

熟悉日志内容后，再使用采集通道脚本执行。

这里是先启动好各个组件(hadoop\zookeeper\kafka)，再采集数据(cluster.sh)。

## 2 业务数据采集

### 2.1 mysql生产原始数据

```sql
create database gmall;	
```


**一定要用提供的SQLyog客户端执行sql脚本，如果在命令行下使用source命令会出现中文乱码的问题**

**执行生成数据的jar包一定要在mysql5环境下**

**如果虚拟机使用的mysql8，可以在主机上安装mysql5，利用客户端导出成sql脚本，再使用source命令导入mysql8**

### 2.2 原始数据sqoop导入到hdfs

在执行导入命令前，先执行 `bin/sqoop list-databases --connect jdbc:mysql://bigdata101:3306/ --username root --password root` 测试下sqoop是否安装成功

直接执行提供的脚本，会报 `-bash: ./mysql_to_hdfs.sh: /bin/bash^M: bad interpreter: No such file or dir` 错，要先执行 `sed -i 's/\r$//' mysql_to_hdfs.sh` 命令。


如果在第一节修改了系统时间，还没有修改回当前时间，那么需要先修改回当前时间。

初次导入 `mysql_to_hdfs.sh first 2022-09-20`

每日导入 `mysql_to_hdfs.sh all 2022-09-21` `mysql_to_hdfs.sh all 2022-09-22`

## 3 数仓分层搭建

见 `数仓.md`