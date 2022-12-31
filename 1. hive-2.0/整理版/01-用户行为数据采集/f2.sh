#! /bin/bash
# 原始数据消费脚本 kafka --> [flume] --> hdfs
case $1 in
"start"){
	for i in bigdata101
	do
		echo " --------启动 $i 消费 flume-------"
		ssh $i "nohup /opt/flume-1.7.0/bin/flume-ng agent --conf-file /opt/flume-1.7.0/conf/kafka-flume-hdfs.conf --name
a2 -Dflume.root.logger=INFO,LOGFILE >/opt/flume-1.7.0/log.txt 2>&1 &"
	done
};;
"stop"){
	for i in bigdata101
	do
		echo " --------停止 $i 消费 flume-------"
		ssh $i "ps -ef | grep kafka-flume-hdfs | grep -v grep |awk '{print \$2}' | xargs kill"
	done
};;
esac