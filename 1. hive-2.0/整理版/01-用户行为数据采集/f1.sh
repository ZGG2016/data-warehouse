#! /bin/bash
# 原始数据采集脚本 logfile -->  [flume] --> kafka
case $1 in
"start"){
	for i in bigdata101
	do
		echo " --------启动 $i 采集 flume-------"
		ssh $i "nohup /opt/flume-1.7.0/bin/flume-ng agent --conf-file /opt/flume-1.7.0/conf/file-flume-kafka.conf --name
a1 -Dflume.root.logger=INFO,LOGFILE >/opt/flume-1.7.0/test1 2>&1 &"
	done
};;
"stop"){
	for i in bigdata101
	do
		echo " --------停止 $i 采集 flume-------"
		ssh $i "ps -ef | grep file-flume-kafka | grep -v grep |awk '{print \$2}' | xargs kill"
	done
};;
esac