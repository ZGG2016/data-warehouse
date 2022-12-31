#! /bin/bash
# 采集通道脚本
case $1 in
"start"){
	echo " -------- 开始采集、导入 -------"

	#启动 Flume 采集集群
	f1.sh start

	#启动 Flume 消费集群
	f2.sh start
};;
"stop"){
	echo " -------- 停止采集、导入 -------"

	#停止 Flume 消费集群
	f2.sh stop

	#停止 Flume 采集集群
	f1.sh stop

};;
esac