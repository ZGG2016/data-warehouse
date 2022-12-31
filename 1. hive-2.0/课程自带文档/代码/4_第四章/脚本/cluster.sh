#! /bin/bash

case $1 in
"start"){
 echo " -------- 启动 集群 -------"

 echo " -------- 启动 hadoop集群 -------"
 /opt/module/hadoop-2.7.2/sbin/start-dfs.sh 
 ssh hadoop103 "source /etc/profile ; /opt/module/hadoop-2.7.2/sbin/start-yarn.sh"

 #启动 Zookeeper集群
 zk.sh start

     #Zookeeper的启动需要一定时间，此时间根据用户计算机的性能而定，可适当调整
      sleep 4s;

 #启动 Flume采集集群
 f1.sh start

 #启动 Kafka采集集群
      #Kafka的启动需要一定时间，此时间根据用户计算机的性能而定，可适当调整 
      kf.sh start

      sleep 6s;

 #启动 Flume消费集群
 f2.sh start
};;
"stop"){
    echo " -------- 停止 集群 -------"

 
    #停止 Flume消费集群
 f2.sh stop

 #停止 Kafka采集集群
 kf.sh stop

    sleep 6s;

 #停止 Flume采集集群
 f1.sh stop

 #停止 Zookeeper集群
 zk.sh stop

 echo " -------- 停止 hadoop集群 -------"
 ssh hadoop103 "/opt/module/hadoop-2.7.2/sbin/stop-yarn.sh"
 /opt/module/hadoop-2.7.2/sbin/stop-dfs.sh 
};;
esac
