#!/bin/bash

if [ $# -lt 1 ]
then
    echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
exit
fi

case $1 in
start)
    for i in bigdata102
    do
        echo "====================> START $i KF <===================="
        ssh $i;/opt/kafka_2.12-3.2.1/bin/kafka-server-start.sh -daemon /opt/kafka_2.12-3.2.1/config/server.properties
    done
;;
stop)
    for i in bigdata102
    do
        echo "====================> STOP $i KF <===================="
        ssh $i;/opt/kafka_2.12-3.2.1/bin/kafka-server-stop.sh
    done
;;
kc)
    if [ $2 ]
    then
        /opt/kafka_2.12-3.2.1/bin/kafka-console-consumer.sh --bootstrap-server bigdata102:9092 --topic $2
    else
        echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
    fi
;;
kp)
    if [ $2 ]
    then
        /opt/kafka_2.12-3.2.1/bin/kafka-console-producer.sh --broker-list bigdata102:9092 --topic $2
    else
        echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
    fi
;;
list)
    /opt/kafka_2.12-3.2.1/bin/kafka-topics.sh --list --bootstrap-server bigdata102:9092
;;
describe)
    if [ $2 ]
    then
        /opt/kafka_2.12-3.2.1/bin/kafka-topics.sh --describe --bootstrap-server bigdata102:9092 --topic $2
    else
        echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
    fi
;;
delete)
    if [ $2 ]
    then
        /opt/kafka_2.12-3.2.1/bin/kafka-topics.sh --delete --bootstrap-server bigdata102:9092 --topic $2
    else
        echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
    fi
;;
*)
    echo "Usage: kf.sh {start|stop|kc [topic]|kp [topic] |list |delete [topic] |describe [topic]}"
exit
;;
esac