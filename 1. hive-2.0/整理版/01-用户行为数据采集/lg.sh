#!/bin/bash
# 原始数据生产脚本
for i in bigdata101
do
	ssh $i "java -jar log-generator-1.0-SNAPSHOT-jar-with-dependencies.jar >/dev/null"
done