#!/bin/bash
# 时间同步、修改脚本
for i in bigdata101 bigdata102 bigdata103
do
	echo "========== $i =========="
	ssh -t $i "sudo date -s $1"
done