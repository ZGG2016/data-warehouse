#!/bin/bash

#根据传递的日期参数修改配置文件的日期
if [ $# -ge 1 ]
then
    sed -i "/mock.date/c mock.date: $1" /root/db_log/application.properties
fi

cd /root/db_log; java -jar gmall2020-mock-db-2021-01-22 >/dev/null 2>&1 &