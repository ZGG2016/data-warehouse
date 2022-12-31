#!/bin/bash

#根据传递的日期参数修改配置文件的日期
if [ $# -ge 1 ]
then
    sed -i "/mock.date/c mock.date: $1" /root/app_log/application.yml
fi

cd /root/app_log; java -jar gmall2020-mock-log-2021-11-29.jar >/dev/null 2>&1 &