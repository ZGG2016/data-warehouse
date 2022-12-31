#!/bin/bash

# 定义变量，方便后续修改
APP=gmall
hive=/opt/module/hive/bin/hive

# 如果输入了日期参数，则取输入参数作为日期值；如果没输入日期参数，则取当前时间的前一天作为日期值
if [ -n "$1" ] ;then
 do_date=$1
else 
 do_date=`date -d "-1 day" +%F`  
fi 

sql="
insert overwrite table "$APP".dwd_base_event_log partition(dt='$do_date')
select
    "$APP".base_analizer(line,'mid') as mid_id,
    "$APP".base_analizer(line,'uid') as user_id,
    "$APP".base_analizer(line,'vc') as version_code,
    "$APP".base_analizer(line,'vn') as version_name,
    "$APP".base_analizer(line,'l') as lang,
    "$APP".base_analizer(line,'sr') as source,
    "$APP".base_analizer(line,'os') as os,
    "$APP".base_analizer(line,'ar') as area,
    "$APP".base_analizer(line,'md') as model,
    "$APP".base_analizer(line,'ba') as brand,
    "$APP".base_analizer(line,'sv') as sdk_version,
    "$APP".base_analizer(line,'g') as gmail,
    "$APP".base_analizer(line,'hw') as height_width,
    "$APP".base_analizer(line,'t') as app_time,
    "$APP".base_analizer(line,'nw') as network,
    "$APP".base_analizer(line,'ln') as lng,
    "$APP".base_analizer(line,'la') as lat,
    event_name,
    event_json,
    "$APP".base_analizer(line,'st') as server_time
from "$APP".ods_event_log lateral view "$APP".flat_analizer("$APP".base_
analizer(line,'et')) tem_flat as event_name,event_json
where dt='$do_date'  and "$APP".base_analizer(line,'et')<>'';
"

$hive -e "$sql"

