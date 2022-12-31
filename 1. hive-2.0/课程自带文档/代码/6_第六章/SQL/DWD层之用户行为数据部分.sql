DWD层之用户行为数据部分

1. 启动日志表

建表语句
------------------------------------------------------
drop table if exists dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log( --字段为示例数据中出现的字段
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `entry` string, 
    `open_ad_type` string, 
    `action` string, 
    `loading_time` string, 
    `detail` string, 
    `extend1` string
)
PARTITIONED BY (dt string) --以日期为分区
stored as  parquet  --存储格式为列式存储格式Parquet
location '/warehouse/gmall/dwd/dwd_start_log/' --指定存储目录
tblproperties ("parquet.compression"="lzo") --指定压缩格式为LZO
;
----------------------------------------------------

数据加载
----------------------------------------------------
insert overwrite table dwd_start_log –PARTITION (dt='2020-03-10') –-指定分区
select 
    get_json_object(line,'$.mid') mid_id, --使用Hive内置的JSON解析工具解析JSON字符串，获取字段值
    get_json_object(line,'$.uid') user_id,
    get_json_object(line,'$.vc') version_code,
    get_json_object(line,'$.vn') version_name,
    get_json_object(line,'$.l') lang,
    get_json_object(line,'$.sr') source,
    get_json_object(line,'$.os') os,
    get_json_object(line,'$.ar') area,
    get_json_object(line,'$.md') model,
    get_json_object(line,'$.ba') brand,
    get_json_object(line,'$.sv') sdk_version,
    get_json_object(line,'$.g') gmail,
    get_json_object(line,'$.hw') height_width,
    get_json_object(line,'$.t') app_time,
    get_json_object(line,'$.nw') network,
    get_json_object(line,'$.ln') lng,
    get_json_object(line,'$.la') lat,
    get_json_object(line,'$.entry') entry,
    get_json_object(line,'$.open_ad_type') open_ad_type,
    get_json_object(line,'$.action') action,
    get_json_object(line,'$.loading_time') loading_time,
    get_json_object(line,'$.detail') detail,
    get_json_object(line,'$.extend1') extend1
from ods_start_log  --数据来源为ODS层的启动日志表
where dt='2020-03-10'; --指定数据日期
---------------------------------------------------

导入脚本 dwd_start_log.sh
---------------------------------------------------
#!/bin/bash
# 定义变量，方便后续修改
APP=gmall
hive=/opt/module/hive/bin/hive

# 如果输入了日期参数，则取输入参数作为日期值；如果没有输入日期参数，则取当前时间的前一天作为日期值
if [ -n "$1" ] ;then
 do_date=$1
else 
 do_date=`date -d "-1 day" +%F`  
fi 

sql="
insert overwrite table "$APP".dwd_start_log
PARTITION (dt='$do_date')
select 
    get_json_object(line,'$.mid') mid_id,
    get_json_object(line,'$.uid') user_id,
    get_json_object(line,'$.vc') version_code,
    get_json_object(line,'$.vn') version_name,
    get_json_object(line,'$.l') lang,
    get_json_object(line,'$.sr') source,
    get_json_object(line,'$.os') os,
    get_json_object(line,'$.ar') area,
    get_json_object(line,'$.md') model,
    get_json_object(line,'$.ba') brand,
    get_json_object(line,'$.sv') sdk_version,
    get_json_object(line,'$.g') gmail,
    get_json_object(line,'$.hw') height_width,
    get_json_object(line,'$.t') app_time,
    get_json_object(line,'$.nw') network,
    get_json_object(line,'$.ln') lng,
    get_json_object(line,'$.la') lat,
    get_json_object(line,'$.entry') entry,
    get_json_object(line,'$.open_ad_type') open_ad_type,
    get_json_object(line,'$.action') action,
    get_json_object(line,'$.loading_time') loading_time,
    get_json_object(line,'$.detail') detail,
    get_json_object(line,'$.extend1') extend1
from "$APP".ods_start_log 
where dt='$do_date';
"

$hive -e "$sql"
--------------------------------------------------

2. 用户行为事件基础明细表

建表语句
--------------------------------------------------
drop table if exists dwd_base_event_log;
CREATE EXTERNAL TABLE dwd_base_event_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string, 
    `app_time` string, 
    `network` string, 
    `lng` string, 
    `lat` string, 
    `event_name` string, 
    `event_json` string, 
    `server_time` string
)
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_base_event_log/'
TBLPROPERTIES('parquet.compression'='lzo');
----------------------------------------------------

创建永久函数
----------------------------------------------------
create function base_analizer as 'com.atguigu.udf.BaseFieldUDF' using jar 'hdfs://hadoop102:9000/user/hive/jars/hivefunction-1.0-SNAPSHOT.jar';

create function flat_analizer as 'com.atguigu.udtf.EventJsonUDTF' using jar 'hdfs://hadoop102:9000/user/hive/jars/hivefunction-1.0-SNAPSHOT.jar'; 
----------------------------------------------------

数据加载
----------------------------------------------------
insert overwrite table dwd_base_event_log partition(dt='2020-03-10')
select
    base_analizer(line,'mid') as mid_id,
    base_analizer(line,'uid') as user_id,
    base_analizer(line,'vc') as version_code,
    base_analizer(line,'vn') as version_name,
    base_analizer(line,'l') as lang,
    base_analizer(line,'sr') as source,
    base_analizer(line,'os') as os,
    base_analizer(line,'ar') as area,
    base_analizer(line,'md') as model,
    base_analizer(line,'ba') as brand,
    base_analizer(line,'sv') as sdk_version,
    base_analizer(line,'g') as gmail,
    base_analizer(line,'hw') as height_width,
    base_analizer(line,'t') as app_time,
    base_analizer(line,'nw') as network,
    base_analizer(line,'ln') as lng,
    base_analizer(line,'la') as lat,
    event_name,
    event_json,
    base_analizer(line,'st') as server_time
from ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tmp_flat as event_name,event_json
where dt='2020-03-10' and base_analizer(line,'et')<>'';
-------------------------------------------------------

导入脚本 dwd_base_log.sh
-------------------------------------------------------
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
----------------------------------------------------------

3. 用户行为事件分类表

商品点击表建表语句
----------------------------------------------------------
drop table if exists dwd_display_log;
CREATE EXTERNAL TABLE dwd_display_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
    `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `action` string,
    `goodsid` string,
    `place` string,
    `extend1` string,
    `category` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_display_log/';
----------------------------------------------------------

商品点击表数据加载
----------------------------------------------------------
insert overwrite table dwd_display_log
PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.goodsid') goodsid,
    get_json_object(event_json,'$.kv.place') place,
    get_json_object(event_json,'$.kv.extend1') extend1,
    get_json_object(event_json,'$.kv.category') category,
    server_time
from dwd_base_event_log 
where dt='2020-03-10' and event_name='display';
-----------------------------------------------------------

商品详情页表建表语句
-----------------------------------------------------------
drop table if exists dwd_newsdetail_log;
CREATE EXTERNAL TABLE dwd_newsdetail_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string, 
    `app_time` string,  
    `network` string, 
    `lng` string, 
    `lat` string, 
    `entry` string,
    `action` string,
    `goodsid` string,
    `showtype` string,
    `news_staytime` string,
    `loading_time` string,
    `type1` string,
    `category` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_newsdetail_log/';
-------------------------------------------------------------

商品详情页表数据加载
-------------------------------------------------------------
insert overwrite table dwd_newsdetail_log PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.entry') entry,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.goodsid') goodsid,
    get_json_object(event_json,'$.kv.showtype') showtype,
    get_json_object(event_json,'$.kv.news_staytime') news_staytime,
    get_json_object(event_json,'$.kv.loading_time') loading_time,
    get_json_object(event_json,'$.kv.type1') type1,
    get_json_object(event_json,'$.kv.category') category,
    server_time
from dwd_base_event_log
where dt='2020-03-10' and event_name='newsdetail';
--------------------------------------------------------------

商品列表页表建表语句
--------------------------------------------------------------
drop table if exists dwd_loading_log;
CREATE EXTERNAL TABLE dwd_loading_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string,
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `action` string,
    `loading_time` string,
    `loading_way` string,
    `extend1` string,
    `extend2` string,
    `type` string,
    `type1` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_loading_log/';
-------------------------------------------------------------

商品列表页表数据加载
-------------------------------------------------------------
insert overwrite table dwd_loading_log PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.loading_time') loading_time,
    get_json_object(event_json,'$.kv.loading_way') loading_way,
    get_json_object(event_json,'$.kv.extend1') extend1,
    get_json_object(event_json,'$.kv.extend2') extend2,
    get_json_object(event_json,'$.kv.type') type,
    get_json_object(event_json,'$.kv.type1') type1,
    server_time
from dwd_base_event_log
where dt='2020-03-10' and event_name='loading';
------------------------------------------------------------

广告表建表语句
------------------------------------------------------------
drop table if exists dwd_ad_log;
CREATE EXTERNAL TABLE dwd_ad_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `entry` string,
    `action` string,
    `content` string,
    `detail` string,
    `ad_source` string,
    `behavior` string,
    `newstype` string,
    `show_style` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_ad_log/';
-------------------------------------------------------------

广告表数据加载
-------------------------------------------------------------
insert overwrite table dwd_ad_log PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.entry') entry,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.content') content,
    get_json_object(event_json,'$.kv.detail') detail,
    get_json_object(event_json,'$.kv.source') ad_source,
    get_json_object(event_json,'$.kv.behavior') behavior,
    get_json_object(event_json,'$.kv.newstype') newstype,
    get_json_object(event_json,'$.kv.show_style') show_style,
    server_time
from dwd_base_event_log 
where dt='2020-03-10' and event_name='ad';
-------------------------------------------------------------

消息通知表建表语句
-------------------------------------------------------------
drop table if exists dwd_notification_log;
CREATE EXTERNAL TABLE dwd_notification_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string,
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `action` string,
    `noti_type` string,
    `ap_time` string,
    `content` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_notification_log/';
------------------------------------------------------------

消息通知表数据加载
------------------------------------------------------------
insert overwrite table dwd_notification_log
PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.noti_type') noti_type,
    get_json_object(event_json,'$.kv.ap_time') ap_time,
    get_json_object(event_json,'$.kv.content') content,
    server_time
from dwd_base_event_log
where dt='2020-03-10' and event_name='notification';
-----------------------------------------------------------

用户后台活跃表建表语句
-----------------------------------------------------------
drop table if exists dwd_active_background_log;
CREATE EXTERNAL TABLE dwd_active_background_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
     `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `active_source` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_background_log/';
-----------------------------------------------------------

用户后台活跃表数据加载
-----------------------------------------------------------
insert overwrite table dwd_active_background_log
PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.active_source') active_source,
    server_time
from dwd_base_event_log
where dt='2020-03-10' and event_name='active_background';
------------------------------------------------------------

评价表建表语句
------------------------------------------------------------
drop table if exists dwd_comment_log;
CREATE EXTERNAL TABLE dwd_comment_log(
    `mid_id` string,
    `user_id` string,
    `version_code` string,
    `version_name` string,
    `lang` string,
    `source` string,
    `os` string,
    `area` string,
    `model` string,
    `brand` string,
    `sdk_version` string,
    `gmail` string,
    `height_width` string,
    `app_time` string,
    `network` string,
    `lng` string,
    `lat` string,
    `comment_id` int,
    `userid` int,
    `p_comment_id` int, 
    `content` string,
    `addtime` string,
    `other_id` int,
    `praise_count` int,
    `reply_count` int,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_comment_log/';
------------------------------------------------------------

评价表数据加载
------------------------------------------------------------
insert overwrite table dwd_comment_log
PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.comment_id') comment_id,
    get_json_object(event_json,'$.kv.userid') userid,
    get_json_object(event_json,'$.kv.p_comment_id') p_comment_id,
    get_json_object(event_json,'$.kv.content') content,
    get_json_object(event_json,'$.kv.addtime') addtime,
    get_json_object(event_json,'$.kv.other_id') other_id,
    get_json_object(event_json,'$.kv.praise_count') praise_count,
    get_json_object(event_json,'$.kv.reply_count') reply_count,
    server_time
from dwd_base_event_log
where dt='2020-03-10' and event_name='comment';
-------------------------------------------------------------

收藏表建表语句
-------------------------------------------------------------
drop table if exists dwd_favorites_log;
CREATE EXTERNAL TABLE dwd_favorites_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `id` int, 
    `course_id` int, 
    `userid` int,
    `add_time` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_favorites_log/';
-------------------------------------------------------------

收藏表数据加载
-------------------------------------------------------------
insert overwrite table dwd_favorites_log
PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.id') id,
    get_json_object(event_json,'$.kv.course_id') course_id,
    get_json_object(event_json,'$.kv.userid') userid,
    get_json_object(event_json,'$.kv.add_time') add_time,
    server_time
from dwd_base_event_log 
where dt='2020-03-10' and event_name='favorites';
------------------------------------------------------------

点赞表建表语句
------------------------------------------------------------
drop table if exists dwd_praise_log;
CREATE EXTERNAL TABLE dwd_praise_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `id` string, 
    `userid` string, 
    `target_id` string,
    `type` string,
    `add_time` string,
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_praise_log/';
------------------------------------------------------------

点赞表数据加载
------------------------------------------------------------
insert overwrite table dwd_praise_log
PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.id') id,
    get_json_object(event_json,'$.kv.userid') userid,
    get_json_object(event_json,'$.kv.target_id') target_id,
    get_json_object(event_json,'$.kv.type') type,
    get_json_object(event_json,'$.kv.add_time') add_time,
    server_time
from dwd_base_event_log
where dt='2020-03-10' and event_name='praise';
------------------------------------------------------------

错误日志表建表语句
------------------------------------------------------------
drop table if exists dwd_error_log;
CREATE EXTERNAL TABLE dwd_error_log(
    `mid_id` string,
    `user_id` string, 
    `version_code` string, 
    `version_name` string, 
    `lang` string, 
    `source` string, 
    `os` string, 
    `area` string, 
    `model` string,
    `brand` string, 
    `sdk_version` string, 
    `gmail` string, 
    `height_width` string,  
    `app_time` string,
    `network` string, 
    `lng` string, 
    `lat` string, 
    `errorBrief` string, 
    `errorDetail` string, 
    `server_time` string
)
PARTITIONED BY (dt string)
location '/warehouse/gmall/dwd/dwd_error_log/';
-----------------------------------------------------------

错误日志表数据加载
-----------------------------------------------------------
insert overwrite table dwd_error_log
PARTITION (dt='2020-03-10')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.errorBrief') errorBrief,
    get_json_object(event_json,'$.kv.errorDetail') errorDetail,
    server_time
from dwd_base_event_log 
where dt='2020-03-10' and event_name='error';
-------------------------------------------------------------

用户行为事件分类表数据导入脚本 ods_to_dwd_event_log.sh
-------------------------------------------------------------
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
insert overwrite table "$APP".dwd_display_log
PARTITION (dt='$do_date')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.goodsid') goodsid,
    get_json_object(event_json,'$.kv.place') place,
    get_json_object(event_json,'$.kv.extend1') extend1,
    get_json_object(event_json,'$.kv.category') category,
    server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='display';


insert overwrite table "$APP".dwd_newsdetail_log
PARTITION (dt='$do_date')
select 
    mid_id,
    user_id,
    version_code,
    version_name,
    lang,
    source,
    os,
    area,
    model,
    brand,
    sdk_version,
    gmail,
    height_width,
    app_time,
    network,
    lng,
    lat,
	get_json_object(event_json,'$.kv.entry') entry,
	get_json_object(event_json,'$.kv.action') action,
	get_json_object(event_json,'$.kv.goodsid') goodsid,
	get_json_object(event_json,'$.kv.showtype') showtype,
	get_json_object(event_json,'$.kv.news_staytime') news_staytime,
	get_json_object(event_json,'$.kv.loading_time') loading_time,
	get_json_object(event_json,'$.kv.type1') type1,
	get_json_object(event_json,'$.kv.category') category,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='newsdetail';


insert overwrite table "$APP".dwd_loading_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.action') action,
	get_json_object(event_json,'$.kv.loading_time') loading_time,
	get_json_object(event_json,'$.kv.loading_way') loading_way,
	get_json_object(event_json,'$.kv.extend1') extend1,
	get_json_object(event_json,'$.kv.extend2') extend2,
	get_json_object(event_json,'$.kv.type') type,
	get_json_object(event_json,'$.kv.type1') type1,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='loading';


insert overwrite table "$APP".dwd_ad_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
    get_json_object(event_json,'$.kv.entry') entry,
    get_json_object(event_json,'$.kv.action') action,
    get_json_object(event_json,'$.kv.contentType') contentType,
    get_json_object(event_json,'$.kv.displayMills') displayMills,
    get_json_object(event_json,'$.kv.itemId') itemId,
    get_json_object(event_json,'$.kv.activityId') activityId,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='ad';


insert overwrite table "$APP".dwd_notification_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.action') action,
	get_json_object(event_json,'$.kv.noti_type') noti_type,
	get_json_object(event_json,'$.kv.ap_time') ap_time,
	get_json_object(event_json,'$.kv.content') content,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='notification';


insert overwrite table "$APP".dwd_active_background_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.active_source') active_source,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='active_background';


insert overwrite table "$APP".dwd_comment_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.comment_id') comment_id,
	get_json_object(event_json,'$.kv.userid') userid,
	get_json_object(event_json,'$.kv.p_comment_id') p_comment_id,
	get_json_object(event_json,'$.kv.content') content,
	get_json_object(event_json,'$.kv.addtime') addtime,
	get_json_object(event_json,'$.kv.other_id') other_id,
	get_json_object(event_json,'$.kv.praise_count') praise_count,
	get_json_object(event_json,'$.kv.reply_count') reply_count,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='comment';


insert overwrite table "$APP".dwd_favorites_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.id') id,
	get_json_object(event_json,'$.kv.course_id') course_id,
	get_json_object(event_json,'$.kv.userid') userid,
	get_json_object(event_json,'$.kv.add_time') add_time,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='favorites';


insert overwrite table "$APP".dwd_praise_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.id') id,
	get_json_object(event_json,'$.kv.userid') userid,
	get_json_object(event_json,'$.kv.target_id') target_id,
	get_json_object(event_json,'$.kv.type') type,
	get_json_object(event_json,'$.kv.add_time') add_time,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='praise';


insert overwrite table "$APP".dwd_error_log
PARTITION (dt='$do_date')
select 
	mid_id,
	user_id,
	version_code,
	version_name,
	lang,
	source,
	os,
	area,
	model,
	brand,
	sdk_version,
	gmail,
	height_width,
	app_time,
	network,
	lng,
	lat,
	get_json_object(event_json,'$.kv.errorBrief') errorBrief,
	get_json_object(event_json,'$.kv.errorDetail') errorDetail,
	server_time
from "$APP".dwd_base_event_log 
where dt='$do_date' and event_name='error';
"

$hive -e "$sql"
--------------------------------------------------------------















