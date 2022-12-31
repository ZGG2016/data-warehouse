-- 启动表
drop table if exists dwd_start_log;
CREATE EXTERNAL TABLE dwd_start_log(
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
PARTITIONED BY (dt string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_start_log/'
TBLPROPERTIES('parquet.compression'='lzo');

-- {"action":"1","ar":"MX","ba":"HTC","detail":"201","en":"start","entry":"2","extend1":"","g":"3997DR54@gmail.com","hw":"640*1136","l":"pt","la":"-4.1","ln":"-53.0","loading_time":"0","md":"HTC-3","mid":"0","nw":"3G","open_ad_type":"1","os":"8.0.7","sr":"U","sv":"V2.3.5","t":"1663575497457","uid":"0","vc":"2","vn":"1.3.7"}
insert overwrite table dwd_start_log
PARTITION (dt='2022-09-20')
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
from ods_start_log
where dt='2022-09-20';


-- 也可以使用脚本导入
-- ods_to_dwd_start_log.sh 2022-09-21
-- ods_to_dwd_start_log.sh 2022-09-22

---------------------------------------------------------------

-- 事件表

-- 先建事件日志基础明细表（过渡表）
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
`server_time` string)
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dwd/dwd_base_event_log/'
TBLPROPERTIES('parquet.compression'='lzo');

-- 再写UDF UDTF函数(hivefunction java工程)
-- add jar /root/hivefunction-1.0-SNAPSHOT.jar;
-- create function base_analizer as 'org.zgg.udf.BaseFieldUDF';
-- create function flat_analizer as 'org.zgg.udtf.EventJsonUDTF';
-- 这里注意，如果修改了代码并重新上传了jar包，要重启客户端


-- 再使用函数解析、导入ods数据
insert overwrite table dwd_base_event_log partition(dt='2022-09-20')
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
from ods_event_log lateral view flat_analizer(base_analizer(line,'et')) tmp_flat as
event_name,event_json
where dt='2022-09-20' and base_analizer(line,'et')<>'';

-- 也可使用脚本导入(ods_to_dwd_base_event_log.sh)
-- ods_to_dwd_base_event_log.sh 2022-09-21
-- ods_to_dwd_base_event_log.sh 2022-09-22


-- 接下来，开始解析出各个用户行为表  （商品点击表、商品详情页表.....）
-- 见 `解析出各个用户行为表.sql`