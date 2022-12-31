-- 每日设备行为，主要按照设备 id 统计
drop table if exists dws_uv_detail_daycount;
create external table dws_uv_detail_daycount
(
`mid_id` string COMMENT '设备唯一标识',
`user_id` string COMMENT '用户标识',
`version_code` string COMMENT '程序版本号',
`version_name` string COMMENT '程序版本名',
`lang` string COMMENT '系统语言',
`source` string COMMENT '渠道号',
`os` string COMMENT '安卓系统版本',
`area` string COMMENT '区域',
`model` string COMMENT '手机型号',
`brand` string COMMENT '手机品牌',
`sdk_version` string COMMENT 'sdkVersion',
`gmail` string COMMENT 'gmail',
`height_width` string COMMENT '屏幕宽高',
`app_time` string COMMENT '客户端日志产生时的时间',
`network` string COMMENT '网络模式',
`lng` string COMMENT '经度',
`lat` string COMMENT '纬度',
`login_count` bigint COMMENT '活跃次数'
)
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_daycount';


insert overwrite table dws_uv_detail_daycount partition(dt='2022-09-20')
select
mid_id,
concat_ws('|', collect_set(user_id)) user_id,
concat_ws('|', collect_set(version_code)) version_code,
concat_ws('|', collect_set(version_name)) version_name,
concat_ws('|', collect_set(lang))lang,
concat_ws('|', collect_set(source)) source,
concat_ws('|', collect_set(os)) os,
concat_ws('|', collect_set(area)) area,
concat_ws('|', collect_set(model)) model,
concat_ws('|', collect_set(brand)) brand,
concat_ws('|', collect_set(sdk_version)) sdk_version,
concat_ws('|', collect_set(gmail)) gmail,
concat_ws('|', collect_set(height_width)) height_width,
concat_ws('|', collect_set(app_time)) app_time,
concat_ws('|', collect_set(network)) network,
concat_ws('|', collect_set(lng)) lng,
concat_ws('|', collect_set(lat)) lat,
count(*) login_count
from dwd_start_log
where dt='2022-09-20'
group by mid_id;


drop table if exists dwt_uv_topic;
create external table dwt_uv_topic
(
`mid_id` string COMMENT '设备唯一标识',
`user_id` string COMMENT '用户标识',
`version_code` string COMMENT '程序版本号',
`version_name` string COMMENT '程序版本名',
`lang` string COMMENT '系统语言',
`source` string COMMENT '渠道号',
`os` string COMMENT '安卓系统版本',
`area` string COMMENT '区域',
`model` string COMMENT '手机型号',
`brand` string COMMENT '手机品牌',
`sdk_version` string COMMENT 'sdkVersion',
`gmail` string COMMENT 'gmail',
`height_width` string COMMENT '屏幕宽高',
`app_time` string COMMENT '客户端日志产生时的时间',
`network` string COMMENT '网络模式',
`lng` string COMMENT '经度',
`lat` string COMMENT '纬度',
`login_date_first` string comment '首次活跃时间',
`login_date_last` string comment '末次活跃时间',
`login_day_count` bigint comment '当日活跃次数',
`login_count` bigint comment '累积活跃天数'
)
stored as parquet
location '/warehouse/gmall/dwt/dwt_uv_topic';

insert overwrite table dwt_uv_topic
select
nvl(new.mid_id,old.mid_id),
nvl(new.user_id,old.user_id),
nvl(new.version_code,old.version_code),
nvl(new.version_name,old.version_name),
nvl(new.lang,old.lang),
nvl(new.source,old.source),
nvl(new.os,old.os),
nvl(new.area,old.area),
nvl(new.model,old.model),
nvl(new.brand,old.brand),
nvl(new.sdk_version,old.sdk_version),
nvl(new.gmail,old.gmail),
nvl(new.height_width,old.height_width),
nvl(new.app_time,old.app_time),
nvl(new.network,old.network),
nvl(new.lng,old.lng),
nvl(new.lat,old.lat),
if(old.mid_id is null,'2022-09-20',old.login_date_first),
if(new.mid_id is not null,'2022-09-20',old.login_date_last),
if(new.mid_id is not null, new.login_count,0),
nvl(old.login_count,0)+if(new.login_count>0,1,0)
from
(
select
*
from dwt_uv_topic
)old
full outer join
(
select
*
from dws_uv_detail_daycount
where dt='2022-09-20'
)new
on old.mid_id=new.mid_id;
