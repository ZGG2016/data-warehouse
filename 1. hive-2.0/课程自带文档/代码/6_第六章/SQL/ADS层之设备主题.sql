ADS层之设备主题

1. 活跃设备数
建表语句
--------------------------------------------------------------
drop table if exists ads_uv_count;
create external table ads_uv_count( 
    `dt` string COMMENT '统计日期',
    `day_count` bigint COMMENT '当日活跃设备数量',
    `wk_count`  bigint COMMENT '当周活跃设备数量',
    `mn_count`  bigint COMMENT '当月活跃设备数量',
    `is_weekend` string COMMENT 'Y或N表示是否是周末,用于得到本周最终结果',
    `is_monthend` string COMMENT 'Y或N表示是否是月末,用于得到本月最终结果' 
) COMMENT '活跃设备数表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_uv_count 
select  
    '2020-03-10' dt,
    daycount.ct,
    wkcount.ct,
    mncount.ct,
    if(date_add(next_day('2020-03-10','MO'),-1)='2020-03-10','Y','N') ,
    if(last_day('2020-03-10')='2020-03-10','Y','N') 
from 
(
    select  
        '2020-03-10' dt,
        count(*) ct
    from dwt_uv_topic
    where login_date_last='2020-03-10'  
)daycount join 
( 
    select  
        '2020-03-10' dt,
        count (*) ct
    from dwt_uv_topic
    where login_date_last>=date_add(next_day('2020-03-10','MO'),-7) 
    and login_date_last<= date_add(next_day('2020-03-10','MO'),-1) 
) wkcount on daycount.dt=wkcount.dt
join 
( 
    select  
        '2020-03-10' dt,
        count (*) ct
    from dwt_uv_topic
    where date_format(login_date_last,'yyyy-MM')=date_format('2020-03-10',
'yyyy-MM')  
)mncount on daycount.dt=mncount.dt;
--------------------------------------------------------------

2. 每日新增设备数
建表语句
--------------------------------------------------------------
drop table if exists ads_new_mid_count;
create external table ads_new_mid_count
(
    `create_date`     string comment '创建时间' ,
    `new_mid_count`   BIGINT comment '新增设备数量' 
)  COMMENT '每日新增设备数表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_new_mid_count 
select
    login_date_first,
    count(*)
from dwt_uv_topic
where login_date_first='2020-03-10'
group by login_date_first;
--------------------------------------------------------------

3. 沉默设备数
建表语句
--------------------------------------------------------------
drop table if exists ads_silent_count;
create external table ads_silent_count( 
    `dt` string COMMENT '统计日期',
    `silent_count` bigint COMMENT '沉默设备数'
)COMMENT '沉默设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_silent_count';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_silent_count
select
    '2020-03-15',
    count(*) 
from dwt_uv_topic
where login_date_first=login_date_last
and login_date_last<=date_add('2020-03-15',-7);
--------------------------------------------------------------

4. 本周回流设备数
建表语句
--------------------------------------------------------------
drop table if exists ads_back_count;
create external table ads_back_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '统计日期所在周',
    `wastage_count` bigint COMMENT '回流设备数'
) COMMENT '本周回流设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_back_count
select
    '2020-03-15',
    count(*)
from
(
    select
        mid_id
    from dwt_uv_topic
    where login_date_last>=date_add(next_day('2020-03-15','MO'),-7) 
    and login_date_last<= date_add(next_day('2020-03-15','MO'),-1)
    and login_date_first<date_add(next_day('2020-03-15','MO'),-7)
)current_wk
left join
(
    select
        mid_id
    from dws_uv_detail_daycount
    where dt>=date_add(next_day('2020-03-15','MO'),-7*2) 
    and dt<= date_add(next_day('2020-03-15','MO'),-7-1) 
    group by mid_id
)last_wk
on current_wk.mid_id=last_wk.mid_id
where last_wk.mid_id is null;
--------------------------------------------------------------

5. 流失设备数
建表语句
--------------------------------------------------------------
drop table if exists ads_wastage_count;
create external table ads_wastage_count( 
    `dt` string COMMENT '统计日期',
    `wastage_count` bigint COMMENT '流失设备数'
) COMMENT '流失设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_wastage_count
select
     '2020-03-20',
     count(*)
from 
(
    select 
        mid_id
    from dwt_uv_topic
    where login_date_last<=date_add('2020-03-20',-7)
    group by mid_id
)t1;
--------------------------------------------------------------

6. 留存率
建表语句
--------------------------------------------------------------
drop table if exists ads_user_retention_day_rate;
create external table ads_user_retention_day_rate 
(
     `stat_date`          string comment '统计日期',
     `create_date`       string  comment '设备新增日期',
     `retention_day`     int comment '截至当前日期留存天数',
     `retention_count`    bigint comment  '留存数量',
     `new_mid_count`     bigint comment '设备新增数量',
     `retention_ratio`   decimal(10,2) comment '留存率'
)  COMMENT '每日设备留存表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_user_retention_day_rate
select
    '2020-03-10',--统计日期
    date_add('2020-03-10',-1),--新增日期
    1,--留存天数
    sum(if(login_date_first=date_add('2020-03-10',-1) and login_date_last=
'2020-03-10',1,0)),--2020-03-09的1日留存数
    sum(if(login_date_first=date_add('2020-03-10',-1),1,0)),--2020-03-09新增
    sum(if(login_date_first=date_add('2020-03-10',-1) and login_date_last=
'2020-03-10',1,0))/sum(if(login_date_first=date_add('2020-03-10',-1),1,0))*100
from dwt_uv_topic

union all

select
    '2020-03-10',--统计日期
    date_add('2020-03-10',-2),--新增日期
    2,--留存天数
    sum(if(login_date_first=date_add('2020-03-10',-2) and login_date_last=
'2020-03-10',1,0)),--2020-03-08的2日留存数
    sum(if(login_date_first=date_add('2020-03-10',-2),1,0)),--2020-03-08新增
    sum(if(login_date_first=date_add('2020-03-10',-2) and login_date_last=
'2020-03-10',1,0))/sum(if(login_date_first=date_add('2020-03-10',-2),1,0))*100
from dwt_uv_topic

union all

select
    '2020-03-10',--统计日期
    date_add('2020-03-10',-3),--新增日期
    3,--留存天数
    sum(if(login_date_first=date_add('2020-03-10',-3) and login_date_last=
'2020-03-10',1,0)),--2020-03-07的3日留存数
    sum(if(login_date_first=date_add('2020-03-10',-3),1,0)),--2020-03-07新增
    sum(if(login_date_first=date_add('2020-03-10',-3) and login_date_last=
'2020-03-10',1,0))/sum(if(login_date_first=date_add('2020-03-10',-3),1,0))*100
from dwt_uv_topic;
--------------------------------------------------------------

7. 最近连续三周活跃设备数
建表语句
--------------------------------------------------------------
drop table if exists ads_continuity_wk_count;
create external table ads_continuity_wk_count( 
    `dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
    `wk_dt` string COMMENT '持续时间',
    `continuity_count` bigint COMMENT '活跃次数'
) 
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_continuity_wk_count
select
    '2020-03-10',
    concat(date_add(next_day('2020-03-10','MO'),-7*3),'_',date_add(next_day('2020-03-10','MO'),-1)),
    count(*)
from
(
    select
        mid_id
    from
    (
        Select -–查找本周活跃设备
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('2020-03-10','monday'),-7)
        and dt<=date_add(next_day('2020-03-10','monday'),-1)
        group by mid_id

        union all

        select --查找上周活跃设备
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('2020-03-10','monday'),-7*2)
        and dt<=date_add(next_day('2020-03-10','monday'),-7-1)
        group by mid_id

        union all

        select --查找上上周活跃设备
            mid_id
        from dws_uv_detail_daycount
        where dt>=date_add(next_day('2020-03-10','monday'),-7*3)
        and dt<=date_add(next_day('2020-03-10','monday'),-7*2-1)
        group by mid_id 
    )t1
    group by mid_id  --对三周内的所有活跃设备进行分组
    having count(*)=3 –-分组后mid_id个数为3的设备为最近连续三周活跃设备
)t2;
--------------------------------------------------------------

8. 最近7天内连续3天活跃设备数
建表语句
--------------------------------------------------------------
drop table if exists ads_continuity_uv_count;
create external table ads_continuity_uv_count( 
    `dt` string COMMENT '统计日期',
    `wk_dt` string COMMENT '最近七天日期',
    `continuity_count` bigint
) COMMENT '最近七天内连续三天活跃设备数表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_uv_count';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_continuity_uv_count
select
    '2020-03-10',
    concat(date_add('2020-03-10',-6),'_','2020-03-10'),
    count(*)
from
(
    select mid_id
    from
    (
        select mid_id      
        from
        (
            select 
                mid_id,
                date_sub(dt,rank) date_dif --取排序值与日期值之差作为连续标志
            from
            (
                select 
                    mid_id,
                    dt,
                    –-对七天内登录过的设备按照登录日期进行排序
                    rank() over(partition by mid_id order by dt) rank 
                from dws_uv_detail_daycount
                where dt>=date_add('2020-03-10',-6) and dt<='2020-03-10'
            )t1
        )t2 
        group by mid_id,date_dif –-按照连续标志和设备的mid进行分组
        having count(*)>=3 --分组后个数大于3的设备为最近七天内连续三天活跃的设备
    )t3 
    group by mid_id
)t4;
--------------------------------------------------------------

