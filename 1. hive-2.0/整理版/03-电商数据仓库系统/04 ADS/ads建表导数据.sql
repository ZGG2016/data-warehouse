-- 活跃设备数（日、周、月）
drop table if exists ads_uv_count;
create external table ads_uv_count(
`dt` string COMMENT '统计日期',
`day_count` bigint COMMENT '当日用户数量',
`wk_count` bigint COMMENT '当周用户数量',
`mn_count` bigint COMMENT '当月用户数量',
`is_weekend` string COMMENT 'Y,N 是否是周末,用于得到本周最终结果',
`is_monthend` string COMMENT 'Y,N 是否是月末,用于得到本月最终结果'
) COMMENT '活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_uv_count/';

insert into table ads_uv_count
select
'2022-09-20' dt,
daycount.ct,
wkcount.ct,
mncount.ct,
if(date_add(next_day('2022-09-20','MO'),-1)='2022-09-20','Y','N') ,
if(last_day('2022-09-20')='2022-09-20','Y','N')
from
(
select
'2022-09-20' dt,
count(*) ct
from dwt_uv_topic
where login_date_last='2022-09-20'
)daycount join
(
select
'2022-09-20' dt,
count (*) ct
from dwt_uv_topic
where login_date_last>=date_add(next_day('2022-09-20','MO'),-7)
and login_date_last<= date_add(next_day('2022-09-20','MO'),-1)
) wkcount on daycount.dt=wkcount.dt
join
(
select
'2022-09-20' dt,
count (*) ct
from dwt_uv_topic
where date_format(login_date_last,'yyyy-MM')=date_format('2022-09-20','yyyy-MM')
)mncount on daycount.dt=mncount.dt;

-- 或
insert into table ads_uv_count
select
    '2022-09-20',
    sum(if(login_date_last='2022-09-20',1,0)),
    sum(if(login_date_last>=date_add(next_day('2022-09-20','monday'),-7) and login_date_last<=date_add(next_day('2022-09-20','monday'),-1) ,1,0)),
    sum(if(date_format(login_date_last,'yyyy-MM')=date_format('2022-09-20','yyyy-MM'),1,0)),
    if('2022-09-20'=date_add(next_day('2022-09-20','monday'),-1),'Y','N'),
    if('2022-09-20'=last_day('2022-09-20'),'Y','N')
from dwt_uv_topic;

-- 每日新增设备
drop table if exists ads_new_mid_count;
create external table ads_new_mid_count
(
`create_date` string comment '创建时间' ,
`new_mid_count` BIGINT comment '新增设备数量'
) COMMENT '每日新增设备信息数量'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_new_mid_count/';

insert into table ads_new_mid_count
select
'2022-09-20',
count(*)
from dwt_uv_topic
where login_date_first='2022-09-20';

-- 沉默用户数
drop table if exists ads_silent_count;
create external table ads_silent_count(
`dt` string COMMENT '统计日期',
`silent_count` bigint COMMENT '沉默设备数'
)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_silent_count';

insert into table ads_silent_count
select
'2022-09-20',
count(*)
from dwt_uv_topic
where login_date_first=login_date_last
and login_date_last<=date_add('2022-09-20',-7);

-- 本周回流用户数
drop table if exists ads_back_count;
create external table ads_back_count(
`dt` string COMMENT '统计日期',
`wk_dt` string COMMENT '统计日期所在周',
`wastage_count` bigint COMMENT '回流设备数'
)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_back_count';

insert into table ads_back_count
select
'2022-09-20',
concat(date_add(next_day('2022-09-20','MO'),-7),'_',date_add(next_day('2022-09-20','MO'),-1)),
count(*)
from
(
select
mid_id
from dwt_uv_topic
where login_date_last>=date_add(next_day('2022-09-20','MO'),-7)
and login_date_last<= date_add(next_day('2022-09-20','MO'),-1)
and login_date_first<date_add(next_day('2022-09-20','MO'),-7)
)current_wk
left join
(
select
mid_id
from dws_uv_detail_daycount
where dt>=date_add(next_day('2022-09-20','MO'),-7*2)
and dt<= date_add(next_day('2022-09-20','MO'),-7-1)
group by mid_id
)last_wk
on current_wk.mid_id=last_wk.mid_id
where last_wk.mid_id is null;

-- 流失用户数
drop table if exists ads_wastage_count;
create external table ads_wastage_count(
`dt` string COMMENT '统计日期',
`wastage_count` bigint COMMENT '流失设备数'
)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_wastage_count';

insert into table ads_wastage_count
select
'2022-09-20',
count(*)
from
(
select
mid_id
from dwt_uv_topic
where login_date_last<=date_add('2022-09-20',-7)
group by mid_id
)t1;


select
'2022-09-20',
count(*)
from
(
select
*
from dwt_uv_topic
where login_date_last<=date_add('2022-09-20',-7)
)t1;

-- 留存率
drop table if exists ads_user_retention_day_rate;
create external table ads_user_retention_day_rate
(
`stat_date` string comment '统计日期',
`create_date` string comment '设备新增日期',
`retention_day` int comment '截止当前日期留存天数',
`retention_count` bigint comment '留存数量',
`new_mid_count` bigint comment '设备新增数量',
`retention_ratio` decimal(10,2) comment '留存率'
) COMMENT '每日用户留存情况'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_retention_day_rate/';


insert into table ads_user_retention_day_rate
select
'2022-09-20',--统计日期
date_add('2022-09-20',-1),--新增日期
1,--留存天数
sum(if(login_date_first=date_add('2022-09-20',-1) and
login_date_last='2022-09-20',1,0)),--2020-03-09 的 1 日留存数
sum(if(login_date_first=date_add('2022-09-20',-1),1,0)),--2020-03-09 新增
sum(if(login_date_first=date_add('2022-09-20',-1) and
login_date_last='2022-09-20',1,0))/sum(if(login_date_first=date_add('2022-09-20',-1),1,0))*100
from dwt_uv_topic
union all
select
'2022-09-20',--统计日期
date_add('2022-09-20',-2),--新增日期
2,--留存天数
sum(if(login_date_first=date_add('2022-09-20',-2) and
login_date_last='2022-09-20',1,0)),--2020-03-08 的 2 日留存数
sum(if(login_date_first=date_add('2022-09-20',-2),1,0)),--2020-03-08 新增
sum(if(login_date_first=date_add('2022-09-20',-2) and
login_date_last='2022-09-20',1,0))/sum(if(login_date_first=date_add('2022-09-20',-2),1,0))*100
from dwt_uv_topic
union all
select
'2022-09-20',--统计日期
date_add('2022-09-20',-3),--新增日期
3,--留存天数
sum(if(login_date_first=date_add('2022-09-20',-3) and
login_date_last='2022-09-20',1,0)),--2020-03-07 的 3 日留存数
sum(if(login_date_first=date_add('2022-09-20',-3),1,0)),--2020-03-07 新增
sum(if(login_date_first=date_add('2022-09-20',-3) and
login_date_last='2022-09-20',1,0))/sum(if(login_date_first=date_add('2022-09-20',-3),1,0))*100
from dwt_uv_topic;

--  最近连续三周活跃用户数
drop table if exists ads_continuity_wk_count;
create external table ads_continuity_wk_count(
`dt` string COMMENT '统计日期,一般用结束周周日日期,如果每天计算一次,可用当天日期',
`wk_dt` string COMMENT '持续时间',
`continuity_count` bigint COMMENT '活跃次数'
)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_wk_count';

insert into table ads_continuity_wk_count
select
'2022-09-20',
concat(date_add(next_day('2022-09-20','MO'),-7*3),'_',date_add(next_day('2022-09-20','MO'),-1)),
count(*)
from
(
select
mid_id
from
(
select
mid_id
from dws_uv_detail_daycount
where dt>=date_add(next_day('2022-09-20','monday'),-7)
and dt<=date_add(next_day('2022-09-20','monday'),-1)
group by mid_id
union all
select
mid_id
from dws_uv_detail_daycount
where dt>=date_add(next_day('2022-09-20','monday'),-7*2)
and dt<=date_add(next_day('2022-09-20','monday'),-7-1)
group by mid_id
union all
select
mid_id
from dws_uv_detail_daycount
where dt>=date_add(next_day('2022-09-20','monday'),-7*3)
and dt<=date_add(next_day('2022-09-20','monday'),-7*2-1)
group by mid_id
)t1
group by mid_id
having count(*)=3
)t2;

-- 最近七天内连续三天活跃用户数
drop table if exists ads_continuity_uv_count;
create external table ads_continuity_uv_count(
`dt` string COMMENT '统计日期',
`wk_dt` string COMMENT '最近 7 天日期',
`continuity_count` bigint
) COMMENT '连续活跃设备数'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_continuity_uv_count';

insert into table ads_continuity_uv_count
select
'2022-09-20',
concat(date_add('2022-09-20',-6),'_','2022-09-20'),
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
date_sub(dt,rank) date_dif
from
(
select
mid_id,
dt,
rank() over(partition by mid_id order by dt) rank
from dws_uv_detail_daycount
where dt>=date_add('2022-09-20',-6) and
dt<='2022-09-20'
)t1
)t2
group by mid_id,date_dif
having count(*)>=3
)t3
group by mid_id
)t4;

-- 会员主题信息
drop table if exists ads_user_topic;
create external table ads_user_topic(
`dt` string COMMENT '统计日期',
`day_users` string COMMENT '活跃会员数',
`day_new_users` string COMMENT '新增会员数',
`day_new_payment_users` string COMMENT '新增消费会员数',
`payment_users` string COMMENT '总付费会员数',
`users` string COMMENT '总会员数',
`day_users2users` decimal(10,2) COMMENT '会员活跃率',
`payment_users2users` decimal(10,2) COMMENT '会员付费率',
`day_new_users2users` decimal(10,2) COMMENT '会员新鲜度'
) COMMENT '会员主题信息表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_topic';

insert into table ads_user_topic
select
'2022-09-20',
sum(if(login_date_last='2022-09-20',1,0)),
sum(if(login_date_first='2022-09-20',1,0)),
sum(if(payment_date_first='2022-09-20',1,0)),
sum(if(payment_count>0,1,0)),
count(*),
sum(if(login_date_last='2022-09-20',1,0))/count(*),
sum(if(payment_count>0,1,0))/count(*),
sum(if(login_date_first='2022-09-20',1,0))/sum(if(login_date_last='2022-09-20',1,0))
from dwt_user_topic;

-- 漏斗分析
drop table if exists ads_user_action_convert_day;
create external table ads_user_action_convert_day(
`dt` string COMMENT '统计日期',
`total_visitor_m_count` bigint COMMENT '总访问人数',
`cart_u_count` bigint COMMENT '加入购物车的人数',
`visitor2cart_convert_ratio` decimal(10,2) COMMENT '访问到加入购物车转化率',
`order_u_count` bigint COMMENT '下单人数',
`cart2order_convert_ratio` decimal(10,2) COMMENT '加入购物车到下单转化率',
`payment_u_count` bigint COMMENT '支付人数',
`order2payment_convert_ratio` decimal(10,2) COMMENT '下单到支付的转化率'
) COMMENT '用户行为漏斗分析'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_action_convert_day/';

insert into table ads_user_action_convert_day
select
'2022-09-20',
uv.day_count,
ua.cart_count,
cast(ua.cart_count/uv.day_count as decimal(10,2)) visitor2cart_convert_ratio,
ua.order_count,
cast(ua.order_count/ua.cart_count as decimal(10,2)) visitor2order_convert_ratio,
ua.payment_count,
cast(ua.payment_count/ua.order_count as decimal(10,2)) order2payment_convert_ratio
from
(
select
dt,
sum(if(cart_count>0,1,0)) cart_count,
sum(if(order_count>0,1,0)) order_count,
sum(if(payment_count>0,1,0)) payment_count
from dws_user_action_daycount
where dt='2022-09-20'
group by dt
)ua join ads_uv_count uv on uv.dt=ua.dt;

--  商品个数信息
drop table if exists ads_product_info;
create external table ads_product_info(
`dt` string COMMENT '统计日期',
`sku_num` string COMMENT 'sku 个数',
`spu_num` string COMMENT 'spu 个数'
) COMMENT '商品个数信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_info';


insert into table ads_product_info
select
'2022-09-20' dt,
sku_num,
spu_num
from
(
select
'2022-09-20' dt,
count(*) sku_num
from
dwt_sku_topic
) tmp_sku_num
join
(
select
'2022-09-20' dt,
count(*) spu_num
from
(
select
spu_id
from
dwt_sku_topic
group by
spu_id
) tmp_spu_id
) tmp_spu_num
on
tmp_sku_num.dt=tmp_spu_num.dt;   

-- 商品销量排名
drop table if exists ads_product_sale_topN;
create external table ads_product_sale_topN(
`dt` string COMMENT '统计日期',
`sku_id` string COMMENT '商品 ID',
`payment_amount` bigint COMMENT '销量'
) COMMENT '商品个数信息'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_sale_topN';

insert into table ads_product_sale_topN
select
'2022-09-20' dt,
sku_id,
payment_amount
from
dws_sku_action_daycount
where
dt='2022-09-20'
order by payment_amount desc
limit 10;

-- 商品收藏排名
drop table if exists ads_product_favor_topN;
create external table ads_product_favor_topN(
`dt` string COMMENT '统计日期',
`sku_id` string COMMENT '商品 ID',
`favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏 TopN'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_favor_topN';

insert into table ads_product_favor_topN
select
'2022-09-20' dt,
sku_id,
favor_count
from
dws_sku_action_daycount
where
dt='2022-09-20'
order by favor_count desc
limit 10;

-- 商品加入购物车排名
drop table if exists ads_product_cart_topN;
create external table ads_product_cart_topN(
`dt` string COMMENT '统计日期',
`sku_id` string COMMENT '商品 ID',
`cart_num` bigint COMMENT '加入购物车数量'
) COMMENT '商品加入购物车 TopN'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_cart_topN';

insert into table ads_product_cart_topN
select
'2022-09-20' dt,
sku_id,
cart_num
from
dws_sku_action_daycount
where
dt='2022-09-20'
order by cart_num desc
limit 10;

-- 商品退款率排名( 最近 30 天)
drop table if exists ads_product_refund_topN;
create external table ads_product_refund_topN(
`dt` string COMMENT '统计日期',
`sku_id` string COMMENT '商品 ID',
`refund_ratio` decimal(10,2) COMMENT '退款率'
) COMMENT '商品退款率 TopN'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_refund_topN';

insert into table ads_product_refund_topN
select
'2022-09-20',
sku_id,
refund_last_30d_count/payment_last_30d_count*100 refund_ratio
from dwt_sku_topic
order by refund_ratio desc
limit 10;

-- 商品差评率
drop table if exists ads_appraise_bad_topN;
create external table ads_appraise_bad_topN(
`dt` string COMMENT '统计日期',
`sku_id` string COMMENT '商品 ID',
`appraise_bad_ratio` decimal(10,2) COMMENT '差评率'
) COMMENT '商品差评率 TopN'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_appraise_bad_topN';

insert into table ads_appraise_bad_topN
select
'2022-09-20' dt,
sku_id,
appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) appraise_bad_ratio
from
dws_sku_action_daycount
where
dt='2022-09-20'
order by appraise_bad_ratio desc
limit 10;

-- 下单数目统计
drop table if exists ads_order_daycount;
create external table ads_order_daycount(
dt string comment '统计日期',
order_count bigint comment '单日下单笔数',
order_amount bigint comment '单日下单金额',
order_users bigint comment '单日下单用户数'
) comment '每日订单总计表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_order_daycount';

insert into table ads_order_daycount
select
'2022-09-20',
sum(order_count),
sum(order_amount),
sum(if(order_count>0,1,0))
from dws_user_action_daycount
where dt='2022-09-20';

-- 支付信息统计
drop table if exists ads_payment_daycount;
create external table ads_payment_daycount(
dt string comment '统计日期',
order_count bigint comment '单日支付笔数',
order_amount bigint comment '单日支付金额',
payment_user_count bigint comment '单日支付人数',
payment_sku_count bigint comment '单日支付商品数',
payment_avg_time double comment '下单到支付的平均时长，取分钟数'
) comment '每日订单总计表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_payment_daycount';

insert into table ads_payment_daycount
select
tmp_payment.dt,
tmp_payment.payment_count,
tmp_payment.payment_amount,
tmp_payment.payment_user_count,
tmp_skucount.payment_sku_count,
tmp_time.payment_avg_time
from
(
select
'2022-09-20' dt,
sum(payment_count) payment_count,
sum(payment_amount) payment_amount,
sum(if(payment_count>0,1,0)) payment_user_count
from dws_user_action_daycount
where dt='2022-09-20'
)tmp_payment
join
(
select
'2022-09-20' dt,
sum(if(payment_count>0,1,0)) payment_sku_count
from dws_sku_action_daycount
where dt='2022-09-20'
)tmp_skucount on tmp_payment.dt=tmp_skucount.dt
join
(
select
'2022-09-20' dt,
sum(unix_timestamp(payment_time)-unix_timestamp(create_time))/count(*)/60 payment_avg_time
from dwd_fact_order_info
where dt='2022-09-20'
and payment_time is not null
)tmp_time on tmp_payment.dt=tmp_time.dt;

-- 复购率
drop table ads_sale_tm_category1_stat_mn;
create external table ads_sale_tm_category1_stat_mn
(
tm_id string comment '品牌 id',
category1_id string comment '1 级品类 id ',
category1_name string comment '1 级品类名称 ',
buycount bigint comment '购买人数',
buy_twice_last bigint comment '两次以上购买人数',
buy_twice_last_ratio decimal(10,2) comment '单次复购率',
buy_3times_last bigint comment '三次以上购买人数',
buy_3times_last_ratio decimal(10,2) comment '多次复购率',
stat_mn string comment '统计月份',
stat_date string comment '统计日期'
) COMMENT '复购率统计'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_sale_tm_category1_stat_mn/';

insert into table ads_sale_tm_category1_stat_mn
select
mn.sku_tm_id,
mn.sku_category1_id,
mn.sku_category1_name,
sum(if(mn.order_count>=1,1,0)) buycount,
sum(if(mn.order_count>=2,1,0)) buyTwiceLast,
sum(if(mn.order_count>=2,1,0))/sum( if(mn.order_count>=1,1,0)) buyTwiceLastRatio,
sum(if(mn.order_count>=3,1,0)) buy3timeLast ,
sum(if(mn.order_count>=3,1,0))/sum( if(mn.order_count>=1,1,0)) buy3timeLastRatio ,
date_format('2022-09-20' ,'yyyy-MM') stat_mn,
'2022-09-20' stat_date
from
(
select
user_id,
sd.sku_tm_id,
sd.sku_category1_id,
sd.sku_category1_name,
sum(order_count) order_count
from dws_sale_detail_daycount sd
where date_format(dt,'yyyy-MM')=date_format('2022-09-20' ,'yyyy-MM')
group by user_id, sd.sku_tm_id, sd.sku_category1_id, sd.sku_category1_name
) mn
group by mn.sku_tm_id, mn.sku_category1_id, mn.sku_category1_name;