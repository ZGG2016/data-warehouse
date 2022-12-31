ADS层之会员主题

1. 会员主题信息
建表语句
--------------------------------------------------------------
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
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_user_topic
select
    '2020-03-10',
    sum(if(login_date_last='2020-03-10',1,0)),
    sum(if(login_date_first='2020-03-10',1,0)),
    sum(if(payment_date_first='2020-03-10',1,0)),
    sum(if(payment_count>0,1,0)),
    count(*),
    sum(if(login_date_last='2020-03-10',1,0))/count(*),
    sum(if(payment_count>0,1,0))/count(*),
    sum(if(login_date_first='2020-03-10',1,0))/sum(if(login_date_last='2020-03-10',1,0))
from dwt_user_topic;
--------------------------------------------------------------

2. 用户行为漏斗分析
建表语句
--------------------------------------------------------------
drop table if exists ads_user_action_convert_day;
create external  table ads_user_action_convert_day(
    `dt` string COMMENT '统计日期',
    `total_visitor_m_count`  bigint COMMENT '总访问人数',
    `cart_u_count` bigint COMMENT '加入购物车的人数',
    `visitor2cart_convert_ratio` decimal(10,2) COMMENT '访问到加入购物车的转化率',
    `order_u_count` bigint     COMMENT '下单人数',
    `cart2order_convert_ratio`  decimal(10,2) COMMENT '加入购物车到下单的转化率',
    `payment_u_count` bigint     COMMENT '支付人数',
    `order2payment_convert_ratio` decimal(10,2) COMMENT '下单到支付的转化率'
 ) COMMENT '用户行为漏斗分析表'
row format delimited  fields terminated by '\t'
location '/warehouse/gmall/ads/ads_user_action_convert_day/';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_user_action_convert_day
select 
    '2020-03-10',
    uv.day_count,
    ua.cart_count,
    cast(ua.cart_count/uv.day_count as  decimal(10,2)) visitor2cart_convert_ratio,
    ua.order_count,
    cast(ua.order_count/ua.cart_count as  decimal(10,2)) visitor2order_convert_
ratio,
    ua.payment_count,
    cast(ua.payment_count/ua.order_count as  decimal(10,2)) order2payment_
convert_ratio
from  
(
    select 
        dt,
        sum(if(cart_count>0,1,0)) cart_count,
        sum(if(order_count>0,1,0)) order_count,
        sum(if(payment_count>0,1,0)) payment_count
    from dws_user_action_daycount
where dt='2020-03-10'
group by dt
)ua join ads_uv_count uv on uv.dt=ua.dt;
--------------------------------------------------------------