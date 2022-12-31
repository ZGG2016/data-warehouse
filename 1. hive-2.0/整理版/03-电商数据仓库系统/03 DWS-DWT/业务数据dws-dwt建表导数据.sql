-- 每日 会员 行为 
drop table if exists dws_user_action_daycount;
create external table dws_user_action_daycount
(
user_id string comment '用户 id',
login_count bigint comment '登录次数',
cart_count bigint comment '加入购物车次数',
cart_amount double comment '加入购物车金额',
order_count bigint comment '下单次数',
order_amount decimal(16,2) comment '下单金额',
payment_count bigint comment '支付次数',
payment_amount decimal(16,2) comment '支付金额'
) COMMENT '每日用户行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action_daycount/'
tblproperties ("parquet.compression"="lzo");

with 
tmp_login as
(
    select
        user_id,
        count(*) login_count
    from dwd_start_log
    where dt='2022-09-20'
        and user_id is not null
    group by user_id
),
tmp_cart as
(
    select
        user_id,
        count(*) cart_count,
        sum(cart_price*sku_num) cart_amount
    from dwd_fact_cart_info
    where dt='2022-09-20'
        and user_id is not null
        and date_format(create_time,'yyyy-MM-dd')='2022-09-20'
    group by user_id
),
tmp_order as
(
    select
        user_id,
        count(*) order_count,
        sum(final_total_amount) order_amount
    from dwd_fact_order_info
    where dt='2022-09-20'
    group by user_id
) ,
tmp_payment as
(
    select
        user_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from dwd_fact_payment_info
    where dt='2022-09-20'
    group by user_id
)
insert overwrite table dws_user_action_daycount partition(dt='2022-09-20')
select
    user_actions.user_id,
    sum(user_actions.login_count),
    sum(user_actions.cart_count),
    sum(user_actions.cart_amount),
    sum(user_actions.order_count),
    sum(user_actions.order_amount),
    sum(user_actions.payment_count),
    sum(user_actions.payment_amount)
from
(
select
    user_id,
    login_count,
    0 cart_count,
    0 cart_amount,
    0 order_count,
    0 order_amount,
    0 payment_count,
    0 payment_amount
from tmp_login
union all
select
    user_id,
    0 login_count,
    cart_count,
    cart_amount,
    0 order_count,
    0 order_amount,
    0 payment_count,
    0 payment_amount
from tmp_cart
union all
select
    user_id,
    0 login_count,
    0 cart_count,
    0 cart_amount,
    order_count,
    order_amount,
    0 payment_count,
    0 payment_amount
from tmp_order
union all
select
    user_id,
    0 login_count,
    0 cart_count,
    0 cart_amount,
    0 order_count,
    0 order_amount,
    payment_count,
    payment_amount
from tmp_payment
) user_actions
group by user_id;

-- 会员 主题宽表
drop table if exists dwt_user_topic;
create external table dwt_user_topic
(
user_id string comment '用户 id',
login_date_first string comment '首次登录时间',
login_date_last string comment '末次登录时间',
login_count bigint comment '累积登录天数',
login_last_30d_count bigint comment '最近 30 日登录天数',
order_date_first string comment '首次下单时间',
order_date_last string comment '末次下单时间',
order_count bigint comment '累积下单次数',
order_amount decimal(16,2) comment '累积下单金额',
order_last_30d_count bigint comment '最近 30 日下单次数',
order_last_30d_amount bigint comment '最近 30 日下单金额',
payment_date_first string comment '首次支付时间',
payment_date_last string comment '末次支付时间',
payment_count decimal(16,2) comment '累积支付次数',
payment_amount decimal(16,2) comment '累积支付金额',
payment_last_30d_count decimal(16,2) comment '最近 30 日支付次数',
payment_last_30d_amount decimal(16,2) comment '最近 30 日支付金额'
)COMMENT '用户主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_user_topic/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'2022-09-20',old.login_date_first),
    if(new.login_count>0,'2022-09-20',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.order_date_first is null and new.order_count>0,'2022-09-20',old.order_date_first),
    if(new.order_count>0,'2022-09-20',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'2022-09-20',old.payment_date_first),
    if(new.payment_count>0,'2022-09-20',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from dwt_user_topic old
full outer join
(
select
    user_id,
    sum(if(dt='2022-09-20',login_count,0)) login_count,
    sum(if(dt='2022-09-20',order_count,0)) order_count,
    sum(if(dt='2022-09-20',order_amount,0)) order_amount,
    sum(if(dt='2022-09-20',payment_count,0)) payment_count,
    sum(if(dt='2022-09-20',payment_amount,0)) payment_amount,
    sum(if(login_count>0,1,0)) login_last_30d_count,
    sum(order_count) order_last_30d_count,
    sum(order_amount) order_last_30d_amount,
    sum(payment_count) payment_last_30d_count,
    sum(payment_amount) payment_last_30d_amount
from dws_user_action_daycount
where dt>=date_add('2022-09-20',-30)
group by user_id
)new
on old.user_id=new.user_id;

-- 每日商品行为
drop table if exists dws_sku_action_daycount;
create external table dws_sku_action_daycount
(
sku_id string comment 'sku_id',
order_count bigint comment '被下单次数',
order_num bigint comment '被下单件数',
order_amount decimal(16,2) comment '被下单金额',
payment_count bigint comment '被支付次数',
payment_num bigint comment '被支付件数',
payment_amount decimal(16,2) comment '被支付金额',
refund_count bigint comment '被退款次数',
refund_num bigint comment '被退款件数',
refund_amount decimal(16,2) comment '被退款金额',
cart_count bigint comment '被加入购物车次数',
cart_num bigint comment '被加入购物车件数',
favor_count bigint comment '被收藏次数',
appraise_good_count bigint comment '好评数',
appraise_mid_count bigint comment '中评数',
appraise_bad_count bigint comment '差评数',
appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sku_action_daycount/'
tblproperties ("parquet.compression"="lzo");

with
tmp_order as
(
select
sku_id,
count(*) order_count,
sum(sku_num) order_num,
sum(total_amount) order_amount
from dwd_fact_order_detail
where dt='2022-09-20'
group by sku_id
),
tmp_payment as
(
select
sku_id,
count(*) payment_count,
sum(sku_num) payment_num,
sum(total_amount) payment_amount
from dwd_fact_order_detail
where dt='2022-09-20' 
and order_id in
(
select
id
from dwd_fact_order_info
where (dt='2022-09-20' or dt=date_add('2022-09-20',-1)) and date_format(payment_time,'yyyy-MM-dd')='2022-09-20'
)
group by sku_id
),
tmp_refund as
(
select
sku_id,
count(*) refund_count,
sum(refund_num) refund_num,
sum(refund_amount) refund_amount
from dwd_fact_order_refund_info
where dt='2022-09-20'
group by sku_id
),
tmp_cart as
(
select
sku_id,
count(*) cart_count,
sum(sku_num) cart_num
from dwd_fact_cart_info
where dt='2022-09-20' and date_format(create_time,'yyyy-MM-dd')='2022-09-20'
group by sku_id
),
tmp_favor as
(
select
sku_id,
count(*) favor_count
from dwd_fact_favor_info
where dt='2022-09-20' and date_format(create_time,'yyyy-MM-dd')='2022-09-20'
group by sku_id
),
tmp_appraise as
(
select
sku_id,
sum(if(appraise='1201',1,0)) appraise_good_count,
sum(if(appraise='1202',1,0)) appraise_mid_count,
sum(if(appraise='1203',1,0)) appraise_bad_count,
sum(if(appraise='1204',1,0)) appraise_default_count
from dwd_fact_comment_info
where dt='2022-09-20'
group by sku_id
)
insert overwrite table dws_sku_action_daycount partition(dt='2022-09-20')
select
sku_id,
sum(order_count),
sum(order_num),
sum(order_amount),
sum(payment_count),
sum(payment_num),
sum(payment_amount),
sum(refund_count),
sum(refund_num),
sum(refund_amount),
sum(cart_count),
sum(cart_num),
sum(favor_count),
sum(appraise_good_count),
sum(appraise_mid_count),
sum(appraise_bad_count),
sum(appraise_default_count)
from
(
select
sku_id,
order_count,
order_num,
order_amount,
0 payment_count,
0 payment_num,
0 payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount,
0 cart_count,
0 cart_num,
0 favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_order
union all
select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
payment_count,
payment_num,
payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount,
0 cart_count,
0 cart_num,
0 favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_payment
union all
select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
0 payment_count,
0 payment_num,
0 payment_amount,
refund_count,
refund_num,
refund_amount,
0 cart_count,
0 cart_num,
0 favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_refund
union all
select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
0 payment_count,
0 payment_num,
0 payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount,
cart_count,
cart_num,
0 favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_cart
union all
select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
0 payment_count,
0 payment_num,
0 payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount,
0 cart_count,
0 cart_num,
favor_count,
0 appraise_good_count,
0 appraise_mid_count,
0 appraise_bad_count,
0 appraise_default_count
from tmp_favor
union all
select
sku_id,
0 order_count,
0 order_num,
0 order_amount,
0 payment_count,
0 payment_num,
0 payment_amount,
0 refund_count,
0 refund_num,
0 refund_amount,
0 cart_count,
0 cart_num,
0 favor_count,
appraise_good_count,
appraise_mid_count,
appraise_bad_count,
appraise_default_count
from tmp_appraise
)tmp
group by sku_id;

-- 商品主题宽表
drop table if exists dwt_sku_topic;
create external table dwt_sku_topic
(
sku_id string comment 'sku_id',
spu_id string comment 'spu_id',
order_last_30d_count bigint comment '最近 30 日被下单次数',
order_last_30d_num bigint comment '最近 30 日被下单件数',
order_last_30d_amount decimal(16,2) comment '最近 30 日被下单金额',
order_count bigint comment '累积被下单次数',
order_num bigint comment '累积被下单件数',
order_amount decimal(16,2) comment '累积被下单金额',
payment_last_30d_count bigint comment '最近 30 日被支付次数',
payment_last_30d_num bigint comment '最近 30 日被支付件数',
payment_last_30d_amount decimal(16,2) comment '最近 30 日被支付金额',
payment_count bigint comment '累积被支付次数',
payment_num bigint comment '累积被支付件数',
payment_amount decimal(16,2) comment '累积被支付金额',
refund_last_30d_count bigint comment '最近三十日退款次数',
refund_last_30d_num bigint comment '最近三十日退款件数',
refund_last_30d_amount decimal(10,2) comment '最近三十日退款金额',
refund_count bigint comment '累积退款次数',
refund_num bigint comment '累积退款件数',
refund_amount decimal(10,2) comment '累积退款金额',
cart_last_30d_count bigint comment '最近 30 日被加入购物车次数',
cart_last_30d_num bigint comment '最近 30 日被加入购物车件数',
cart_count bigint comment '累积被加入购物车次数',
cart_num bigint comment '累积被加入购物车件数',
favor_last_30d_count bigint comment '最近 30 日被收藏次数',
favor_count bigint comment '累积被收藏次数',
appraise_last_30d_good_count bigint comment '最近 30 日好评数',
appraise_last_30d_mid_count bigint comment '最近 30 日中评数',
appraise_last_30d_bad_count bigint comment '最近 30 日差评数',
appraise_last_30d_default_count bigint comment '最近 30 日默认评价数',
appraise_good_count bigint comment '累积好评数',
appraise_mid_count bigint comment '累积中评数',
appraise_bad_count bigint comment '累积差评数',
appraise_default_count bigint comment '累积默认评价数'
)COMMENT '商品主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_sku_topic/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dwt_sku_topic
select
nvl(new.sku_id,old.sku_id),
sku_info.spu_id,
nvl(new.order_count30,0),
nvl(new.order_num30,0),
nvl(new.order_amount30,0),
nvl(old.order_count,0) + nvl(new.order_count,0),
nvl(old.order_num,0) + nvl(new.order_num,0),
nvl(old.order_amount,0) + nvl(new.order_amount,0),
nvl(new.payment_count30,0),
nvl(new.payment_num30,0),
nvl(new.payment_amount30,0),
nvl(old.payment_count,0) + nvl(new.payment_count,0),
nvl(old.payment_num,0) + nvl(new.payment_count,0),
nvl(old.payment_amount,0) + nvl(new.payment_count,0),
nvl(new.refund_count30,0),
nvl(new.refund_num30,0),
nvl(new.refund_amount30,0),
nvl(old.refund_count,0) + nvl(new.refund_count,0),
nvl(old.refund_num,0) + nvl(new.refund_num,0),
nvl(old.refund_amount,0) + nvl(new.refund_amount,0),
nvl(new.cart_count30,0),
nvl(new.cart_num30,0),
nvl(old.cart_count,0) + nvl(new.cart_count,0),
nvl(old.cart_num,0) + nvl(new.cart_num,0),
nvl(new.favor_count30,0),
nvl(old.favor_count,0) + nvl(new.favor_count,0),
nvl(new.appraise_good_count30,0),
nvl(new.appraise_mid_count30,0),
nvl(new.appraise_bad_count30,0),
nvl(new.appraise_default_count30,0) ,
nvl(old.appraise_good_count,0) + nvl(new.appraise_good_count,0),
nvl(old.appraise_mid_count,0) + nvl(new.appraise_mid_count,0),
nvl(old.appraise_bad_count,0) + nvl(new.appraise_bad_count,0),
nvl(old.appraise_default_count,0) + nvl(new.appraise_default_count,0)
from
(
select
sku_id,
spu_id,
order_last_30d_count,
order_last_30d_num,
order_last_30d_amount,
order_count,
order_num,
order_amount ,
payment_last_30d_count,
payment_last_30d_num,
payment_last_30d_amount,
payment_count,
payment_num,
payment_amount,
refund_last_30d_count,
refund_last_30d_num,
refund_last_30d_amount,
refund_count,
refund_num,
refund_amount,
cart_last_30d_count,
cart_last_30d_num,
cart_count,
cart_num,
favor_last_30d_count,
favor_count,
appraise_last_30d_good_count,
appraise_last_30d_mid_count,
appraise_last_30d_bad_count,
appraise_last_30d_default_count,
appraise_good_count,
appraise_mid_count,
appraise_bad_count,
appraise_default_count
from dwt_sku_topic
)old
full outer join
(
select
sku_id,
sum(if(dt='2022-09-20', order_count,0 )) order_count,
sum(if(dt='2022-09-20',order_num ,0 )) order_num,
sum(if(dt='2022-09-20',order_amount,0 )) order_amount ,
sum(if(dt='2022-09-20',payment_count,0 )) payment_count,
sum(if(dt='2022-09-20',payment_num,0 )) payment_num,
sum(if(dt='2022-09-20',payment_amount,0 )) payment_amount,
sum(if(dt='2022-09-20',refund_count,0 )) refund_count,
sum(if(dt='2022-09-20',refund_num,0 )) refund_num,
sum(if(dt='2022-09-20',refund_amount,0 )) refund_amount,
sum(if(dt='2022-09-20',cart_count,0 )) cart_count,
sum(if(dt='2022-09-20',cart_num,0 )) cart_num,
sum(if(dt='2022-09-20',favor_count,0 )) favor_count,
sum(if(dt='2022-09-20',appraise_good_count,0 )) appraise_good_count,
sum(if(dt='2022-09-20',appraise_mid_count,0 ) ) appraise_mid_count ,
sum(if(dt='2022-09-20',appraise_bad_count,0 )) appraise_bad_count,
sum(if(dt='2022-09-20',appraise_default_count,0 )) appraise_default_count,
sum(order_count) order_count30 ,
sum(order_num) order_num30,
sum(order_amount) order_amount30,
sum(payment_count) payment_count30,
sum(payment_num) payment_num30,
sum(payment_amount) payment_amount30,
sum(refund_count) refund_count30,
sum(refund_num) refund_num30,
sum(refund_amount) refund_amount30,
sum(cart_count) cart_count30,
sum(cart_num) cart_num30,
sum(favor_count) favor_count30,
sum(appraise_good_count) appraise_good_count30,
sum(appraise_mid_count) appraise_mid_count30,
sum(appraise_bad_count) appraise_bad_count30,
sum(appraise_default_count) appraise_default_count30
from dws_sku_action_daycount
where dt >= date_add ('2022-09-20', -30)
group by sku_id
)new
on new.sku_id = old.sku_id
left join
(select * from dwd_dim_sku_info where dt='2022-09-20' ) sku_info
on nvl(new.sku_id,old.sku_id)= sku_info.id;


-- 每日优惠券统计 （预留）
drop table if exists dws_coupon_use_daycount;
create external table dws_coupon_use_daycount
(
`coupon_id` string COMMENT '优惠券 ID',
`coupon_name` string COMMENT '购物券名称',
`coupon_type` string COMMENT '购物券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
`condition_amount` string COMMENT '满额数',
`condition_num` string COMMENT '满件数',
`activity_id` string COMMENT '活动编号',
`benefit_amount` string COMMENT '减金额',
`benefit_discount` string COMMENT '折扣',
`create_time` string COMMENT '创建时间',
`range_type` string COMMENT '范围类型 1、商品 2、品类 3、品牌',
`spu_id` string COMMENT '商品 id',
`tm_id` string COMMENT '品牌 id',
`category3_id` string COMMENT '品类 id',
`limit_num` string COMMENT '最多领用次数',
`get_count` bigint COMMENT '领用次数',
`using_count` bigint COMMENT '使用(下单)次数',
`used_count` bigint COMMENT '使用(支付)次数'
) COMMENT '每日优惠券统计'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_coupon_use_daycount/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dws_coupon_use_daycount partition(dt='2022-09-20')
select
cu.coupon_id,
ci.coupon_name,
ci.coupon_type,
ci.condition_amount,
ci.condition_num,
ci.activity_id,
ci.benefit_amount,
ci.benefit_discount,
ci.create_time,
ci.range_type,
ci.spu_id,
ci.tm_id,
ci.category3_id,
ci.limit_num,
cu.get_count,
cu.using_count,
cu.used_count
from
(
select
coupon_id,
sum(if(date_format(get_time,'yyyy-MM-dd')='2022-09-20',1,0))
get_count,
sum(if(date_format(using_time,'yyyy-MM-dd')='2022-09-20',1,0))
using_count,
sum(if(date_format(used_time,'yyyy-MM-dd')='2022-09-20',1,0))
used_count
from dwd_fact_coupon_use
where dt='2022-09-20'
group by coupon_id
)cu
left join
(
select
*
from dwd_dim_coupon_info
where dt='2022-09-20'
)ci on cu.coupon_id=ci.id;

-- 优惠 券主题宽表 （预留）
drop table if exists dwt_coupon_topic;
create external table dwt_coupon_topic
(
`coupon_id` string COMMENT '优惠券 ID',
`get_day_count` bigint COMMENT '当日领用次数',
`using_day_count` bigint COMMENT '当日使用(下单)次数',
`used_day_count` bigint COMMENT '当日使用(支付)次数',
`get_count` bigint COMMENT '累积领用次数',
`using_count` bigint COMMENT '累积使用(下单)次数',
`used_count` bigint COMMENT '累积使用(支付)次数'
)COMMENT '购物券主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_coupon_topic/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dwt_coupon_topic
select
nvl(new.coupon_id,old.coupon_id),
nvl(new.get_count,0),
nvl(new.using_count,0),
nvl(new.used_count,0),
nvl(old.get_count,0)+nvl(new.get_count,0),
nvl(old.using_count,0)+nvl(new.using_count,0),
nvl(old.used_count,0)+nvl(new.used_count,0)
from
(
select
*
from dwt_coupon_topic
)old
full outer join
(
select
coupon_id,
get_count,
using_count,
used_count
from dws_coupon_use_daycount
where dt='2022-09-20'
)new
on old.coupon_id=new.coupon_id;

-- 每日活动统计 （预留）
drop table if exists dws_activity_info_daycount;
create external table dws_activity_info_daycount(
`id` string COMMENT '编号',
`activity_name` string COMMENT '活动名称',
`activity_type` string COMMENT '活动类型',
`start_time` string COMMENT '开始时间',
`end_time` string COMMENT '结束时间',
`create_time` string COMMENT '创建时间',
`order_count` bigint COMMENT '下单次数',
`payment_count` bigint COMMENT '支付次数'
) COMMENT '购物车信息表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dws/dws_activity_info_daycount/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dws_activity_info_daycount partition(dt='2022-09-20')
select
oi.activity_id,
ai.activity_name,
ai.activity_type,
ai.start_time,
ai.end_time,
ai.create_time,
oi.order_count,
oi.payment_count
from
(
select
activity_id,
sum(if(date_format(create_time,'yyyy-MM-dd')='2022-09-20',1,0)) order_count,
sum(if(date_format(payment_time,'yyyy-MM-dd')='2022-09-20',1,0)) payment_count
from dwd_fact_order_info
where (dt='2022-09-20' or dt=date_add('2022-09-20',-1))
and activity_id is not null
group by activity_id
)oi
join
(
select
*
from dwd_dim_activity_info
where dt='2022-09-20'
)ai
on oi.activity_id=ai.id;

-- 活动主题宽表 （预留）
drop table if exists dwt_activity_topic;
create external table dwt_activity_topic(
`id` string COMMENT '活动 id',
`activity_name` string COMMENT '活动名称',
`order_day_count` bigint COMMENT '当日日下单次数',
`payment_day_count` bigint COMMENT '当日支付次数',
`order_count` bigint COMMENT '累积下单次数',
`payment_count` bigint COMMENT '累积支付次数'
) COMMENT '活动主题宽表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dwt/dwt_activity_topic/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dwt_activity_topic
select
nvl(new.id,old.id),
nvl(new.activity_name,old.activity_name),
nvl(new.order_count,0),
nvl(new.payment_count,0),
nvl(old.order_count,0)+nvl(new.order_count,0),
nvl(old.payment_count,0)+nvl(new.payment_count,0)
from
(
select
*
from dwt_activity_topic
)old
full outer join
(
select
id,
activity_name,
order_count,
payment_count
from dws_activity_info_daycount
where dt='2022-09-20'
)new
on old.id=new.id;

-- 每日购买行为
drop table if exists dws_sale_detail_daycount;
create external table dws_sale_detail_daycount
(
user_id string comment '用户 id',
sku_id string comment '商品 id',
user_gender string comment '用户性别',
user_age string comment '用户年龄',
user_level string comment '用户等级',
order_price decimal(10,2) comment '商品价格',
sku_name string comment '商品名称',
sku_tm_id string comment '品牌 id',
sku_category3_id string comment '商品三级品类 id',
sku_category2_id string comment '商品二级品类 id',
sku_category1_id string comment '商品一级品类 id',
sku_category3_name string comment '商品三级品类名称',
sku_category2_name string comment '商品二级品类名称',
sku_category1_name string comment '商品一级品类名称',
spu_id string comment '商品 spu',
sku_num int comment '购买个数',
order_count bigint comment '当日下单单数',
order_amount decimal(16,2) comment '当日下单金额'
) COMMENT '每日购买行为'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sale_detail_daycount/'
tblproperties ("parquet.compression"="lzo");

insert overwrite table dws_sale_detail_daycount partition(dt='2022-09-20')
select
op.user_id,
op.sku_id,
ui.gender,
months_between('2022-09-20', ui.birthday)/12 age,
ui.user_level,
si.price,
si.sku_name,
si.tm_id,
si.category3_id,
si.category2_id,
si.category1_id,
si.category3_name,
si.category2_name,
si.category1_name,
si.spu_id,
op.sku_num,
op.order_count,
op.order_amount
from
(
select
user_id,
sku_id,
sum(sku_num) sku_num,
count(*) order_count,
sum(total_amount) order_amount
from dwd_fact_order_detail
where dt='2022-09-20'
group by user_id, sku_id
)op
join
(
select
*
from dwd_dim_user_info_his
where end_date='9999-99-99'
)ui on op.user_id = ui.id
join
(
select
*
from dwd_dim_sku_info
where dt='2022-09-20'
)si on op.sku_id = si.id;