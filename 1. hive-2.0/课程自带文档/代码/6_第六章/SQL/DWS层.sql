DWS层

1. 每日设备行为表
建表语句
----------------------------------------------------------
drop table if exists dws_uv_detail_daycount;
create external table dws_uv_detail_daycount
(
    `mid_id` string COMMENT '设备唯一标识',
    `user_id` string COMMENT '用户标识',
    `version_code` string COMMENT '程序版本号', 
    `version_name` string COMMENT '程序版本名', 
    `lang` string COMMENT '系统语言', 
    `source` string COMMENT '渠道号', 
    `os` string COMMENT 'Android版本', 
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
) COMMENT'每日设备行为表'
partitioned by(dt string)
stored as parquet
location '/warehouse/gmall/dws/dws_uv_detail_daycount';
----------------------------------------------------------

数据加载
----------------------------------------------------------
insert overwrite table dws_uv_detail_daycount partition(dt='2020-03-10')
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
where dt='2020-03-10'
group by mid_id;
----------------------------------------------------------

2. 业务数据之每日会员行为
建表语句
----------------------------------------------------------
drop table if exists dws_user_action_daycount;
create external table dws_user_action_daycount
(   
    user_id string comment '用户 id',
    login_count bigint comment '登录次数',
    cart_count bigint comment '加入购物车次数',
    cart_amount double comment '加入购物车金额',
    order_count bigint comment '下单次数',
    order_amount    decimal(16,2)  comment '下单金额',
    payment_count   bigint      comment '支付次数',
    payment_amount  decimal(16,2) comment '支付金额',
    order_stats array<struct<sku_id:string,sku_num:bigint,order_count:bigint,
order_amount:decimal(20,2)>> comment '下单明细统计'
) COMMENT '每日会员行为表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_user_action_daycount/';
----------------------------------------------------------

数据加载
----------------------------------------------------------
with
tmp_login as --当日登录统计，统计每个会员当日登录次数
(
    select
        user_id,
        count(*) login_count --登录次数
    from dwd_start_log
    where dt='2020-03-10'
    and user_id is not null
    group by user_id
),
tmp_cart as --当日加入购物车统计，统计每个会员当日加入购物车情况
(
    select
        user_id,
        count(*) cart_count, --加入购物车次数
        sum(cart_price*sku_num) cart_amount --加入购物车金额
    from dwd_fact_cart_info
    where dt='2020-03-10'
and user_id is not null
and date_format(create_time,'yyyy-MM-dd')='2020-03-10'
    group by user_id
),
tmp_order as --当日下单统计，统计每个会员当日下单情况
(
    select
        user_id,
        count(*) order_count, --下单次数
        sum(final_total_amount) order_amount --下单金额
    from dwd_fact_order_info
    where dt='2020-03-10'
    group by user_id
) ,
tmp_payment as --当日支付统计，统计每个会员当日支付情况
(
    select
        user_id,
        count(*) payment_count, --支付次数
        sum(payment_amount) payment_amount --支付金额
    from dwd_fact_payment_info
    where dt='2020-03-10'
    group by user_id
),
tmp_order_detail as --当日订单详情统计，统计每个会员当日下单详细信息
(
    select
        user_id,
--结构为struct数组，每个数组元素对应该会员当日下单的一个商品，包含sku_id,sku_num（下单个
--数），order_count（下单次数），order_amount（下单金额）
        collect_set(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_
count',order_count,'order_amount',order_amount)) order_stats 
    from
    (
        select
            user_id,
            sku_id,
            sum(sku_num) sku_num,
            count(*) order_count,
            cast(sum(total_amount) as decimal(20,2)) order_amount
        from dwd_fact_order_detail
        where dt='2020-03-10'
        group by user_id,sku_id
    )tmp
    group by user_id
)

insert overwrite table dws_user_action_daycount partition(dt='2020-03-10')
select
    coalesce(tmp_login.user_id,tmp_cart.user_id,tmp_order.user_id,tmp_payment.
user_id,tmp_order_detail.user_id),
    login_count,
    nvl(cart_count,0),
    nvl(cart_amount,0),
    nvl(order_count,0),
    nvl(order_amount,0),
    nvl(payment_count,0),
    nvl(payment_amount,0),
    order_stats
from tmp_login
full outer join tmp_cart on tmp_login.user_id=tmp_cart.user_id
full outer join tmp_order on tmp_login.user_id=tmp_order.user_id
full outer join tmp_payment on tmp_login.user_id=tmp_payment.user_id
full outer join tmp_order_detail on tmp_login.user_id=tmp_order_detail.user_id
;
----------------------------------------------------------

3. 业务数据之每日商品行为
建表语句
----------------------------------------------------------
drop table if exists dws_sku_action_daycount;
create external table dws_sku_action_daycount 
(   
    sku_id string comment 'sku_id',
    order_count bigint comment '被下单次数',
    order_num bigint comment '被下单件数',
    order_amount decimal(16,2) comment '被下单金额',
    payment_count bigint  comment '被支付次数',
    payment_num bigint comment '被支付件数',
    payment_amount decimal(16,2) comment '被支付金额',
    refund_count bigint  comment '被退款次数',
    refund_num bigint comment '被退款件数',
    refund_amount  decimal(16,2) comment '被退款金额',
    cart_count bigint comment '被加入购物车次数',
    cart_num bigint comment '被加入购物车件数',
    favor_count bigint comment '被收藏次数',
    appraise_good_count bigint comment '好评数',
    appraise_mid_count bigint comment '中评数',
    appraise_bad_count bigint comment '差评数',
    appraise_default_count bigint comment '默认评价数'
) COMMENT '每日商品行为表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_sku_action_daycount/';
----------------------------------------------------------

数据加载
----------------------------------------------------------
with 
tmp_order as --下单情况统计，统计每款商品（SKU）当日被下单的情况
(
    select
        sku_id,
        count(*) order_count, --被下单次数
        sum(sku_num) order_num, --被下单个数
        sum(total_amount) order_amount --被下单金额
    from dwd_fact_order_detail
    where dt='2020-03-10'
    group by sku_id
),
tmp_payment as --支付统计，统计每款商品（SKU）当日被支付的情况
(
    select
        sku_id,
        count(*) payment_count, --被支付次数
        sum(sku_num) payment_num, --被支付个数
        sum(total_amount) payment_amount --被支付金额
    from dwd_fact_order_detail
    where dt='2020-03-10'
    and order_id in
    (
        select
            id
        from dwd_fact_order_info
        where (dt='2020-03-10'
        or dt=date_add('2020-03-10',-1))
        and date_format(payment_time,'yyyy-MM-dd')='2020-03-10'
    )
    group by sku_id
),
tmp_refund as --退款统计
(
    select
        sku_id,
        count(*) refund_count, --被退款次数
        sum(refund_num) refund_num, --被退款件数
        sum(refund_amount) refund_amount --被退款金额
    from dwd_fact_order_refund_info
    where dt='2020-03-10'
    group by sku_id
),
tmp_cart as --加入购物车统计
(
    select
        sku_id,
        count(*) cart_count, --被加入购物车次数
        sum(sku_num) cart_num --被加入购物车个数
    from dwd_fact_cart_info
    where dt='2020-03-10'
    and date_format(create_time,'yyyy-MM-dd')='2020-03-10'
    group by sku_id
),
tmp_favor as --收藏统计 
(
    select
        sku_id,
        count(*) favor_count --被收藏次数
    from dwd_fact_favor_info
    where dt='2020-03-10'
    and date_format(create_time,'yyyy-MM-dd')='2020-03-10'
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
where dt='2020-03-10'
group by sku_id
)

insert overwrite table dws_sku_action_daycount partition(dt='2020-03-10')
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
----------------------------------------------------------

4. 业务数据之每日优惠券统计表
建表语句
----------------------------------------------------------
drop table if exists dws_coupon_use_daycount;
create external table dws_coupon_use_daycount
(   
    `coupon_id` string  COMMENT '优惠券id',
    `coupon_name` string COMMENT '优惠券名称',
    `coupon_type` string COMMENT '优惠券类型 1 现金券 2 折扣券 3 满减券 4 满件打折券',
    `condition_amount` string COMMENT '满额数',
    `condition_num` string COMMENT '满件数',
    `activity_id` string COMMENT '活动编号',
    `benefit_amount` string COMMENT '满减金额',
    `benefit_discount` string COMMENT '折扣',
    `create_time` string COMMENT '创建时间',
    `range_type` string COMMENT '范围类型 1商品 2品类 3品牌',
    `spu_id` string COMMENT '商品id',
    `tm_id` string COMMENT '品牌id',
    `category3_id` string COMMENT '品类id',
    `limit_num` string COMMENT '最多领用次数',
    `get_count` bigint COMMENT '领用次数',
    `using_count` bigint COMMENT '使用(下单)次数',
    `used_count` bigint COMMENT '使用(支付)次数'
) COMMENT '每日优惠券统计表'
PARTITIONED BY (`dt` string)
stored as parquet
location '/warehouse/gmall/dws/dws_coupon_use_daycount/';
----------------------------------------------------------

数据加载
----------------------------------------------------------
insert overwrite table dws_coupon_use_daycount partition(dt='2020-03-10')
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
        sum(if(date_format(get_time,'yyyy-MM-dd')='2020-03-10',1,0)) get_count,
        sum(if(date_format(using_time,'yyyy-MM-dd')='2020-03-10',1,0)) using_
count,
        sum(if(date_format(used_time,'yyyy-MM-dd')='2020-03-10',1,0)) used_
count
    from dwd_fact_coupon_use
    where dt='2020-03-10'
    group by coupon_id
)cu
left join
(
    select
        *
    from dwd_dim_coupon_info
    where dt='2020-03-10'
)ci on cu.coupon_id=ci.id;
----------------------------------------------------------


5. 业务数据之每日活动统计表
建表语句
----------------------------------------------------------
drop table if exists dws_activity_info_daycount;
create external table dws_activity_info_daycount(
    `id` string COMMENT '编号',
    `activity_name` string  COMMENT '活动名称',
    `activity_type` string  COMMENT '活动类型',
    `start_time` string  COMMENT '开始时间',
    `end_time` string  COMMENT '结束时间',
    `create_time` string  COMMENT '创建时间',
    `order_count` bigint COMMENT '下单次数',
    `payment_count` bigint COMMENT '支付次数'
) COMMENT '每日活动统计表'
PARTITIONED BY (`dt` string)
row format delimited fields terminated by '\t'
location '/warehouse/gmall/dws/dws_activity_info_daycount/';
----------------------------------------------------------

数据加载
----------------------------------------------------------
insert overwrite table dws_activity_info_daycount partition(dt='2020-03-10')
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
        sum(if(date_format(create_time,'yyyy-MM-dd')='2020-03-10',1,0)) order_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='2020-03-10',1,0)) payment_count
    from dwd_fact_order_info
    where (dt='2020-03-10' or dt=date_add('2020-03-10',-1))
    and activity_id is not null
    group by activity_id
)oi
join
(
    select
        *
    from dwd_dim_activity_info
    where dt='2020-03-10'
)ai
on oi.activity_id=ai.id;
----------------------------------------------------------

DWS层数据导入脚本 dwd_to_dws.sh
----------------------------------------------------------
#!/bin/bash

APP=gmall
hive=/opt/module/hive/bin/hive

# 如果输入了日期参数，则取输入参数作为日期值；如果没有输入日期参数，则取当前时间的前一天作为日期值
if [ -n "$1" ] ;then
    do_date=$1
else
    do_date=`date -d "-1 day" +%F`
fi

sql="
insert overwrite table ${APP}.dws_uv_detail_daycount partition(dt='$do_date')
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
from ${APP}.dwd_start_log
where dt='$do_date'
group by mid_id;


with
tmp_login as
(
    select
        user_id,
        count(*) login_count
    from ${APP}.dwd_start_log
    where dt='$do_date'
    and user_id is not null
    group by user_id
),
tmp_cart as
(
    select
        user_id,
        count(*) cart_count,
        sum(cart_price*sku_num) cart_amount
    from ${APP}.dwd_fact_cart_info
    where dt='$do_date'
    and user_id is not null
    and date_format(create_time,'yyyy-MM-dd')='$do_date'
    group by user_id
),
tmp_order as
(
    select
        user_id,
        count(*) order_count,
        sum(final_total_amount) order_amount
    from ${APP}.dwd_fact_order_info
    where dt='$do_date'
    group by user_id
) ,
tmp_payment as
(
    select
        user_id,
        count(*) payment_count,
        sum(payment_amount) payment_amount
    from ${APP}.dwd_fact_payment_info
    where dt='$do_date'
    group by user_id
),
tmp_order_detail as
(
    select
        user_id,
        collect_set(named_struct('sku_id',sku_id,'sku_num',sku_num,'order_count',
order_count,'order_amount',order_amount)) order_stats
    from
    (
        select
            user_id,
            sku_id,
            sum(sku_num) sku_num,
            count(*) order_count,
            cast(sum(total_amount) as decimal(20,2)) order_amount
        from ${APP}.dwd_fact_order_detail
        where dt='$do_date'
        group by user_id,sku_id
    )tmp
    group by user_id
)

insert overwrite table ${APP}.dws_user_action_daycount partition(dt='$do_date')
select
    coalesce(tmp_login.user_id,tmp_cart.user_id,tmp_order.user_id,tmp_payment.
user_id,tmp_order_detail.user_id),
    login_count,
    nvl(cart_count,0),
    nvl(cart_amount,0),
    nvl(order_count,0),
    nvl(order_amount,0),
    nvl(payment_count,0),
    nvl(payment_amount,0),
    order_stats
from tmp_login
full outer join tmp_cart on tmp_login.user_id=tmp_cart.user_id
full outer join tmp_order on tmp_login.user_id=tmp_order.user_id
full outer join tmp_payment on tmp_login.user_id=tmp_payment.user_id
full outer join tmp_order_detail on tmp_login.user_id=tmp_order_detail.user_id;

with 
tmp_order as
(
    select
        sku_id,
        count(*) order_count,
        sum(sku_num) order_num,
        sum(total_amount) order_amount
    from ${APP}.dwd_fact_order_detail
    where dt='$do_date'
    group by sku_id
),
tmp_payment as
(
    select
        sku_id,
        count(*) payment_count,
        sum(sku_num) payment_num,
        sum(total_amount) payment_amount
    from ${APP}.dwd_fact_order_detail
    where dt='$do_date'
    and order_id in
    (
        select
            id
        from ${APP}.dwd_fact_order_info
        where (dt='$do_date' or dt=date_add('$do_date',-1))
        and date_format(payment_time,'yyyy-MM-dd')='$do_date'
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
    from ${APP}.dwd_fact_order_refund_info
    where dt='$do_date'
    group by sku_id
),
tmp_cart as
(
    select
        sku_id,
        count(*) cart_count,
        sum(sku_num) cart_num
    from ${APP}.dwd_fact_cart_info
    where dt='$do_date'
    and date_format(create_time,'yyyy-MM-dd')='$do_date'
    group by sku_id
),
tmp_favor as
(
    select
        sku_id,
        count(*) favor_count
    from ${APP}.dwd_fact_favor_info
    where dt='$do_date'
    and date_format(create_time,'yyyy-MM-dd')='$do_date'
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
    from ${APP}.dwd_fact_comment_info
    where dt='$do_date'
    group by sku_id
)

insert overwrite table ${APP}.dws_sku_action_daycount partition(dt='$do_date')
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


insert overwrite table ${APP}.dws_coupon_use_daycount partition(dt='$do_date')
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
        sum(if(date_format(get_time,'yyyy-MM-dd')='$do_date',1,0)) get_count,
        sum(if(date_format(using_time,'yyyy-MM-dd')='$do_date',1,0)) using_count,
        sum(if(date_format(used_time,'yyyy-MM-dd')='$do_date',1,0)) used_count
    from ${APP}.dwd_fact_coupon_use
    where dt='$do_date'
    group by coupon_id
)cu
left join
(
    select
        *
    from ${APP}.dwd_dim_coupon_info
    where dt='$do_date'
)ci on cu.coupon_id=ci.id;

insert overwrite table ${APP}.dws_activity_info_daycount partition(dt='$do_date')
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
        sum(if(date_format(create_time,'yyyy-MM-dd')='$do_date',1,0)) order_count,
        sum(if(date_format(payment_time,'yyyy-MM-dd')='$do_date',1,0)) payment_count
    from ${APP}.dwd_fact_order_info
    where (dt='$do_date' or dt=date_add('$do_date',-1))
    and activity_id is not null
    group by activity_id
)oi
join
(
    select
        *
    from ${APP}.dwd_dim_activity_info
    where dt='$do_date'
)ai
on oi.activity_id=ai.id;
"
$hive -e "$sql"
----------------------------------------------------------