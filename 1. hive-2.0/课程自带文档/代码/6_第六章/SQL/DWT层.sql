DWT层

1. 设备主题宽表
建表语句
---------------------------------------------------------------
drop table if exists dwt_uv_topic;
create external table dwt_uv_topic
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
    `login_date_first` string  comment '首次活跃时间',
    `login_date_last` string  comment '末次活跃时间',
    `login_day_count` bigint comment '当日活跃次数',
    `login_count` bigint comment '累计活跃天数'
) COMMENT'设备主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_uv_topic';
---------------------------------------------------------------

数据加载
---------------------------------------------------------------
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
    if(old.mid_id is null,'2020-03-10',old.login_date_first),
    if(new.mid_id is not null,'2020-03-10',old.login_date_last),
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
    where dt='2020-03-10'
)new
on old.mid_id=new.mid_id;
---------------------------------------------------------------




2. 会员主题宽表
建表语句
---------------------------------------------------------------
drop table if exists dwt_user_topic;
create external table dwt_user_topic
(
    user_id string  comment '用户id',
    login_date_first string  comment '首次登录时间',
    login_date_last string  comment '末次登录时间',
    login_count bigint comment '累计登录天数',
    login_last_30d_count bigint comment '最近30日登录天数',
    order_date_first string  comment '首次下单时间',
    order_date_last string  comment '末次下单时间',
    order_count bigint comment '累计下单次数',
    order_amount decimal(16,2) comment '累计下单金额',
    order_last_30d_count bigint comment '最近30日下单次数',
    order_last_30d_amount bigint comment '最近30日下单金额',
    payment_date_first string  comment '首次支付时间',
    payment_date_last string  comment '末次支付时间',
    payment_count decimal(16,2) comment '累计支付次数',
    payment_amount decimal(16,2) comment '累计支付金额',
    payment_last_30d_count decimal(16,2) comment '最近30日支付次数',
    payment_last_30d_amount decimal(16,2) comment '最近30日支付金额'
 )COMMENT '会员主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_user_topic/';
---------------------------------------------------------------

数据加载
---------------------------------------------------------------
insert overwrite table dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'2020-03-10',old.
login_date_first),
    if(new.login_count>0,'2020-03-10',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.order_date_first is null and new.order_count>0,'2020-03-10',old.
order_date_first),
    if(new.order_count>0,'2020-03-10',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'2020-03-10',old.
payment_date_first),
    if(new.payment_count>0,'2020-03-10',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from
dwt_user_topic old
full outer join
(
    select
        user_id,
        sum(if(dt='2020-03-10',login_count,0)) login_count,
        sum(if(dt='2020-03-10',order_count,0)) order_count,
        sum(if(dt='2020-03-10',order_amount,0)) order_amount,
        sum(if(dt='2020-03-10',payment_count,0)) payment_count,
        sum(if(dt='2020-03-10',payment_amount,0)) payment_amount,
        sum(if(login_count>0,1,0)) login_last_30d_count,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from dws_user_action_daycount
    where dt>=date_add( '2020-03-10',-30)
    group by user_id
)new
on old.user_id=new.user_id;
---------------------------------------------------------------



3. 商品主题宽表
建表语句
---------------------------------------------------------------
drop table if exists dwt_sku_topic;
create external table dwt_sku_topic
(
    sku_id string comment 'sku_id',
    spu_id string comment 'spu_id',
    order_last_30d_count bigint comment '最近30日被下单次数',
    order_last_30d_num bigint comment '最近30日被下单件数',
    order_last_30d_amount decimal(16,2)  comment '最近30日被下单金额',
    order_count bigint comment '累计被下单次数',
    order_num bigint comment '累计被下单件数',
    order_amount decimal(16,2) comment '累计被下单金额',
    payment_last_30d_count   bigint  comment '最近30日被支付次数',
    payment_last_30d_num bigint comment '最近30日被支付件数',
    payment_last_30d_amount  decimal(16,2) comment '最近30日被支付金额',
    payment_count   bigint  comment '累计被支付次数',
    payment_num bigint comment '累计被支付件数',
    payment_amount  decimal(16,2) comment '累计被支付金额',
    refund_last_30d_count bigint comment '最近30日退款次数',
    refund_last_30d_num bigint comment '最近30日退款件数',
    refund_last_30d_amount decimal(10,2) comment '最近30日退款金额',
    refund_count bigint comment '累计退款次数',
    refund_num bigint comment '累计退款件数',
    refund_amount decimal(10,2) comment '累计退款金额',
    cart_last_30d_count bigint comment '最近30日被加入购物车次数',
    cart_last_30d_num bigint comment '最近30日被加入购物车件数',
    cart_count bigint comment '累计被加入购物车次数',
    cart_num bigint comment '累计被加入购物车件数',
    favor_last_30d_count bigint comment '最近30日被收藏次数',
    favor_count bigint comment '累计被收藏次数',
    appraise_last_30d_good_count bigint comment '最近30日好评数',
    appraise_last_30d_mid_count bigint comment '最近30日中评数',
    appraise_last_30d_bad_count bigint comment '最近30日差评数',
    appraise_last_30d_default_count bigint comment '最近30日默认评价数',
    appraise_good_count bigint comment '累计好评数',
    appraise_mid_count bigint comment '累计中评数',
    appraise_bad_count bigint comment '累计差评数',
    appraise_default_count bigint comment '累计默认评价数'
 )COMMENT '商品主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_sku_topic/';
---------------------------------------------------------------

数据加载
---------------------------------------------------------------
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
    nvl(new.appraise_default_count30,0)  ,
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
        order_amount  ,
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
        sum(if(dt='2020-03-10', order_count,0 )) order_count,
        sum(if(dt='2020-03-10',order_num ,0 ))  order_num, 
        sum(if(dt='2020-03-10',order_amount,0 )) order_amount ,
        sum(if(dt='2020-03-10',payment_count,0 )) payment_count,
        sum(if(dt='2020-03-10',payment_num,0 )) payment_num,
        sum(if(dt='2020-03-10',payment_amount,0 )) payment_amount,
        sum(if(dt='2020-03-10',refund_count,0 )) refund_count,
        sum(if(dt='2020-03-10',refund_num,0 )) refund_num,
        sum(if(dt='2020-03-10',refund_amount,0 )) refund_amount,  
        sum(if(dt='2020-03-10',cart_count,0 )) cart_count,
        sum(if(dt='2020-03-10',cart_num,0 )) cart_num,
        sum(if(dt='2020-03-10',favor_count,0 )) favor_count,
        sum(if(dt='2020-03-10',appraise_good_count,0 )) appraise_good_count,  
        sum(if(dt='2020-03-10',appraise_mid_count,0 ) ) appraise_mid_count ,
        sum(if(dt='2020-03-10',appraise_bad_count,0 )) appraise_bad_count,  
        sum(if(dt='2020-03-10',appraise_default_count,0 )) appraise_default_
count,
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
    where dt >= date_add ('2020-03-10', -30)
    group by sku_id    
)new 
on new.sku_id = old.sku_id
left join 
(select * from dwd_dim_sku_info where dt='2020-03-10') sku_info
on nvl(new.sku_id,old.sku_id)= sku_info.id;
---------------------------------------------------------------


4. 优惠券主题宽表
建表语句
---------------------------------------------------------------
drop table if exists dwt_coupon_topic;
create external table dwt_coupon_topic
(
    `coupon_id` string  COMMENT '优惠券id',
    `get_day_count` bigint COMMENT '当日领用次数',
    `using_day_count` bigint COMMENT '当日使用(下单)次数',
    `used_day_count` bigint COMMENT '当日使用(支付)次数',
    `get_count` bigint COMMENT '累积计用次数',
    `using_count` bigint COMMENT '累计使用(下单)次数',
    `used_count` bigint COMMENT '累计使用(支付)次数'
)COMMENT '优惠券主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_coupon_topic/';
---------------------------------------------------------------

数据加载
---------------------------------------------------------------
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
    where dt='2020-03-10'
)new
on old.coupon_id=new.coupon_id;
---------------------------------------------------------------

4. 活动主题宽表
建表语句
---------------------------------------------------------------
drop table if exists dwt_activity_topic;
create external table dwt_activity_topic(
    `id` string COMMENT '活动id',
    `activity_name` string  COMMENT '活动名称',
    `order_day_count` bigint COMMENT '当日下单次数',
    `payment_day_count` bigint COMMENT '当日支付次数',
    `order_count` bigint COMMENT '累计下单次数',
    `payment_count` bigint COMMENT '累计支付次数'
) COMMENT '活动主题宽表'
stored as parquet
location '/warehouse/gmall/dwt/dwt_activity_topic/';
---------------------------------------------------------------

数据加载
---------------------------------------------------------------
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
    where dt='2020-03-10'
)new
on old.id=new.id;
---------------------------------------------------------------

DWT层数据导入脚本 dws_to_dwt.sh
---------------------------------------------------------------
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
insert overwrite table ${APP}.dwt_uv_topic
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
    nvl(old.login_date_first,'$do_date'),
    if(new.login_count>0,'$do_date',old.login_date_last),
    nvl(new.login_count,0),
    nvl(new.login_count,0)+nvl(old.login_count,0)
from
(
    select
        *
    from ${APP}.dwt_uv_topic
)old
full outer join
(
    select
        *
    from ${APP}.dws_uv_detail_daycount
    where dt='$do_date'
)new
on old.mid_id=new.mid_id;

insert overwrite table ${APP}.dwt_user_topic
select
    nvl(new.user_id,old.user_id),
    if(old.login_date_first is null and new.login_count>0,'$do_date',old.login_
date_first),
    if(new.login_count>0,'$do_date',old.login_date_last),
    nvl(old.login_count,0)+if(new.login_count>0,1,0),
    nvl(new.login_last_30d_count,0),
    if(old.order_date_first is null and new.order_count>0,'$do_date',old.order_
date_first),
    if(new.order_count>0,'$do_date',old.order_date_last),
    nvl(old.order_count,0)+nvl(new.order_count,0),
    nvl(old.order_amount,0)+nvl(new.order_amount,0),
    nvl(new.order_last_30d_count,0),
    nvl(new.order_last_30d_amount,0),
    if(old.payment_date_first is null and new.payment_count>0,'$do_date',old.
payment_date_first),
    if(new.payment_count>0,'$do_date',old.payment_date_last),
    nvl(old.payment_count,0)+nvl(new.payment_count,0),
    nvl(old.payment_amount,0)+nvl(new.payment_amount,0),
    nvl(new.payment_last_30d_count,0),
    nvl(new.payment_last_30d_amount,0)
from
(
    select 
        *
    from ${APP}.dwt_user_topic
)old
full outer join
(
    select
        user_id,
        sum(if(dt='$do_date',login_count,0)) login_count,
        sum(if(dt='$do_date',order_count,0)) order_count,
        sum(if(dt='$do_date',order_amount,0)) order_amount,
        sum(if(dt='$do_date',payment_count,0)) payment_count,
        sum(if(dt='$do_date',payment_amount,0)) payment_amount,
        sum(if(order_count>0,1,0)) login_last_30d_count,
        sum(order_count) order_last_30d_count,
        sum(order_amount) order_last_30d_amount,
        sum(payment_count) payment_last_30d_count,
        sum(payment_amount) payment_last_30d_amount
    from ${APP}.dws_user_action_daycount
    where dt>=date_add( '$do_date',-30)
    group by user_id
)new
on old.user_id=new.user_id;

with
sku_act as
(
select 
    sku_id,
    sum(if(dt='$do_date', order_count,0 )) order_count,
    sum(if(dt='$do_date',order_num ,0 ))  order_num, 
    sum(if(dt='$do_date',order_amount,0 )) order_amount ,
    sum(if(dt='$do_date',payment_count,0 )) payment_count,
    sum(if(dt='$do_date',payment_num,0 )) payment_num,
    sum(if(dt='$do_date',payment_amount,0 )) payment_amount,
    sum(if(dt='$do_date',refund_count,0 )) refund_count,
    sum(if(dt='$do_date',refund_num,0 )) refund_num,
    sum(if(dt='$do_date',refund_amount,0 )) refund_amount,  
    sum(if(dt='$do_date',cart_count,0 )) cart_count,
    sum(if(dt='$do_date',cart_num,0 )) cart_num,
    sum(if(dt='$do_date',favor_count,0 )) favor_count,
    sum(if(dt='$do_date',appraise_good_count,0 )) appraise_good_count,  
    sum(if(dt='$do_date',appraise_mid_count,0 ) ) appraise_mid_count ,
    sum(if(dt='$do_date',appraise_bad_count,0 )) appraise_bad_count,  
    sum(if(dt='$do_date',appraise_default_count,0 )) appraise_default_count,
    sum( order_count  ) order_count30 ,
    sum( order_num  )  order_num30,
    sum(order_amount ) order_amount30,
    sum(payment_count ) payment_count30,
    sum(payment_num ) payment_num30,
    sum(payment_amount ) payment_amount30,
    sum(refund_count  ) refund_count30,
    sum(refund_num ) refund_num30,
    sum(refund_amount ) refund_amount30,
    sum(cart_count  ) cart_count30,
    sum(cart_num ) cart_num30,
    sum(favor_count ) favor_count30,
    sum(appraise_good_count ) appraise_good_count30,
    sum(appraise_mid_count  ) appraise_mid_count30,
    sum(appraise_bad_count ) appraise_bad_count30,
    sum(appraise_default_count )  appraise_default_count30 
from ${APP}.dws_sku_action_daycount
where dt>=date_add ( '$do_date',-30)
group by sku_id
),
sku_topic
as 
(
select
    sku_id,
    spu_id,
    order_last_30d_count,
    order_last_30d_num,
    order_last_30d_amount,
    order_count,
    order_num,
    order_amount  ,
    payment_last_30d_count,
    payment_last_30d_num,
    payment_last_30d_amount,
    payment_count,
    payment_num,
    payment_amount,
    refund_last_30d_count,
    refund_last_30d_num,
    refund_last_30d_amount ,
    refund_count  ,
    refund_num ,
    refund_amount  ,
    cart_last_30d_count  ,
    cart_last_30d_num  ,
    cart_count  ,
    cart_num  ,
    favor_last_30d_count  ,
    favor_count  ,
    appraise_last_30d_good_count  ,
    appraise_last_30d_mid_count  ,
    appraise_last_30d_bad_count  ,
    appraise_last_30d_default_count  ,
    appraise_good_count  ,
    appraise_mid_count  ,
    appraise_bad_count  ,
    appraise_default_count 
from ${APP}.dwt_sku_topic
)
insert overwrite table ${APP}.dwt_sku_topic
select 
    nvl(sku_act.sku_id,sku_topic.sku_id) ,
    sku_info.spu_id,
    nvl (sku_act.order_count30,0)      ,
    nvl (sku_act.order_num30,0)   ,
    nvl (sku_act.order_amount30,0)   ,
    nvl(sku_topic.order_count,0)+ nvl (sku_act.order_count,0) ,
    nvl(sku_topic.order_num,0)+ nvl (sku_act.order_num,0)   ,
    nvl(sku_topic.order_amount,0)+ nvl (sku_act.order_amount,0),
    nvl (sku_act.payment_count30,0),
    nvl (sku_act.payment_num30,0),
    nvl (sku_act.payment_amount30,0),
    nvl(sku_topic.payment_count,0)+ nvl (sku_act.payment_count,0) ,
    nvl(sku_topic.payment_num,0)+ nvl (sku_act.payment_count,0)  ,
    nvl(sku_topic.payment_amount,0)+ nvl (sku_act.payment_count,0)  ,
    nvl (refund_count30,0),
    nvl (sku_act.refund_num30,0),
    nvl (sku_act.refund_amount30,0),
    nvl(sku_topic.refund_count,0)+ nvl (sku_act.refund_count,0),
    nvl(sku_topic.refund_num,0)+ nvl (sku_act.refund_num,0),
    nvl(sku_topic.refund_amount,0)+ nvl (sku_act.refund_amount,0),
    nvl(sku_act.cart_count30,0)  ,
    nvl(sku_act.cart_num30,0)  ,
    nvl(sku_topic.cart_count  ,0)+ nvl (sku_act.cart_count,0),
    nvl( sku_topic.cart_num  ,0)+ nvl (sku_act.cart_num,0),
    nvl(sku_act.favor_count30 ,0)  ,
    nvl (sku_topic.favor_count  ,0)+ nvl (sku_act.favor_count,0),
    nvl (sku_act.appraise_good_count30 ,0)  ,
    nvl (sku_act.appraise_mid_count30 ,0)  ,
    nvl (sku_act.appraise_bad_count30 ,0)  ,
    nvl (sku_act.appraise_default_count30 ,0)  ,
    nvl (sku_topic.appraise_good_count  ,0)+ nvl (sku_act.appraise_good_count,0)  ,
    nvl (sku_topic.appraise_mid_count   ,0)+ nvl (sku_act.appraise_mid_count,0) ,
    nvl (sku_topic.appraise_bad_count  ,0)+ nvl (sku_act.appraise_bad_count,0)  ,
    nvl (sku_topic.appraise_default_count  ,0)+ nvl (sku_act.appraise_default_
count,0) 
from sku_act
full outer join sku_topic
on sku_act.sku_id =sku_topic.sku_id
left join
(select * from ${APP}.dwd_dim_sku_info where dt='$do_date') sku_info
on nvl(sku_topic.sku_id,sku_act.sku_id)= sku_info.id;

insert overwrite table ${APP}.dwt_coupon_topic
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
    from ${APP}.dwt_coupon_topic
)old
full outer join
(
    select
        coupon_id,
        get_count,
        using_count,
        used_count
    from ${APP}.dws_coupon_use_daycount
    where dt='$do_date'
)new
on old.coupon_id=new.coupon_id;

insert overwrite table ${APP}.dwt_activity_topic
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
    from ${APP}.dwt_activity_topic
)old
full outer join
(
    select
        id,
        activity_name,
        order_count,
        payment_count
    from ${APP}.dws_activity_info_daycount
    where dt='$do_date'
)new
on old.id=new.id;
"

$hive -e "$sql"
---------------------------------------------------------------
