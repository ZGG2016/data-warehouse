ADS层之会员主题

1. 商品个数信息
建表语句
--------------------------------------------------------------
drop table if exists ads_product_info;
create external table ads_product_info(
    `dt` string COMMENT '统计日期',
    `sku_num` string COMMENT 'sku个数',
    `spu_num` string COMMENT 'spu个数'
) COMMENT '商品个数信息表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_info';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_product_info
select
    '2020-03-10' dt,
    sku_num,
    spu_num
from
(
    select
        '2020-03-10' dt,
        count(*) sku_num
    from
        dwt_sku_topic
) tmp_sku_num
join
(
    select
        '2020-03-10' dt,
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
--------------------------------------------------------------

2. 商品销量排名
建表语句
--------------------------------------------------------------
drop table if exists ads_product_sale_topN;
create external table ads_product_sale_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品id',
    `payment_amount` bigint COMMENT '销量'
) COMMENT '商品销量排名表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_sale_topN';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_product_sale_topN
select
    '2020-03-10' dt,
    sku_id,
    payment_amount
from
    dws_sku_action_daycount
where
    dt='2020-03-10'
order by payment_amount desc
limit 10;
--------------------------------------------------------------

3. 商品收藏排名
建表语句
--------------------------------------------------------------
drop table if exists ads_product_favor_topN;
create external table ads_product_favor_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品id',
    `favor_count` bigint COMMENT '收藏量'
) COMMENT '商品收藏排名表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_favor_topN';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_product_favor_topN
select
    '2020-03-10' dt,
    sku_id,
    favor_count
from
    dws_sku_action_daycount
where
    dt='2020-03-10'
order by favor_count desc
limit 10;
--------------------------------------------------------------

4. 商品加入购物车排名
建表语句
--------------------------------------------------------------
drop table if exists ads_product_cart_topN;
create external table ads_product_cart_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品id',
    `cart_num` bigint COMMENT '加入购物车数量'
) COMMENT '商品加入购物车排名表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_cart_topN';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_product_cart_topN
select
    '2020-03-10' dt,
    sku_id,
    cart_num
from
    dws_sku_action_daycount
where
    dt='2020-03-10'
order by cart_num desc
limit 10;
--------------------------------------------------------------

5. 商品退款率排名
建表语句
--------------------------------------------------------------
drop table if exists ads_product_refund_topN;
create external table ads_product_refund_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品id',
    `refund_ratio` decimal(10,2) COMMENT '退款率'
) COMMENT '商品退款率排名表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_product_refund_topN';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_product_refund_topN
select
    '2020-03-10',
    sku_id,
    refund_last_30d_count/payment_last_30d_count*100 refund_ratio
from dwt_sku_topic
order by refund_ratio desc
limit 10;
--------------------------------------------------------------

6. 商品差评率
建表语句
--------------------------------------------------------------
drop table if exists ads_appraise_bad_topN;
create external table ads_appraise_bad_topN(
    `dt` string COMMENT '统计日期',
    `sku_id` string COMMENT '商品id',
    `appraise_bad_ratio` decimal(10,2) COMMENT '差评率'
) COMMENT '商品差评率排名表'
row format delimited fields terminated by '\t'
location '/warehouse/gmall/ads/ads_appraise_bad_topN';
--------------------------------------------------------------

数据加载
--------------------------------------------------------------
insert into table ads_appraise_bad_topN
select
    '2020-03-10' dt,
    sku_id,
appraise_bad_count/(appraise_good_count+appraise_mid_count+appraise_bad_count+appraise_default_count) appraise_bad_ratio
from
    dws_sku_action_daycount
where
    dt='2020-03-10'
order by appraise_bad_ratio desc
limit 10;
--------------------------------------------------------------