
-- 事件日志
drop table if exists ods_start_log;

create external table ods_start_log (`line` string)
partitioned by (`dt` string)
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_start_log';

load data inpath '/origin_data/gmall/log/topic_start/2022-09-20' into table gmall.ods_start_log partition(dt='2022-09-20');

hadoop jar /opt/hadoop-3.3.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_start_log/dt=2022-09-20


-- 事件日志
drop table if exists ods_event_log;

create external table ods_event_log (`line` string)
partitioned by (`dt` string)
STORED AS
INPUTFORMAT 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
location '/warehouse/gmall/ods/ods_event_log';

load data inpath '/origin_data/gmall/log/topic_event/2022-09-20' into table gmall.ods_event_log partition(dt='2022-09-20');

hadoop jar /opt/hadoop-3.3.3/share/hadoop/common/hadoop-lzo-0.4.20.jar com.hadoop.compression.lzo.DistributedLzoIndexer /warehouse/gmall/ods/ods_event_log/dt=2022-09-20


-- 可以使用命令加载一天的数据，也可以封装成脚本执行(hdfs_to_ods_log.sh)