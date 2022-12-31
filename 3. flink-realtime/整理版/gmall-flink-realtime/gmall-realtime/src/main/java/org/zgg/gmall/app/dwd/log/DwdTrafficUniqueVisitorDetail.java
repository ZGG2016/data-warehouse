package org.zgg.gmall.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONAware;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.zgg.gmall.util.DateFormatUtil;
import org.zgg.gmall.util.MyKafkaUtil;

/**
 * 独立访客事务事实表
 */
//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> Kafka(DWD)
//程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwdTrafficUniqueVisitorDetail -> Kafka(ZK)
public class DwdTrafficUniqueVisitorDetail {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1); //生产环境中设置为Kafka主题的分区数
        //1.1 开启CheckPoint
        env.enableCheckpointing(5 * 60000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(10 * 60000L);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(2);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L));

        //1.2 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata102:9000/flink-ck");
        System.setProperty("HADOOP_USER_NAME","root");

        //TODO 2.读取Kafka 页面日志主题创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "unique_visitor_detail_211126";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.过滤掉上一跳页面不为null的数据并将每行数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String s, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    String lastPageId = jsonObject.getJSONObject("page").getString("last_page_id");
                    if (lastPageId == null) {
                        collector.collect(jsonObject);
                    }
                } catch (Exception e) {
                    System.out.println(s);
                }
            }
        });

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程实现按照Mid的去重
        SingleOutputStreamOperator<JSONObject> uvDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-visit", String.class);
                //设置状态的TTL
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();
                stateDescriptor.enableTimeToLive(ttlConfig);
                lastVisitState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                String lastDate = lastVisitState.value();
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                if (lastDate != null || !lastDate.equals(curDate)) {
                    lastVisitState.update(curDate);
                    return true;
                } else {
                    return false;
                }
            }
        });

        //TODO 6.将数据写到Kafka
        String targetTopic = "dwd_traffic_unique_visitor_detail";
        uvDS.print(">>>>>>>>");
        uvDS.map(JSONAware::toJSONString)
                .addSink(MyKafkaUtil.getFlinkKafkaProducer(targetTopic));

        //TODO 7.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
