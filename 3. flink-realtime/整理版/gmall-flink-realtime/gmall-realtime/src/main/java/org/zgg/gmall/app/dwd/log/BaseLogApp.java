package org.zgg.gmall.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.zgg.gmall.util.DateFormatUtil;
import org.zgg.gmall.util.MyKafkaUtil;

/**
 * 用户行为数据分流
 */
//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD)
//程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK)
public class BaseLogApp {
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

        //TODO 2.消费Kafka topic_log 主题的数据创建流
        String topic = "topic_log";
        String groupId = "base_log_app";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.过滤掉非JSON格式的数据&将每行数据转换为JSON对象
        OutputTag<String> dirtyTag = new OutputTag<String>("Ditry"){};
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String s, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> collector) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(s);
                    collector.collect(jsonObject);
                } catch (Exception e) {
                    context.output(dirtyTag, s);
                }
            }
        });

        //获取侧输出流脏数据并打印
        DataStream<String> dirtyDS = jsonObjDS.getSideOutput(dirtyTag);
//        dirtyDS.print("Dirty>>>>>>>>>>>>");

        //TODO 4.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.使用状态编程做新老访客标记校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            private ValueState<String> lastVisitState;

            @Override
            public void open(Configuration parameters) throws Exception {
                lastVisitState = getRuntimeContext().getState(new ValueStateDescriptor<String>("last-visit", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取is_new标记 & ts 并将时间戳转换为年月日
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");
                String curDate = DateFormatUtil.toDate(ts);

                //获取状态中的日期
                String lastDate = lastVisitState.value();

                //判断is_new标记是否为"1"
                if ("1".equals(isNew)) {
                    if (lastDate == null) {
                        lastVisitState.update(curDate);
                    } else if (!lastDate.equals(curDate)) {
                        value.getJSONObject("common").put("is_new", "0");
                    }
                } else if (lastDate == null) {
                    lastVisitState.update(DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L));
                }
                return value;
            }
        });

        //TODO 6.使用侧输出流进行分流处理  页面日志放到主流  启动、曝光、动作、错误放到侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {};
        OutputTag<String> displayTag = new OutputTag<String>("display") {};
        OutputTag<String> actionTag = new OutputTag<String>("action") {};
        OutputTag<String> errorTag = new OutputTag<String>("error") {};
        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context context, Collector<String> collector) throws Exception {
                //尝试获取错误信息
                String err = value.getString("err");
                if (err != null) {
                    context.output(errorTag, value.toJSONString());
                }
                value.remove("err");

                //尝试获取启动信息
                String start = value.getString("start");
                if (start != null) {
                    context.output(startTag, value.toJSONString());
                } else {

                    //获取公共信息&页面id&时间戳
                    String common = value.getString("common");
                    String pageId = value.getJSONObject("page").getString("page_id");
                    Long ts = value.getLong("ts");

                    //尝试获取曝光数据
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null && displays.size() > 0) {
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page_id", pageId);
                            display.put("ts", ts);
                            context.output(displayTag, display.toJSONString());
                        }
                    }
                    //尝试获取动作数据
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null && actions.size() > 0) {
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page_id", pageId);
                            context.output(actionTag, action.toJSONString());
                        }
                    }

                    //移除曝光和动作数据&写到页面日志主流
                    value.remove("displays");
                    value.remove("actions");
                    collector.collect(value.toString());
                }
            }
        });

        //TODO 7.提取各个侧输出流数据
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        //TODO 8.将数据打印并写入对应的主题
        pageDS.print("Page>>>>>>>>>>");
        startDS.print("Start>>>>>>>>");
        displayDS.print("Display>>>>");
        actionDS.print("Action>>>>>>");
        errorDS.print("Error>>>>>>>>");

        String page_topic = "dwd_traffic_page_log";
        String start_topic = "dwd_traffic_start_log";
        String display_topic = "dwd_traffic_display_log";
        String action_topic = "dwd_traffic_action_log";
        String error_topic = "dwd_traffic_error_log";

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(page_topic));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(start_topic));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(display_topic));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(action_topic));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer(error_topic));

        //TODO 9.启动任务
        env.execute("BaseLogApp");
    }
}
