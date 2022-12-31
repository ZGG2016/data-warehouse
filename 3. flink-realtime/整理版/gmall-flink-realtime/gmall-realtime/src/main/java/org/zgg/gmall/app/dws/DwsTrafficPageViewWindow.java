package org.zgg.gmall.app.dws;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.zgg.gmall.bean.TrafficHomeDetailPageViewBean;
import org.zgg.gmall.util.DateFormatUtil;
import org.zgg.gmall.util.MyClickHouseUtil;
import org.zgg.gmall.util.MyKafkaUtil;

import java.time.Duration;

/**
 * 流量域页面浏览各窗口汇总表
 */
//数据流：web/app -> Nginx -> 日志服务器(.log) -> Flume -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：     Mock(lg.sh) -> Flume(f1) -> Kafka(ZK) -> BaseLogApp -> Kafka(ZK) -> DwsTrafficPageViewWindow -> ClickHouse(ZK)
public class DwsTrafficPageViewWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1.1 状态后端设置
//        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
//        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
//        env.getCheckpointConfig().enableExternalizedCheckpoints(
//                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
//        );
//        env.setRestartStrategy(RestartStrategies.failureRateRestart(
//                3, Time.days(1), Time.minutes(1)
//        ));
//        env.setStateBackend(new HashMapStateBackend());
        //env.getCheckpointConfig().setCheckpointStorage("hdfs://bigdata102:9000/flink-ck");
        //System.setProperty("HADOOP_USER_NAME", "root");

        //TODO 2.读取 Kafka 页面日志主题数据创建流
        String topic = "dwd_traffic_page_log";
        String groupId = "dws_traffic_page_view_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));


        //TODO 3.将每行数据转换为JSON对象并过滤(首页与商品详情页)
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.flatMap(new FlatMapFunction<String, JSONObject>() {
            @Override
            public void flatMap(String value, Collector<JSONObject> out) throws Exception {
                JSONObject jsonObject = JSON.parseObject(value);

                String pageId = jsonObject.getJSONObject("page").getString("page_id");
                if ("home".equals(pageId) || "good_detail".equals(pageId)) {
                    out.collect(jsonObject);
                }
            }
        });

        //TODO 4.提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(
                WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
                            @Override
                            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                                return element.getLong("ts");
                            }
                        }));
        //TODO 5.按照Mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getJSONObject("common").getString("mid"));
        
        //TODO 6.使用状态编程过滤出首页与商品详情页的独立访客
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> trafficHomeDetailDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, TrafficHomeDetailPageViewBean>() {
            private ValueState<String> homeLastState;
            private ValueState<String> detailLastState;

            @Override
            public void open(Configuration parameters) throws Exception {
                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                ValueStateDescriptor<String> homeStateDes = new ValueStateDescriptor<>("home-state", String.class);
                ValueStateDescriptor<String> detailStateDes = new ValueStateDescriptor<>("detail-state", String.class);

                homeStateDes.enableTimeToLive(ttlConfig);
                detailStateDes.enableTimeToLive(ttlConfig);

                homeLastState = getRuntimeContext().getState(homeStateDes);
                detailLastState = getRuntimeContext().getState(detailStateDes);
            }

            @Override
            public void flatMap(JSONObject value, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                Long ts = value.getLong("ts");
                String curDt = DateFormatUtil.toDate(ts);
                String homeLastDt = homeLastState.value();
                String detailLastDt = detailLastState.value();

                long homeCt = 0;
                long detailCt = 0;

                if ("home".equals(value.getJSONObject("page").getString("page_id"))) {
                    if (homeLastDt == null || !homeLastDt.equals(curDt)) {
                        homeCt = 1L;
                        homeLastState.update(curDt);
                    }
                } else {
                    if (detailLastDt == null || !detailLastDt.equals(curDt)) {
                        detailCt = 1L;
                        detailLastState.update(curDt);
                    }
                }

                if (homeCt == 1L || detailCt == 1L) {
                    out.collect(new TrafficHomeDetailPageViewBean("", "",
                            homeCt,
                            detailCt,
                            ts));
                }

            }
        });

        //TODO 7.开窗聚合
        SingleOutputStreamOperator<TrafficHomeDetailPageViewBean> resultDS = trafficHomeDetailDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<TrafficHomeDetailPageViewBean>() {
                    @Override
                    public TrafficHomeDetailPageViewBean reduce(TrafficHomeDetailPageViewBean value1, TrafficHomeDetailPageViewBean value2) throws Exception {
                        value1.setHomeUvCt(value1.getHomeUvCt() + value2.getHomeUvCt());
                        value1.setGoodDetailUvCt(value1.getGoodDetailUvCt() + value2.getGoodDetailUvCt());
                        return value1;
                    }
                }, new AllWindowFunction<TrafficHomeDetailPageViewBean, TrafficHomeDetailPageViewBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<TrafficHomeDetailPageViewBean> values, Collector<TrafficHomeDetailPageViewBean> out) throws Exception {
                        TrafficHomeDetailPageViewBean pageViewBean = values.iterator().next();

                        pageViewBean.setTs(System.currentTimeMillis());
                        pageViewBean.setStt(DateFormatUtil.toYmdHms(timeWindow.getStart()));
                        pageViewBean.setEdt(DateFormatUtil.toYmdHms(timeWindow.getEnd()));

                        out.collect(pageViewBean);
                    }
                });

        //TODO 8.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_traffic_page_view_window values(?,?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("DwsTrafficPageViewWindow");
    }
}