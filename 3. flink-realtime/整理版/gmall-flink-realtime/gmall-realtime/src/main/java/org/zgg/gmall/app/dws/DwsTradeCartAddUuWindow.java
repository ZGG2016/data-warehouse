package org.zgg.gmall.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.zgg.gmall.bean.CartAddUuBean;
import org.zgg.gmall.util.DateFormatUtil;
import org.zgg.gmall.util.MyClickHouseUtil;
import org.zgg.gmall.util.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
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

import java.time.Duration;

/**
 * 交易域加购各窗口汇总表
 */
//数据流：Web/app -> nginx -> 业务服务器(Mysql) -> Maxwell -> Kafka(ODS) -> FlinkApp -> Kafka(DWD) -> FlinkApp -> ClickHouse(DWS)
//程  序：Mock  ->  Mysql  ->  Maxwell -> Kafka(ZK)  ->  DwdTradeCartAdd -> Kafka(ZK) -> DwdTradeCartAdd -> ClickHouse(ZK)
public class DwsTradeCartAddUuWindow {

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

        //TODO 2.读取 Kafka DWD层 加购事实表
        String topic = "dwd_trade_cart_add";
        String groupId = "dws_trade_cart_add_uu_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId));

        //TODO 3.将数据转换为JSON对象
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaDS.map(JSON::parseObject);

        //TODO 4.提取事件时间生成Watermark
        SingleOutputStreamOperator<JSONObject> jsonObjWithWmDS = jsonObjDS.assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {

                String operateTime = element.getString("operate_time");

                if (operateTime != null) {
                    return DateFormatUtil.toTs(operateTime, true);
                } else {
                    return DateFormatUtil.toTs(element.getString("create_time"), true);
                }
            }
        }));

        //TODO 5.按照user_id分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjWithWmDS.keyBy(json -> json.getString("user_id"));

        //TODO 6.使用状态编程提取独立加购用户
        SingleOutputStreamOperator<CartAddUuBean> cartAddDS = keyedStream.flatMap(new RichFlatMapFunction<JSONObject, CartAddUuBean>() {

            private ValueState<String> lastCartAddState;

            @Override
            public void open(Configuration parameters) throws Exception {

                StateTtlConfig ttlConfig = new StateTtlConfig.Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .build();

                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last-cart", String.class);
                stateDescriptor.enableTimeToLive(ttlConfig);

                lastCartAddState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void flatMap(JSONObject value, Collector<CartAddUuBean> out) throws Exception {

                //获取状态数据以及当前数据的日期
                String lastDt = lastCartAddState.value();
                String operateTime = value.getString("operate_time");
                String curDt = null;
                if (operateTime != null) {
                    curDt = operateTime.split(" ")[0];
                } else {
                    String createTime = value.getString("create_time");
                    curDt = createTime.split(" ")[0];
                }

                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastCartAddState.update(curDt);
                    out.collect(new CartAddUuBean(
                            "",
                            "",
                            1L,
                            null));
                }
            }
        });

        //TODO 7.开窗、聚合
        SingleOutputStreamOperator<CartAddUuBean> resultDS = cartAddDS.windowAll(TumblingEventTimeWindows.of(org.apache.flink.streaming.api.windowing.time.Time.seconds(10)))
                .reduce(new ReduceFunction<CartAddUuBean>() {
                    @Override
                    public CartAddUuBean reduce(CartAddUuBean value1, CartAddUuBean value2) throws Exception {
                        value1.setCartAddUuCt(value1.getCartAddUuCt() + value2.getCartAddUuCt());
                        return value1;
                    }
                }, new AllWindowFunction<CartAddUuBean, CartAddUuBean, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow window, Iterable<CartAddUuBean> values, Collector<CartAddUuBean> out) throws Exception {
                        CartAddUuBean next = values.iterator().next();

                        next.setEdt(DateFormatUtil.toYmdHms(window.getEnd()));
                        next.setStt(DateFormatUtil.toYmdHms(window.getStart()));
                        next.setTs(System.currentTimeMillis());

                        out.collect(next);
                    }
                });

        //TODO 8.将数据写出到ClickHouse
        resultDS.print(">>>>>>>>>>>");
        resultDS.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_cart_add_uu_window values (?,?,?,?)"));

        //TODO 9.启动任务
        env.execute("DwsTradeCartAddUuWindow");
    }

}
