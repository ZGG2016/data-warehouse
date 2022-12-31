import bean.WaterSensor;
import bean.WaterSensor2;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class IntervalJoinTest {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //1001,23.6,1324
        SingleOutputStreamOperator<WaterSensor> waterSensorDS1 = env.socketTextStream("bigdata102", 8888)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return new Long(split[2]) * 1000L;
                    }
                })).map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor(split[0],
                            Double.parseDouble(split[1]),
                            Long.parseLong(split[2]));
                });


        SingleOutputStreamOperator<WaterSensor2> waterSensorDS2 = env.socketTextStream("bigdata102", 9999)
                .assignTimestampsAndWatermarks(WatermarkStrategy.<String>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<String>() {
                    @Override
                    public long extractTimestamp(String element, long recordTimestamp) {
                        String[] split = element.split(",");
                        return new Long(split[2]) * 1000L;
                    }
                })).map(line -> {
                    String[] split = line.split(",");
                    return new WaterSensor2(split[0],
                            split[1],
                            Long.parseLong(split[2]));
                });

        //JOIN
        SingleOutputStreamOperator<Tuple2<WaterSensor, WaterSensor2>> result = waterSensorDS1.keyBy(WaterSensor::getId)
                .intervalJoin(waterSensorDS2.keyBy(WaterSensor2::getId))
                .between(Time.seconds(-5), Time.seconds(5))
                .process(new ProcessJoinFunction<WaterSensor, WaterSensor2, Tuple2<WaterSensor, WaterSensor2>>() {
                    @Override
                    public void processElement(WaterSensor left, WaterSensor2 right, Context ctx, Collector<Tuple2<WaterSensor, WaterSensor2>> out) throws Exception {
                        out.collect(new Tuple2<>(left, right));
                    }
                });

        result.print(">>>>>>>>>>>>");

        env.execute();

    }
}
