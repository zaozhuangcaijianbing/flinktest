package mytest1031;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author wangning
 * 从kafka获取
 */
public class Test1 {
    public static void main(String[] args) throws Exception {
        //1、ExecutionEnvironmen
        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(params);
//        env.setParallelism(1);
//        env.enableCheckpointing(5000);


        //2、kafka
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.199.188:9092");
        properties.setProperty("group.id", "test3");
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>("wn_1031", new SimpleStringSchema(), properties);

        DataStream<LogPojo> stream = env.addSource(flinkKafkaConsumer)
                .map(o -> JSONObject.parseObject(o, LogPojo.class));


        //3、水印
        DataStream<LogPojo> watermarkStream = stream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<LogPojo>(Time.seconds(3)) {
            @Override
            public long extractTimestamp(LogPojo element) {
                return element.getTime();
            }
        });


        //4、业务转换
        DataStream<Tuple2<String, Integer>> sum = watermarkStream.map(new MapFunction<LogPojo, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(LogPojo value) throws Exception {
                return new Tuple2<>(value.getProvince(), 1);
            }
        })
                .keyBy(0)
                .timeWindow(Time.seconds(10))
                .sum(1);

        sum.print();


        //业务2
        stream
                .map(new MapFunction<LogPojo, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(LogPojo value) throws Exception {
                        return new Tuple2<>(value.getProvince(), 1);
                    }
                })
                .keyBy(0)
                .sum(1).keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> value) throws Exception {
                return "";
            }
        })
                .fold(new HashMap<String, Integer>(), new FoldFunction<Tuple2<String, Integer>, Map<String, Integer>>() {
                    @Override
                    public Map<String, Integer> fold(Map<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
                        accumulator.put(value.f0, value.f1);
                        return accumulator;
                    }
                }).print();


        env.execute("kafka wordcount");
    }


}
