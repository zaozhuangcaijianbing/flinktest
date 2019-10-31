package mytest1031;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

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
        properties.setProperty("group.id", "test1");
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

        env.execute("kafka wordcount");
    }


}
